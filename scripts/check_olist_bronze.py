#!/usr/bin/env python3
"""Read-only Kafka-to-Iceberg Bronze reconciliation at captured offset/snapshot bounds."""

from __future__ import annotations

import argparse
from functools import reduce
import json
from pathlib import Path
import sys

from ingest_olist_bronze import (
    BRONZE_FIELDS, BronzeError, Settings, build_bronze_frame, check_target,
    kafka_bounds, read_settings,
)

IDENTITY = ("kafka_topic", "kafka_partition", "kafka_offset")


def frozen_bronze(spark, settings: Settings, table: str):
    check_target(spark, settings, table)
    target = settings.target(table)
    refs = spark.sql(f"SELECT snapshot_id FROM {target}.refs WHERE name = 'main'").collect()
    if not refs:
        # Freeze an empty table instead of reading a mutable current view later.
        return None, spark.createDataFrame([], spark.table(target).schema)
    snapshot_id = int(refs[0].snapshot_id)
    return snapshot_id, spark.read.option("snapshot-id", str(snapshot_id)).table(target)


def capture_json(settings: Settings, table: str, bounds: dict) -> dict:
    return {"table": settings.target(table), "topic": settings.topic(table),
            "partitions": {str(p): {"start_inclusive": start, "end_exclusive": end}
                           for p, (start, end) in bounds.items()}}


def status(spark, settings: Settings, table: str) -> None:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    snapshot_id, frame = frozen_bronze(spark, settings, table)
    frame.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        print(json.dumps({"table": settings.target(table), "snapshot_id": snapshot_id,
                          "rows": frame.count(),
                          "parse_errors": frame.where(F.col("parse_error").isNotNull()).count(),
                          "tombstones": frame.where(F.col("is_tombstone")).count()}, sort_keys=True))
        for row in (frame.groupBy("kafka_topic", "kafka_partition")
                    .agg(F.count("*").alias("rows"), F.min("kafka_offset").alias("min_offset"),
                         F.max("kafka_offset").alias("max_offset")).collect()):
            print(json.dumps(row.asDict(), sort_keys=True))
        for row in frame.groupBy("op", "is_tombstone").count().collect():
            print(json.dumps(row.asDict(), sort_keys=True))
    finally:
        frame.unpersist()


def verify(spark, settings: Settings, table: str, allow_retained_only: bool) -> bool:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    bounds = kafka_bounds(spark, settings, table)
    if any(start > 0 for start, _ in bounds.values()) and not allow_retained_only:
        raise BronzeError(f"{table}: earliest offset > 0; cannot audit initial history. "
                          "Use --allow-retained-only only to audit the retained range.")
    snapshot_id, actual_all = frozen_bronze(spark, settings, table)
    topic = settings.topic(table)
    starts = json.dumps({topic: {str(p): start for p, (start, _) in bounds.items()}})
    ends = json.dumps({topic: {str(p): end for p, (_, end) in bounds.items()}})
    kafka = (spark.read.format("kafka").option("kafka.bootstrap.servers", settings.bootstrap)
             .option("subscribe", topic).option("startingOffsets", starts)
             .option("endingOffsets", ends).option("failOnDataLoss", "true").load())
    expected = build_bronze_frame(kafka, table).drop("ingested_at")
    in_range = reduce(
        lambda left, right: left | right,
        [(F.col("kafka_topic") == topic) & (F.col("kafka_partition") == p)
         & (F.col("kafka_offset") >= start) & (F.col("kafka_offset") < end)
         for p, (start, end) in bounds.items()], F.lit(False),
    )
    actual_all.persist(StorageLevel.MEMORY_AND_DISK)
    expected.persist(StorageLevel.MEMORY_AND_DISK)
    actual = actual_all.where(in_range)
    try:
        total_rows = actual_all.count()
        expected_rows = expected.count()
        actual_rows = actual.count()
        invalid = actual_all.where(
            F.col("kafka_topic").isNull() | (F.col("kafka_topic") != topic)
            | F.col("kafka_partition").isNull() | ~F.col("kafka_partition").isin(list(bounds))
            | F.col("kafka_offset").isNull() | (F.col("kafka_offset") < 0)
            | F.col("ingested_at").isNull() | F.col("is_tombstone").isNull()
        ).count()
        duplicate_groups = (actual_all.groupBy(*IDENTITY).count()
                            .where(F.col("count") > 1).count())
        actual_errors = actual_all.where(F.col("parse_error").isNotNull()).count()
        expected_errors = expected.where(F.col("parse_error").isNotNull()).count()
        actual_ids, expected_ids = actual.select(*IDENTITY), expected.select(*IDENTITY)
        missing = expected_ids.join(actual_ids, list(IDENTITY), "left_anti")
        extra = actual_ids.join(expected_ids, list(IDENTITY), "left_anti")
        missing_count, extra_count = missing.count(), extra.count()
        joined = expected.alias("expected").join(actual.alias("bronze"), list(IDENTITY), "inner")
        comparable = [name for name, _ in BRONZE_FIELDS if name not in (*IDENTITY, "ingested_at")]
        mismatched_columns = [
            ~F.col("expected." + name).eqNullSafe(F.col("bronze." + name)) for name in comparable
        ]
        mismatches = joined.where(reduce(lambda left, right: left | right, mismatched_columns))
        mismatch_count = mismatches.select(*IDENTITY).distinct().count()
        column_counts = joined.agg(*[
            F.sum(F.when(condition, 1).otherwise(0)).alias(name)
            for name, condition in zip(comparable, mismatched_columns)
        ]).first().asDict()
        # Detect retention/truncation or added partitions during this audit.
        current_bounds = kafka_bounds(spark, settings, table)
        if set(current_bounds) != set(bounds) or any(
            current_bounds[p][0] > start or current_bounds[p][1] < end
            for p, (start, end) in bounds.items()
        ):
            raise BronzeError(f"{table}: Kafka range changed by retention/truncation; rerun at a stable range")
        ok = not any((invalid, duplicate_groups, actual_errors, expected_errors,
                      missing_count, extra_count, mismatch_count))
        summary = capture_json(settings, table, bounds)
        summary.update(
            snapshot_id=snapshot_id, bronze_total_rows=total_rows,
            kafka_rows_in_range=expected_rows, bronze_rows_in_range=actual_rows,
            bronze_rows_outside_range=total_rows - actual_rows,
            missing=missing_count, extra=extra_count, duplicate_groups=duplicate_groups,
            invalid_identity_or_ingestion_metadata=invalid,
            bronze_parse_errors=actual_errors, kafka_parse_errors=expected_errors,
            mismatched_rows=mismatch_count,
            mismatched_columns={name: count for name, count in column_counts.items() if count},
            scope="retained Kafka range versus one Iceberg snapshot; not PostgreSQL/Silver state",
            passed=ok,
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        for label, frame in (("missing", missing), ("extra", extra),
                             ("mismatch", mismatches.select(*IDENTITY).distinct())):
            for row in frame.limit(10).collect():
                print(json.dumps({"sample": label, **row.asDict()}, sort_keys=True))
        return ok
    finally:
        actual_all.unpersist()
        expected.unpersist()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "bounds", "status", "verify"))
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--tables", help="all or comma-separated table names")
    parser.add_argument("--allow-retained-only", action="store_true")
    args = parser.parse_args()
    spark = None
    try:
        settings = read_settings(args.env_file, args.tables)
        if args.action == "validate":
            print("Read-only checker configuration valid: " + ", ".join(settings.target(t) for t in settings.tables))
            print("No Spark session or data access was started.")
            return 0
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("olist-bronze-check").config("spark.sql.session.timeZone", "UTC").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        passed = True
        for table in settings.tables:
            if args.action == "bounds":
                print(json.dumps(capture_json(settings, table, kafka_bounds(spark, settings, table)), sort_keys=True))
            elif args.action == "status":
                status(spark, settings, table)
            else:
                passed = verify(spark, settings, table, args.allow_retained_only) and passed
        return 0 if passed else 1
    except (BronzeError, OSError, ValueError, KeyError) as exc:
        print(f"Bronze {args.action} failed: {exc}", file=sys.stderr)
        return 2
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
