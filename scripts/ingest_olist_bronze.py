#!/usr/bin/env python3
"""Append Olist Kafka records to Iceberg Bronze; validate does not start Spark."""

from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import signal
import sys
import threading
import time

CATALOG = "lakehouse"
NAMESPACE = "bronze"
KEYS = {
    "customers": ("customer_id",),
    "sellers": ("seller_id",),
    "products": ("product_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
}
DEFAULTS = {
    "BRONZE_TABLES": "orders,order_items",
    "BRONZE_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "BRONZE_TOPIC_PREFIX": "olist_pg.olist.",
    "BRONZE_CHECKPOINT_ROOT": "/opt/spark/work-dir/olist-bronze-checkpoints",
    "BRONZE_TRIGGER_SECONDS": "60",
    "BRONZE_MAX_OFFSETS_PER_TRIGGER": "100000",
}
BASE_FIELDS = (
    ("kafka_topic", "string"), ("kafka_partition", "int"),
    ("kafka_offset", "bigint"), ("kafka_timestamp", "timestamp"),
    ("kafka_timestamp_type", "int"), ("raw_key", "binary"),
    ("raw_value", "binary"),
)
PARSED_FIELDS = (
    ("key_json", "string"), ("key_has_schema", "boolean"),
    ("value_has_schema", "boolean"), ("op", "string"),
    ("source_db", "string"), ("source_schema", "string"),
    ("source_table", "string"), ("source_lsn", "bigint"),
    ("source_tx_id", "bigint"), ("source_ts_ms", "bigint"),
    ("event_ts_ms", "bigint"), ("source_snapshot", "string"),
    ("before_json", "string"), ("after_json", "string"),
    ("is_tombstone", "boolean"), ("parse_error", "string"),
)
BRONZE_FIELDS = BASE_FIELDS + PARSED_FIELDS + (("ingested_at", "timestamp"),)


class BronzeError(Exception):
    """Configuration, data-quality or recovery guard failure."""


@dataclass(frozen=True)
class Settings:
    tables: tuple[str, ...]
    bootstrap: str
    topic_prefix: str
    checkpoint_root: str
    trigger_seconds: int
    max_offsets: int

    def topic(self, table: str) -> str:
        return self.topic_prefix + table

    def target(self, table: str) -> str:
        return f"{CATALOG}.{NAMESPACE}.{table}"

    @property
    def status_file(self) -> Path:
        return Path(self.checkpoint_root).parent / "olist-bronze-status.json"


def read_settings(env_file: Path | None = None, tables: str | None = None) -> Settings:
    """Read only BRONZE_* keys; container environment overrides the existing .env."""
    values = dict(DEFAULTS)
    candidate = env_file or Path(__file__).resolve().parents[1] / ".env"
    if env_file is not None and not candidate.is_file():
        raise BronzeError(f"Missing environment file: {candidate}")
    if candidate.is_file():
        for line in candidate.read_text(encoding="utf-8-sig").splitlines():
            match = re.fullmatch(r"\s*(BRONZE_[A-Z0-9_]+)\s*=\s*(.*?)\s*", line)
            if match and match[1] in DEFAULTS:
                value = match[2]
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                values[match[1]] = value
    values.update({key: os.environ[key] for key in DEFAULTS if key in os.environ})
    selection = tables if tables is not None else values["BRONZE_TABLES"]
    selected = tuple(KEYS) if selection == "all" else tuple(selection.split(","))
    if not selected or len(set(selected)) != len(selected) or any(t not in KEYS for t in selected):
        raise BronzeError("Tables must be 'all' or a unique comma-separated subset of: " + ",".join(KEYS))
    prefix = values["BRONZE_TOPIC_PREFIX"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.", prefix):
        raise BronzeError("BRONZE_TOPIC_PREFIX must be a Kafka topic prefix ending in '.'")
    bootstrap = values["BRONZE_KAFKA_BOOTSTRAP_SERVERS"]
    if not bootstrap or any(char.isspace() for char in bootstrap):
        raise BronzeError("BRONZE_KAFKA_BOOTSTRAP_SERVERS must not be empty or contain spaces")
    root = PurePosixPath(values["BRONZE_CHECKPOINT_ROOT"])
    if (not root.is_absolute() or ".." in root.parts
            or not str(root).startswith("/opt/spark/work-dir/")):
        raise BronzeError("Checkpoint root must be inside the mounted /opt/spark/work-dir/ volume")
    try:
        trigger = int(values["BRONZE_TRIGGER_SECONDS"])
        max_offsets = int(values["BRONZE_MAX_OFFSETS_PER_TRIGGER"])
    except ValueError as exc:
        raise BronzeError("Trigger seconds and max offsets must be integers") from exc
    if trigger < 1 or max_offsets < 1:
        raise BronzeError("Trigger seconds and max offsets must be positive")
    return Settings(selected, bootstrap, prefix, str(root), trigger, max_offsets)


def _json_text(value) -> str | None:
    return None if value is None else json.dumps(value, ensure_ascii=False, sort_keys=True,
                                                separators=(",", ":"), allow_nan=False)


def parse_event(raw_key, raw_value, table: str) -> dict:
    """Keep malformed records with error codes; never discard raw Kafka bytes."""
    result = {name: None for name, _ in PARSED_FIELDS}
    result.update(key_has_schema=False, value_has_schema=False, is_tombstone=raw_value is None)
    errors = []

    def finite_float(text):
        value = float(text)
        if not math.isfinite(value):
            raise ValueError("Non-finite JSON number")
        return value

    def decode(raw, label):
        if raw is None:
            errors.append(f"missing_{label}")
            return None
        try:
            text = bytes(raw).decode("utf-8") if not isinstance(raw, str) else raw
            obj = json.loads(text, parse_float=finite_float,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
            if not isinstance(obj, dict):
                raise ValueError()
            wrapped = "schema" in obj and "payload" in obj
            result[f"{label}_has_schema"] = wrapped
            obj = obj["payload"] if wrapped else obj
            if not isinstance(obj, dict):
                raise ValueError()
            return obj
        except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
            errors.append(f"invalid_{label}_json_object")
            return None

    def integer(value, field):
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or not -(2**63) <= value < 2**63:
            errors.append(f"invalid_{field}")
            return None
        return value

    key = decode(raw_key, "key")
    if key is not None:
        result["key_json"] = _json_text(key)
        if any(column not in key or key[column] is None or isinstance(key[column], (dict, list, bool))
               for column in KEYS[table]):
            errors.append("missing_or_invalid_key_columns")
    if raw_value is not None:
        event = decode(raw_value, "value")
        if event is not None:
            op = event.get("op")
            if op not in ("r", "c", "u", "d"):
                errors.append("unsupported_op")
            else:
                result["op"] = op
            before, after = event.get("before"), event.get("after")
            if before is not None and not isinstance(before, dict):
                errors.append("invalid_before")
            if op in ("r", "c", "u") and not isinstance(after, dict):
                errors.append("missing_after")
            if op == "d" and (after is not None or not isinstance(before, dict)):
                errors.append("invalid_delete_envelope")
            result["before_json"] = _json_text(before)
            result["after_json"] = _json_text(after)
            row = before if op == "d" else after
            if key is not None and isinstance(row, dict) and any(
                key.get(column) != row.get(column) for column in KEYS[table]
            ):
                errors.append("key_row_mismatch")
            source = event.get("source")
            if not isinstance(source, dict):
                errors.append("missing_source")
                source = {}
            for field, source_key in (("source_db", "db"), ("source_schema", "schema"),
                                      ("source_table", "table")):
                value = source.get(source_key)
                if value is not None and not isinstance(value, str):
                    errors.append(f"invalid_{field}")
                else:
                    result[field] = value
            if source.get("schema") != "olist" or source.get("table") != table:
                errors.append("source_table_mismatch")
            for field, value in (("source_lsn", source.get("lsn")),
                                 ("source_tx_id", source.get("txId")),
                                 ("source_ts_ms", source.get("ts_ms")),
                                 ("event_ts_ms", event.get("ts_ms"))):
                result[field] = integer(value, field)
            snapshot = source.get("snapshot")
            if snapshot is not None:
                if isinstance(snapshot, (str, bool)):
                    result["source_snapshot"] = str(snapshot).lower()
                else:
                    errors.append("invalid_source_snapshot")
    result["parse_error"] = ";".join(errors) or None
    return result


def build_bronze_frame(kafka_frame, table: str):
    from pyspark.sql import functions as F, types as T

    types = {"string": T.StringType(), "boolean": T.BooleanType(), "bigint": T.LongType()}
    schema = T.StructType([T.StructField(name, types[kind], True)
                           for name, kind in PARSED_FIELDS])
    parse = F.udf(lambda key, value: parse_event(key, value, table), schema)
    parsed = kafka_frame.withColumn("_cdc", parse("key", "value"))
    return parsed.select(
        F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"), F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").alias("raw_key"), F.col("value").alias("raw_value"),
        *[F.col("_cdc." + name).alias(name) for name, _ in PARSED_FIELDS],
        F.current_timestamp().alias("ingested_at"),
    )


def kafka_bounds(spark, settings: Settings, table: str) -> dict[int, tuple[int, int]]:
    """Read earliest/latest exclusive offsets via Kafka AdminClient, without committing offsets."""
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.setProperty("bootstrap.servers", settings.bootstrap)
    props.setProperty("request.timeout.ms", "20000")
    props.setProperty("default.api.timeout.ms", "30000")
    client = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
    topic = settings.topic(table)
    unit = jvm.java.util.concurrent.TimeUnit.SECONDS
    spec = jvm.org.apache.kafka.clients.admin.OffsetSpec
    try:
        descriptions = client.describeTopics(jvm.java.util.Collections.singletonList(topic))
        description = descriptions.allTopicNames().get(30, unit).get(topic)
        partitions = [info.partition() for info in description.partitions()]
        results = []
        for offset_spec in (spec.earliest(), spec.latest()):
            requested = jvm.java.util.HashMap()
            for partition in partitions:
                requested.put(jvm.org.apache.kafka.common.TopicPartition(topic, partition), offset_spec)
            offsets = client.listOffsets(requested).all().get(30, unit)
            results.append({p: int(offsets.get(jvm.org.apache.kafka.common.TopicPartition(topic, p)).offset())
                            for p in partitions})
        return {p: (results[0][p], results[1][p]) for p in partitions}
    finally:
        client.close()


def check_target(spark, settings: Settings, table: str) -> str:
    """Validate existing DDL, and return the Iceberg table UUID for recovery guards."""
    target = settings.target(table)
    if not spark.catalog.tableExists(target):
        raise BronzeError(f"Missing {target}; apply sql/004_olist_bronze.sql explicitly first")
    actual = tuple((field.name, field.dataType.simpleString()) for field in spark.table(target).schema)
    if actual != BRONZE_FIELDS:
        raise BronzeError(f"{target}: schema differs from sql/004_olist_bronze.sql")
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, target)
    metadata = iceberg.operations().current()
    if (metadata.formatVersion() != 2 or not iceberg.spec().isUnpartitioned()
            or iceberg.properties().get("bronze.contract.version") != "1"):
        raise BronzeError(f"{target}: expected Iceberg V2, unpartitioned layout and contract version 1")
    return str(metadata.uuid())


@contextmanager
def claim_checkpoint(spark, settings: Settings, table: str, table_uuid: str):
    """Prevent concurrent local writers, changed targets and accidental fresh-checkpoint replay."""
    import fcntl

    root = Path(settings.checkpoint_root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / f"{table}.lock").open("a") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise BronzeError(f"{table}: another process holds this checkpoint") from exc
        checkpoint = root / table
        checkpoint.mkdir(exist_ok=True)
        marker = checkpoint / "olist-identity.json"
        identity = {"version": 1, "bootstrap": settings.bootstrap, "topic": settings.topic(table),
                    "target": settings.target(table), "table_uuid": table_uuid}
        if marker.exists():
            if json.loads(marker.read_text(encoding="utf-8")) != identity:
                raise BronzeError(f"{table}: checkpoint identity/table UUID changed; do not reset offsets")
        elif any(checkpoint.iterdir()):
            raise BronzeError(f"{table}: refusing to adopt an unknown non-empty checkpoint")
        has_spark_checkpoint = (checkpoint / "metadata").is_file()
        offset_log = checkpoint / "offsets"
        has_offset_log = offset_log.is_dir() and any(p.name.isdigit() for p in offset_log.iterdir())
        if has_spark_checkpoint and not has_offset_log and spark.table(settings.target(table)).limit(1).count():
            raise BronzeError(f"{table}: non-empty table but checkpoint offset log is missing")
        if not has_spark_checkpoint:
            if spark.table(settings.target(table)).limit(1).count():
                raise BronzeError(f"{table}: table is non-empty but Spark checkpoint is missing")
            bounds = kafka_bounds(spark, settings, table)
            if any(start != 0 for start, _ in bounds.values()):
                raise BronzeError(f"{table}: Kafka earliest offset is not 0; full initial history is unavailable")
        if not marker.exists():
            with marker.open("x", encoding="utf-8") as handle:
                json.dump(identity, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
        try:
            yield checkpoint
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def write_status(settings: Settings, queries, running: bool) -> None:
    payload = {"updated_at": time.time(), "running": running, "tables": list(settings.tables),
               "queries": [{"name": q.name, "id": str(q.id), "run_id": str(q.runId),
                            "active": q.isActive, "progress": q.lastProgress} for q in queries]}
    temporary = settings.status_file.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temporary.replace(settings.status_file)


def run(settings: Settings, available_now: bool = False) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("olist-bronze-stream").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    queries = []
    try:
        # Validate every target before starting any writer.
        uuids = {table: check_target(spark, settings, table) for table in settings.tables}
        with ExitStack() as stack:
            checkpoints = {table: stack.enter_context(claim_checkpoint(spark, settings, table, uuids[table]))
                           for table in settings.tables}
            try:
                for table in settings.tables:
                    source = (spark.readStream.format("kafka")
                              .option("kafka.bootstrap.servers", settings.bootstrap)
                              .option("subscribe", settings.topic(table))
                              .option("startingOffsets", "earliest")
                              .option("failOnDataLoss", "true")
                              .option("maxOffsetsPerTrigger", settings.max_offsets).load())
                    writer = (build_bronze_frame(source, table).writeStream.format("iceberg")
                              .outputMode("append").queryName(f"olist_bronze_{table}")
                              .option("checkpointLocation", str(checkpoints[table])))
                    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(
                        processingTime=f"{settings.trigger_seconds} seconds")
                    query = writer.toTable(settings.target(table))
                    queries.append(query)
                    print(f"Started {query.name}: {settings.topic(table)} -> {settings.target(table)}; "
                          f"checkpoint={checkpoints[table]}", flush=True)
                while not stop.is_set():
                    for query in queries:
                        failure = query.exception()
                        if failure is not None:
                            raise BronzeError(f"{query.name} failed: {failure}")
                        if not available_now and not query.isActive:
                            raise BronzeError(f"{query.name} stopped unexpectedly")
                    write_status(settings, queries, True)
                    if available_now and all(not q.isActive for q in queries):
                        break
                    stop.wait(5)
            finally:
                # Stop all writers before releasing any checkpoint lock, including startup failure.
                for query in queries:
                    if query.isActive:
                        query.stop()
                if queries:
                    write_status(settings, queries, False)
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "run", "health"))
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--tables", help="all or comma-separated table names")
    parser.add_argument("--available-now", action="store_true", help="Drain available Kafka data, then exit")
    args = parser.parse_args()
    try:
        settings = read_settings(args.env_file, args.tables)
        if args.action == "validate":
            print(json.dumps(asdict(settings), indent=2))
            print("Configuration valid; no Spark session, directory, database or stream was created.")
        elif args.action == "health":
            state = json.loads(settings.status_file.read_text(encoding="utf-8"))
            if (not state.get("running") or time.time() - state["updated_at"] > 90
                    or state.get("tables") != list(settings.tables)
                    or len(state.get("queries", [])) != len(settings.tables)
                    or not all(q.get("active") for q in state["queries"])):
                return 1
        else:
            run(settings, args.available_now)
    except (BronzeError, OSError, ValueError, KeyError) as exc:
        print(f"Bronze {args.action} failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
