#!/usr/bin/env python3
"""Read-only capture/status/reconciliation for seven Olist Silver CDC tables.

capture prints dbt --vars JSON. verify independently replays pinned Bronze in
Python, compares a frozen Silver snapshot, then compares a read-only consistent
PostgreSQL view. No action creates tables, directories, reports or dbt builds.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import sys

from profile_olist import SPECS
from load_olist import connection_options
from ingest_olist_bronze import BRONZE_FIELDS, read_settings

ROOT = Path(__file__).resolve().parents[1]
TABLES = ("customers", "sellers", "products", "orders", "order_items", "order_payments", "order_reviews")
KEYS = {
    "customers": ("customer_id",), "sellers": ("seller_id",), "products": ("product_id",),
    "orders": ("order_id",), "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id")
}
NULLABLE = {
    "customers": set(), "sellers": set(),
    "products": {"product_category_name", "product_name_lenght", "product_description_lenght",
                 "product_photos_qty", "product_weight_g", "product_length_cm",
                 "product_height_cm", "product_width_cm"},
    "orders": {"order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date"},
    "order_items": set(), "order_payments": set(),
    "order_reviews": {"review_comment_title", "review_comment_message", "review_answer_timestamp"}
}
TYPES = {"text": "string", "integer": "int", "decimal": "decimal(12,2)", "timestamp": "timestamp_ntz"}
LINEAGE = (("_kafka_topic", "string"), ("_kafka_partition", "int"), ("_kafka_offset", "bigint"),
           ("_source_lsn", "bigint"), ("_bronze_snapshot_id", "bigint"))
REFERENCE_SCHEMA = (("product_category_name", "string"), ("product_category_name_english", "string"),
                    ("reference_loaded_at", "timestamp"))
RAW = ("kafka_topic", "kafka_partition", "kafka_offset", "key_json", "op", "after_json",
       "before_json", "source_lsn", "is_tombstone", "parse_error", "source_db",
       "source_schema", "source_table", "value_is_null")


class SilverError(Exception):
    """Configuration, contract, data quality or audit failure."""


def integer(value, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value < 2**63:
        raise SilverError(f"Invalid integer: {label}")
    return value


def object_json(value, label: str) -> dict:
    def reject_constant(_):
        raise ValueError("Non-finite JSON")
    try:
        result = json.loads(value, parse_float=Decimal, parse_constant=reject_constant)
    except (TypeError, ValueError, RecursionError) as exc:
        raise SilverError(f"Invalid JSON object: {label}") from exc
    if not isinstance(result, dict):
        raise SilverError(f"Invalid JSON object: {label}")
    return result


def canonical_value(value, kind: str, raw_timestamp: bool = False):
    """Lossless canonical business values; never infer a timezone or round money."""
    if value is None:
        return None
    if kind == "text":
        if not isinstance(value, str):
            raise SilverError("Expected a text business value")
        return value
    if kind == "integer":
        if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
            value = int(value)
        number = integer(value, "business integer", 0) if isinstance(value, int) else _bad("integer")
        return number if number < 2**31 else _bad("32-bit source integer")
    if kind == "decimal":
        if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
            raise SilverError("Expected exact Decimal/text money, not floating point")
        try:
            number = Decimal(value)
            if not number.is_finite() or number < 0 or number >= Decimal("10000000000"):
                raise InvalidOperation()
            scaled = number.quantize(Decimal("0.01"))
            if scaled != number:
                raise InvalidOperation()
            return format(scaled, ".2f")
        except (InvalidOperation, ValueError) as exc:
            raise SilverError("Money is not a non-negative DECIMAL(12,2)") from exc
    if kind == "timestamp":
        if raw_timestamp:
            if isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
                value = int(value)
            if isinstance(value, bool) or not isinstance(value, int):
                raise SilverError("Expected Debezium timestamp microseconds")
            try:
                value = datetime(1970, 1, 1) + timedelta(microseconds=value)
            except (OverflowError, ValueError) as exc:
                raise SilverError("Timestamp outside Python/Silver year 1..9999") from exc
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError as exc:
                raise SilverError("Invalid business timestamp") from exc
        if not isinstance(value, datetime) or value.tzinfo is not None:
            raise SilverError("Expected timestamp without timezone; no implicit UTC conversion")
        return value.isoformat(sep=" ", timespec="microseconds")
    raise SilverError(f"Unknown business kind: {kind}")


def _bad(kind):
    raise SilverError(f"Invalid {kind}")


def canonical_row(table: str, values, raw_timestamp: bool = False) -> tuple:
    columns = SPECS[table]["columns"]
    if isinstance(values, dict):
        if set(values) != set(columns):
            raise SilverError(f"{table}: payload fields differ from the source contract")
        values = [values[name] for name in columns]
    if len(values) != len(columns):
        raise SilverError(f"{table}: business column count differs")
    result = []
    for (name, kind), value in zip(columns.items(), values):
        if value is None and name not in NULLABLE.get(table, set()):
            raise SilverError(f"{table}.{name}: unexpected NULL")
        normalized = canonical_value(value, kind, raw_timestamp)
        if normalized is not None and table == "products" and kind == "integer" and normalized < 0:
            raise SilverError(f"{table}.{name}: negative product attribute")
        if normalized is not None and table == "order_reviews" and name == "review_score" and not 1 <= normalized <= 5:
            raise SilverError(f"{table}.{name}: review score outside 1..5")
        result.append(normalized)
    return tuple(result)


def business_key(table: str, values: dict) -> tuple:
    result = []
    for column in KEYS[table]:
        value = canonical_value(values.get(column), SPECS[table]["columns"][column])
        if value is None or (isinstance(value, str) and not 1 <= len(value) <= 32):
            raise SilverError(f"{table}: missing/invalid business key")
        if isinstance(value, int) and value < 1:
            raise SilverError(f"{table}: non-positive business key")
        result.append(value)
    return tuple(result)


def key_of_row(table: str, row: tuple) -> tuple:
    columns = tuple(SPECS[table]["columns"])
    return tuple(row[columns.index(name)] for name in KEYS[table])


def business_digest(table: str, rows: dict) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(list(SPECS[table]["columns"])).encode())
    for key in sorted(rows):
        digest.update(json.dumps(rows[key], ensure_ascii=False, separators=(",", ":")).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def compare_rows(expected: dict, actual: dict, columns: tuple) -> dict:
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    bad_rows = 0
    bad_columns = {}
    samples = []
    for key in expected.keys() & actual.keys():
        different = [name for name, left, right in zip(columns, expected[key], actual[key]) if left != right]
        if different:
            bad_rows += 1
            for name in different:
                bad_columns[name] = bad_columns.get(name, 0) + 1
            if len(samples) < 5:
                samples.append({"key": key, "columns": different})
    return {"missing": len(missing), "extra": len(extra), "mismatched_rows": bad_rows,
            "mismatched_columns": bad_columns, "samples": samples,
            "missing_key_samples": list(sorted(missing))[:5], "extra_key_samples": list(sorted(extra))[:5]}


def replay_events(records, table: str, topic: str, snapshot_id: int) -> tuple[dict, dict, dict]:
    """Independent Python replay; offsets are ordered only within each key's partition."""
    latest, partitions, seen = {}, {}, set()
    databases = set()
    events = tombstones = 0
    for values in records:
        event = dict(zip(RAW, values))
        events += 1
        partition = integer(event["kafka_partition"], "partition")
        offset = integer(event["kafka_offset"], "offset")
        identity = (event["kafka_topic"], partition, offset)
        if identity in seen or identity[0] != topic or event["parse_error"] is not None:
            raise SilverError(f"{table}: duplicate/invalid Kafka identity or parse error")
        seen.add(identity)
        key_json = object_json(event["key_json"], "Kafka key")
        if set(key_json) != set(KEYS[table]):
            raise SilverError(f"{table}: Kafka key fields differ")
        key = business_key(table, key_json)
        if key in partitions and partitions[key] != partition:
            raise SilverError(f"{table}: a key spans partitions; ordering is undefined")
        partitions[key] = partition
        if not isinstance(event["is_tombstone"], bool) or not isinstance(event["value_is_null"], bool):
            raise SilverError(f"{table}: invalid tombstone metadata")
        if event["is_tombstone"]:
            if not event["value_is_null"] or any(event[name] is not None for name in ("op", "after_json", "before_json")):
                raise SilverError(f"{table}: malformed tombstone")
            tombstones += 1
            continue
        if event["value_is_null"] or event["op"] not in ("r", "c", "u", "d"):
            raise SilverError(f"{table}: invalid CDC operation/value")
        if event["source_schema"] != "olist" or event["source_table"] != table or not isinstance(event["source_db"], str) or not event["source_db"]:
            raise SilverError(f"{table}: source identity differs")
        databases.add(event["source_db"])
        if event["source_lsn"] is not None:
            integer(event["source_lsn"], "source_lsn")
        deleted = event["op"] == "d"
        if deleted and event["after_json"] is not None:
            raise SilverError(f"{table}: delete has after payload")
        payload = object_json(event["before_json"] if deleted else event["after_json"], "row payload")
        if business_key(table, payload) != key:
            raise SilverError(f"{table}: Kafka key differs from row key")
        row = None if deleted else canonical_row(table, payload, raw_timestamp=True)
        lineage = (topic, partition, offset, event["source_lsn"], snapshot_id)
        if key not in latest or offset > latest[key][0]:
            latest[key] = (offset, row, lineage)
    if events == 0 or len(databases) != 1:
        raise SilverError(f"{table}: expected nonempty Bronze and one source database")
    rows = {key: value[1] for key, value in latest.items() if value[1] is not None}
    lineage = {key: value[2] for key, value in latest.items() if value[1] is not None}
    return rows, lineage, {"events": events, "tombstones": tombstones,
                           "latest_deleted_keys": sum(value[1] is None for value in latest.values()),
                           "source_db": next(iter(databases))}


class SparkReader:
    def __init__(self, connection):
        self.connection = connection

    def query(self, sql: str) -> list:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()

    def stream(self, sql: str):
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            while True:
                rows = cursor.fetchmany(2000)
                if not rows:
                    return
                yield from rows
        finally:
            cursor.close()

    def properties(self, target: str) -> dict:
        return {str(row[0]): str(row[1]) for row in self.query("SHOW TBLPROPERTIES " + target)}

    def schema(self, target: str) -> tuple:
        return tuple((str(row[0]), re.sub(r"\s+", "", str(row[1]).lower()))
                     for row in self.query("DESCRIBE TABLE " + target)
                     if row[0] and not str(row[0]).startswith("#"))

    def snapshot(self, target: str):
        rows = self.query(f"SELECT snapshot_id FROM {target}.refs WHERE name = 'main'")
        return integer(int(rows[0][0]), "snapshot ID", 1) if rows else None


def require_schema(reader: SparkReader, target: str, expected: tuple):
    if reader.schema(target) != expected:
        raise SilverError(f"{target}: names/types/order differ from the expected contract")


def freeze_silver(reader: SparkReader, table: str) -> dict:
    target = "lakehouse.silver." + table
    expected = tuple((name, TYPES[kind]) for name, kind in SPECS[table]["columns"].items()) + LINEAGE
    require_schema(reader, target, expected)
    properties = reader.properties(target)
    if properties.get("silver.contract.version") != "1" or properties.get("silver.source.table") != table:
        raise SilverError(f"{target}: missing/wrong Silver contract properties")
    raw = properties.get("silver.bronze.snapshot-id", "")
    if not re.fullmatch(r"[1-9][0-9]*", raw):
        raise SilverError(f"{target}: missing/wrong Bronze snapshot property")
    snapshot = reader.snapshot(target)
    if snapshot is None and reader.query("SELECT count(*) FROM " + target)[0][0] != 0:
        raise SilverError(f"{target}: nonempty table without a main snapshot")
    return {"table": target, "snapshot_id": snapshot,
            "bronze_snapshot_id": integer(int(raw), "Bronze snapshot", 1), "properties": properties}


def frozen_from(state: dict) -> str:
    if state["snapshot_id"] is None:
        return state["table"] + " WHERE 1 = 0"
    return state["table"] + " VERSION AS OF " + str(state["snapshot_id"])


def ensure_unchanged(reader: SparkReader, table: str, state: dict):
    current = freeze_silver(reader, table)
    if current != state:
        raise SilverError(f"{state['table']}: changed during audit; rerun without concurrent builds")


def capture(reader: SparkReader, prefix: str) -> dict:
    result = {}
    for table in TABLES:
        target = "lakehouse.bronze." + table
        require_schema(reader, target, BRONZE_FIELDS)
        if reader.properties(target).get("bronze.contract.version") != "1":
            raise SilverError(f"{target}: expected Bronze contract version 1")
        snapshot = reader.snapshot(target)
        if snapshot is None:
            raise SilverError(f"{target}: no committed input snapshot")
        result[table] = snapshot
    return {"bronze_snapshots": result, "bronze_topic_prefix": prefix}


def status(reader: SparkReader):
    for table in TABLES:
        state = freeze_silver(reader, table)
        keys = ", ".join(KEYS[table])
        unique = KEYS[table][0] if len(KEYS[table]) == 1 else "struct(" + keys + ")"
        counts = reader.query("SELECT count(*), count(DISTINCT " + unique + ") FROM " + frozen_from(state))[0]
        ensure_unchanged(reader, table, state)
        print(json.dumps({key: value for key, value in state.items() if key != "properties"} |
                         {"rows": counts[0], "distinct_keys": counts[1]}, sort_keys=True))



def reference_status(reader: SparkReader, env_file: Path) -> bool:
    """Read-only reconciliation for the static translation reference."""
    import psycopg
    from psycopg import sql

    target = "lakehouse.silver.product_category_name_translation"
    require_schema(reader, target, REFERENCE_SCHEMA)
    properties = reader.properties(target)
    if (properties.get("silver.contract.version") != "1"
            or properties.get("silver.source.table") != "product_category_name_translation"):
        raise SilverError(f"{target}: missing/wrong static-reference contract properties")
    actual = {}
    invalid = 0
    for values in reader.stream("SELECT product_category_name, product_category_name_english, reference_loaded_at FROM " + target):
        key, english, loaded_at = values
        if not isinstance(key, str) or not key or key in actual or loaded_at is None:
            invalid += 1
            continue
        if english is not None and not isinstance(english, str):
            invalid += 1
            continue
        actual[key] = (key, english)
    with psycopg.connect(**connection_options(env_file), connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT product_category_name, product_category_name_english FROM {}")
                           .format(sql.Identifier("olist", "product_category_name_translation")))
            source = {}
            for key, english in cursor:
                if not isinstance(key, str) or not key or key in source:
                    raise SilverError("Reference PostgreSQL key is missing or duplicated")
                source[key] = (key, english)
    comparison = compare_rows(source, actual, ("product_category_name", "product_category_name_english"))
    passed = invalid == 0 and not any(comparison[name] for name in ("missing", "extra", "mismatched_rows"))
    report = {"contract_version": 1, "table": target, "silver_rows": len(actual),
              "postgres_rows": len(source), "invalid_rows": invalid,
              "comparison": comparison, "passed": passed,
              "scope": "static JDBC reference versus read-only PostgreSQL table"}
    print(json.dumps(report, ensure_ascii=False, sort_keys=True), flush=True)
    return passed

def baseline_reports(path: Path | None) -> dict:
    if path is None:
        return {}
    reports = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        report = json.loads(line)
        if report.get("contract_version") != 1 or not report.get("passed") or "business_digest" not in report:
            raise SilverError("Baseline must contain successful Silver verify JSON lines")
        name = report["table"]
        if name in reports:
            raise SilverError("Duplicate table in baseline")
        reports[name] = report
    if set(reports) != {"lakehouse.silver." + table for table in TABLES}:
        raise SilverError("Baseline must cover exactly the seven Silver CDC tables")
    return reports


def verify(reader: SparkReader, env_file: Path, prefix: str, baseline: dict) -> bool:
    import psycopg
    from psycopg import sql

    states = {table: freeze_silver(reader, table) for table in TABLES}
    passed = True
    reports = []
    with psycopg.connect(**connection_options(env_file), connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute("SELECT current_database(), pg_current_snapshot()::text")
            database, pg_snapshot = cursor.fetchone()
            for table in TABLES:
                state = states[table]
                columns = tuple(SPECS[table]["columns"])
                bronze = "lakehouse.bronze." + table
                require_schema(reader, bronze, BRONZE_FIELDS)
                selection = ", ".join(RAW[:-1]) + ", (raw_value IS NULL) AS value_is_null"
                replay, expected_lineage, replay_info = replay_events(
                    reader.stream(f"SELECT {selection} FROM {bronze} VERSION AS OF {state['bronze_snapshot_id']}"),
                    table, prefix + table, state["bronze_snapshot_id"])
                if replay_info["source_db"] != database:
                    raise SilverError(f"{table}: Bronze source database differs from PostgreSQL")
                actual, actual_lineage = {}, {}
                duplicates = invalid_lineage = actual_count = 0
                for values in reader.stream("SELECT " + ", ".join(columns + tuple(n for n, _ in LINEAGE)) + " FROM " + frozen_from(state)):
                    actual_count += 1
                    row = canonical_row(table, values[:len(columns)])
                    key = key_of_row(table, row)
                    if key in actual:
                        duplicates += 1
                    actual[key] = row
                    metadata = tuple(values[len(columns):])
                    actual_lineage[key] = metadata
                    valid = metadata[0] == prefix + table
                    valid = valid and type(metadata[1]) is int and metadata[1] >= 0
                    valid = valid and type(metadata[2]) is int and metadata[2] >= 0
                    valid = valid and (metadata[3] is None or (type(metadata[3]) is int and metadata[3] >= 0))
                    valid = valid and type(metadata[4]) is int and metadata[4] == state["bronze_snapshot_id"]
                    invalid_lineage += int(not valid)
                cursor.execute(sql.SQL("SELECT {} FROM {}").format(
                    sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier("olist", table)))
                source = {}
                for values in cursor:
                    row = canonical_row(table, values)
                    key = key_of_row(table, row)
                    if key in source:
                        raise SilverError(f"{table}: duplicate PostgreSQL primary key")
                    source[key] = row
                bronze_comparison = compare_rows(replay, actual, columns)
                pg_comparison = compare_rows(source, actual, columns)
                lineage_mismatch = sum(expected_lineage[key] != actual_lineage[key] for key in replay.keys() & actual.keys())
                digest = business_digest(table, actual)
                old = baseline.get(state["table"])
                baseline_changed = bool(old and (old["bronze_snapshot_id"] != state["bronze_snapshot_id"]
                                                or old["business_digest"] != digest or old["silver_rows"] != actual_count))
                ok = not any((duplicates, invalid_lineage, lineage_mismatch, baseline_changed,
                              *(bronze_comparison[name] for name in ("missing", "extra", "mismatched_rows")),
                              *(pg_comparison[name] for name in ("missing", "extra", "mismatched_rows"))))
                report = {"contract_version": 1, "table": state["table"], "snapshot_id": state["snapshot_id"],
                          "bronze_snapshot_id": state["bronze_snapshot_id"], "postgres_snapshot": pg_snapshot,
                          "silver_rows": actual_count, "postgres_rows": len(source), "bronze_current_rows": len(replay),
                          "bronze_replay": replay_info, "duplicate_rows": duplicates,
                          "invalid_lineage": invalid_lineage, "lineage_mismatched_rows": lineage_mismatch,
                          "bronze_comparison": bronze_comparison, "postgres_comparison": pg_comparison,
                          "business_digest": digest, "baseline_changed": baseline_changed, "passed": ok,
                          "scope": "pinned Bronze replay versus frozen Silver and current read-only PostgreSQL view"}
                ensure_unchanged(reader, table, state)
                reports.append(report)
                passed = ok and passed
            # Catch a rebuild of an earlier table while later tables were checked.
            for table in TABLES:
                ensure_unchanged(reader, table, states[table])
    for report in reports:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True), flush=True)
    return passed


@contextmanager
def spark_connection(host: str, port: int):
    from pyhive import hive
    connection = hive.Connection(host=host, port=port, username="dbt", auth="NOSASL", database="default",
                                 configuration={"spark.sql.defaultCatalog": "lakehouse", "spark.sql.session.timeZone": "UTC"})
    try:
        reader = SparkReader(connection)
        if reader.query("SELECT current_catalog(), current_timezone()")[0] != ("lakehouse", "UTC"):
            raise SilverError("Checker requires lakehouse catalog and UTC session")
        yield reader
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "capture", "status", "reference", "verify"))
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--host", default=os.environ.get("DBT_SPARK_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DBT_SPARK_PORT", "10000")))
    parser.add_argument("--baseline", type=Path, help="Compare against previous successful verify JSON lines")
    args = parser.parse_args()
    try:
        if not args.host or not 1 <= args.port <= 65535 or (args.baseline and args.action != "verify"):
            raise SilverError("Invalid host/port or --baseline action")
        settings = read_settings(args.env_file)
        baseline = baseline_reports(args.baseline)
        if args.action == "validate":
            print("Silver checker configuration valid; no Spark/PostgreSQL connection opened.")
            return 0
        with spark_connection(args.host, args.port) as reader:
            if args.action == "capture":
                print(json.dumps(capture(reader, settings.topic_prefix), sort_keys=True))
            elif args.action == "status":
                status(reader)
            elif args.action == "reference":
                return 0 if reference_status(reader, args.env_file) else 1
            else:
                return 0 if verify(reader, args.env_file, settings.topic_prefix, baseline) else 1
    except Exception as exc:
        print(f"Silver {args.action} failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
