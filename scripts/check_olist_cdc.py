#!/usr/bin/env python3
"""Read-only checks for Olist PostgreSQL CDC and the initial Kafka snapshot."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from olist_cdc import CdcError, PROJECT_ROOT, connect_api, read_settings

TABLES = (
    "customers", "sellers", "products", "orders",
    "order_items", "order_payments", "order_reviews",
)
KEYS = {
    "customers": ("customer_id",),
    "sellers": ("seller_id",),
    "products": ("product_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
}
PUBLICATION = "olist_cdc_pub"
SLOT = "olist_cdc_slot"
CONNECTOR = "olist-postgres-cdc"
TOPIC_PREFIX = "olist_pg.olist."
PLUGIN = "io.debezium.connector.postgresql.PostgresConnector"


def baseline_counts(profile_file: Path) -> dict[str, int]:
    if not profile_file.is_file():
        raise CdcError(f"Missing profile: {profile_file}")
    profile = json.loads(profile_file.read_text(encoding="utf-8"))
    return {table: int(profile["tables"][table]["rows"]) for table in TABLES}


def check_postgres(settings: dict[str, str], expected: dict[str, int],
                   require_slot: bool, require_baseline: bool) -> None:
    import psycopg
    from psycopg import sql

    options = {
        "dbname": settings["POSTGRES_DB"],
        "user": settings["POSTGRES_USER"],
        "password": settings["POSTGRES_PASSWORD"],
        "host": settings.get("POSTGRES_HOST") or "127.0.0.1",
        "port": settings.get("POSTGRES_PORT") or "5433",
    }
    with psycopg.connect(**options) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            wal_level = cur.fetchone()[0]
            cur.execute("SHOW max_replication_slots")
            max_slots = int(cur.fetchone()[0])
            cur.execute("SHOW max_wal_senders")
            max_senders = int(cur.fetchone()[0])
            if wal_level != "logical" or max_slots < 1 or max_senders < 1:
                raise CdcError("PostgreSQL logical replication settings are insufficient")
            print(f"PostgreSQL: wal_level={wal_level}, slots={max_slots}, senders={max_senders}")

            cur.execute(
                "SELECT rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user"
            )
            role = cur.fetchone()
            if not role or not any(role):
                raise CdcError("The .env PostgreSQL role lacks REPLICATION")
            print("PostgreSQL role: REPLICATION privilege available")

            cur.execute(
                "SELECT pubinsert, pubupdate, pubdelete, pubtruncate "
                "FROM pg_publication WHERE pubname = %s",
                (PUBLICATION,),
            )
            flags = cur.fetchone()
            if flags != (True, True, True, False):
                raise CdcError(f"Publication {PUBLICATION} is missing or has wrong operations")
            cur.execute(
                "SELECT schemaname, tablename FROM pg_publication_tables "
                "WHERE pubname = %s",
                (PUBLICATION,),
            )
            published = set(cur.fetchall())
            wanted = {("olist", table) for table in TABLES}
            if published != wanted:
                raise CdcError(f"Publication tables differ: {sorted(published ^ wanted)}")
            print(f"Publication {PUBLICATION}: exactly {len(TABLES)} business tables")

            cur.execute(
                "SELECT plugin, slot_type, database, active FROM pg_replication_slots "
                "WHERE slot_name = %s",
                (SLOT,),
            )
            slot = cur.fetchone()
            if require_slot and slot is None:
                raise CdcError(f"Replication slot {SLOT} has not been created")
            if slot is not None:
                plugin, slot_type, database, active = slot
                if (plugin, slot_type, database) != (
                    "pgoutput", "logical", settings["POSTGRES_DB"]
                ):
                    raise CdcError(f"Replication slot {SLOT} has unexpected properties")
                print(f"Replication slot {SLOT}: pgoutput, active={active}")
            else:
                print(f"Replication slot {SLOT}: not created yet")

            for table in TABLES:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier("olist", table)
                    )
                )
                count = cur.fetchone()[0]
                print(f"olist.{table}: PostgreSQL={count}, CSV baseline={expected[table]}")
                if require_baseline and count != expected[table]:
                    raise CdcError(f"olist.{table} differs from the static CSV baseline")


def check_connect(base_url: str, require_connector: bool) -> None:
    _, plugins = connect_api(base_url, "GET", "/connector-plugins")
    if not isinstance(plugins, list) or PLUGIN not in {
        item.get("class") for item in plugins if isinstance(item, dict)
    }:
        raise CdcError("PostgreSQL Debezium plugin is not available in Kafka Connect")
    print("Kafka Connect: REST API and PostgreSQL plugin available")
    if require_connector:
        code, status = connect_api(base_url, "GET", f"/connectors/{CONNECTOR}/status")
        if code == 404:
            raise CdcError(f"Connector {CONNECTOR} is not registered")
        connector_state = status.get("connector", {}).get("state")
        tasks = status.get("tasks", [])
        if connector_state != "RUNNING" or not tasks or any(
            task.get("state") != "RUNNING" for task in tasks
        ):
            raise CdcError(f"Connector or task is not RUNNING: {connector_state}")
        print(f"Connector {CONNECTOR}: RUNNING, {len(tasks)} task(s) RUNNING")


def compose_command(env_file: Path) -> list[str]:
    return [
        "docker", "compose", "--env-file", str(env_file),
        "-f", str(PROJECT_ROOT / "infra" / "compose.foundation.yml"),
        "-f", str(PROJECT_ROOT / "infra" / "compose.cdc.yml"),
    ]


def kafka_topics(env_file: Path) -> None:
    command = compose_command(env_file) + [
        "exec", "-T", "kafka", "/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "kafka:9092", "--list",
    ]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        raise CdcError("Cannot list Kafka topics; check that Kafka is healthy")
    topics = set(result.stdout.splitlines())
    wanted = {TOPIC_PREFIX + table for table in TABLES}
    found = {topic for topic in topics if topic.startswith(TOPIC_PREFIX)}
    if found != wanted:
        raise CdcError(f"CDC topic mismatch: missing={sorted(wanted - found)}, "
                       f"unexpected={sorted(found - wanted)}")
    print("Kafka: exactly 7 Olist business topics; translation topic absent")


def check_snapshot(env_file: Path, expected: dict[str, int]) -> None:
    for table in TABLES:
        topic = TOPIC_PREFIX + table
        command = compose_command(env_file) + [
            "exec", "-T", "kafka", "/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server", "kafka:9092",
            "--topic", topic,
            "--from-beginning",
            "--max-messages", str(expected[table]),
            "--timeout-ms", "30000",
            "--formatter-property", "print.key=true",
            "--formatter-property", "key.separator=::",
        ]
        count = 0
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8",
        ) as process:
            assert process.stdout is not None
            for line in process.stdout:
                if "::" not in line:
                    raise CdcError(f"{topic}: event has no Kafka key")
                raw_key, raw_value = line.rstrip("\n").split("::", 1)
                try:
                    key = json.loads(raw_key)
                    event = json.loads(raw_value)
                except json.JSONDecodeError as exc:
                    raise CdcError(f"{topic}: invalid JSON event") from exc
                if not isinstance(key, dict) or not isinstance(event, dict):
                    raise CdcError(f"{topic}: key or value is not a JSON object")
                key = key.get("payload", key)
                event = event.get("payload", event)
                if not isinstance(key, dict) or not isinstance(event, dict):
                    raise CdcError(f"{topic}: invalid schema/payload envelope")
                after = event.get("after")
                if event.get("op") != "r" or not isinstance(after, dict):
                    raise CdcError(f"{topic}: expected a snapshot read event (op=r)")
                if any(key.get(column) != after.get(column) for column in KEYS[table]):
                    raise CdcError(f"{topic}: Kafka key differs from the row PK")
                count += 1
            if process.wait() != 0 or count != expected[table]:
                raise CdcError(
                    f"{topic}: read {count} of {expected[table]} expected snapshot events"
                )
        print(f"{topic}: {count} op=r events, keys checked")
    print(f"Snapshot reconciled: {sum(expected.values())} events across 7 topics")


def _db_timestamp_us(value: datetime) -> int:
    """Match Debezium's microsecond encoding for timestamp without time zone."""
    if value.tzinfo is not None:
        raise CdcError("Expected a PostgreSQL timestamp without time zone")
    delta = value - datetime(1970, 1, 1)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _field_matches(source_value, kafka_value) -> bool:
    if isinstance(source_value, Decimal):
        try:
            return source_value == Decimal(str(kafka_value))
        except InvalidOperation:
            return False
    if isinstance(source_value, datetime):
        return _db_timestamp_us(source_value) == kafka_value
    return source_value == kafka_value


def check_snapshot_fields(env_file: Path, settings: dict[str, str],
                          expected: dict[str, int], tables: tuple[str, ...]) -> None:
    """Compare every initial snapshot field with a consistent, read-only PG view."""
    import psycopg
    from psycopg import sql

    options = {
        "dbname": settings["POSTGRES_DB"],
        "user": settings["POSTGRES_USER"],
        "password": settings["POSTGRES_PASSWORD"],
        "host": settings.get("POSTGRES_HOST") or "127.0.0.1",
        "port": settings.get("POSTGRES_PORT") or "5433",
    }
    all_ok = True
    print("Comparing initial Kafka snapshot with current PostgreSQL read-only view.", flush=True)
    with psycopg.connect(**options) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            for table in tables:
                topic = TOPIC_PREFIX + table
                key_columns = KEYS[table]
                cur.execute(sql.SQL("SELECT * FROM {}").format(
                    sql.Identifier("olist", table)
                ))
                columns = tuple(description.name for description in cur.description)
                column_names = set(columns)
                key_indexes = tuple(columns.index(column) for column in key_columns)
                source_rows = {
                    tuple(row[index] for index in key_indexes): row
                    for row in cur.fetchall()
                }
                source_count = len(source_rows)
                if source_count != expected[table]:
                    raise CdcError(
                        f"olist.{table}: source has {source_count} rows; "
                        f"CSV/snapshot baseline is {expected[table]}"
                    )

                command = compose_command(env_file) + [
                    "exec", "-T", "kafka", "/kafka/bin/kafka-console-consumer.sh",
                    "--bootstrap-server", "kafka:9092", "--topic", topic,
                    "--partition", "0", "--offset", "0",
                    "--max-messages", str(expected[table]),
                    "--timeout-ms", "30000",
                    "--formatter-property", "print.key=true",
                    "--formatter-property", "key.separator=::",
                ]
                snapshot_count = duplicate = extra = key_mismatch = 0
                row_mismatch = field_mismatch = column_mismatch = 0
                seen: set[tuple] = set()
                examples: list[str] = []
                process = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    text=True, encoding="utf-8",
                )
                try:
                    assert process.stdout is not None
                    for line in process.stdout:
                        if "::" not in line:
                            raise CdcError(f"{topic}: event has no Kafka key")
                        raw_key, raw_value = line.rstrip("\n").split("::", 1)
                        try:
                            key = json.loads(raw_key)
                            event = json.loads(raw_value)
                        except json.JSONDecodeError as exc:
                            raise CdcError(f"{topic}: invalid JSON event") from exc
                        if not isinstance(key, dict) or not isinstance(event, dict):
                            raise CdcError(f"{topic}: key or value is not a JSON object")
                        key = key.get("payload", key)
                        event = event.get("payload", event)
                        if not isinstance(key, dict) or not isinstance(event, dict):
                            raise CdcError(f"{topic}: invalid schema/payload envelope")
                        after = event.get("after")
                        if event.get("op") != "r" or not isinstance(after, dict):
                            raise CdcError(f"{topic}: expected initial op=r event")
                        snapshot_count += 1
                        pk = tuple(after.get(column) for column in key_columns)
                        if tuple(key.get(column) for column in key_columns) != pk:
                            key_mismatch += 1
                        if pk in seen:
                            duplicate += 1
                            continue
                        seen.add(pk)
                        source_row = source_rows.pop(pk, None)
                        if source_row is None:
                            extra += 1
                            continue
                        shape_diff = sorted(column_names ^ set(after))
                        different_fields = [
                            column for index, column in enumerate(columns)
                            if column in after and not _field_matches(
                                source_row[index], after[column]
                            )
                        ]
                        if shape_diff:
                            column_mismatch += 1
                        if shape_diff or different_fields:
                            row_mismatch += 1
                            field_mismatch += len(shape_diff) + len(different_fields)
                            if len(examples) < 3:
                                examples.append(
                                    f"PK={pk}: columns={shape_diff}; values={different_fields}"
                                )
                    exit_code = process.wait()
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait()
                    if process.stdout is not None:
                        process.stdout.close()
                missing = len(source_rows)
                print(
                    f"olist.{table}: source={source_count}, snapshot={snapshot_count}, "
                    f"distinct_pk={len(seen)}, missing={missing}, extra={extra}, "
                    f"duplicate={duplicate}, key_mismatch={key_mismatch}, "
                    f"column_mismatch={column_mismatch}, row_mismatch={row_mismatch}, "
                    f"field_mismatch={field_mismatch}", flush=True,
                )
                for example in examples:
                    print(f"  {example}", flush=True)
                if (exit_code != 0 or snapshot_count != expected[table] or missing
                        or extra or duplicate or key_mismatch or column_mismatch
                        or row_mismatch):
                    all_ok = False
    if not all_ok:
        raise CdcError("Snapshot field reconciliation found differences")
    print(f"Snapshot fields reconciled: {sum(expected[t] for t in tables)} "
          f"rows across {len(tables)} topic(s)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("preflight", "check", "snapshot", "snapshot-fields"))
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument(
        "--profile", type=Path, default=PROJECT_ROOT / "data" / "olist_profile.json"
    )
    parser.add_argument("--connect-url", default="http://127.0.0.1:8083")
    parser.add_argument("--table", choices=TABLES, help="Only for snapshot-fields")
    args = parser.parse_args()
    try:
        settings = read_settings(args.env_file)
        expected = baseline_counts(args.profile)
        check_postgres(settings, expected, args.action != "preflight",
                       args.action in ("preflight", "snapshot", "snapshot-fields"))
        check_connect(args.connect_url, args.action != "preflight")
        if args.action != "preflight":
            kafka_topics(args.env_file)
        if args.table and args.action != "snapshot-fields":
            raise CdcError("--table is only valid for snapshot-fields")
        if args.action == "snapshot":
            check_snapshot(args.env_file, expected)
        elif args.action == "snapshot-fields":
            tables = (args.table,) if args.table else TABLES
            check_snapshot_fields(args.env_file, settings, expected, tables)
    except (CdcError, OSError, KeyError, ValueError) as exc:
        print(f"CDC {args.action} failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())