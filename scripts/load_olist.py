#!/usr/bin/env python3
"""Check eight fixed Olist CSVs, then optionally load an empty PostgreSQL source.

The default/check action reads local files only. Database writes require the
explicit load action. A load commits only after all eight tables reconcile.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path

from profile_olist import SPECS, profile_directory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLES = tuple(SPECS)
PRIMARY_KEYS = {
    "product_category_name_translation": ("product_category_name",),
    "customers": ("customer_id",),
    "sellers": ("seller_id",),
    "products": ("product_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
}


def check_sources(data_dir: Path, profile_path: Path) -> dict:
    """Reprofile the CSVs and require an exact match to the fixed source report."""
    with profile_path.open("r", encoding="utf-8") as source:
        baseline = json.load(source)
    if baseline.get("complete") is not True or baseline.get("errors"):
        raise ValueError(f"Invalid baseline profile: {profile_path}")
    expected_tables = baseline.get("tables", {})
    if set(expected_tables) != set(TABLES):
        raise ValueError("Baseline profile does not describe exactly the eight source tables")

    for table in TABLES:
        expected = expected_tables[table]
        columns = list(SPECS[table]["columns"])
        if expected.get("headers") != columns or expected.get("malformed_rows") != 0:
            raise ValueError(f"Invalid baseline header or malformed rows: {table}")
        if any(expected["columns"][column]["invalid_kind"] for column in columns):
            raise ValueError(f"Invalid source type in baseline: {table}")
        key = next(
            (item for item in expected["candidate_keys"]
             if item["columns"] == list(PRIMARY_KEYS[table])),
            None,
        )
        if key is None or key["empty_rows"] or key["duplicate_rows"]:
            raise ValueError(f"Invalid primary key in baseline: {table}")
        for relation in expected["foreign_keys"]:
            if (relation["parent_table"] != "product_category_name_translation"
                    and relation["orphan_rows"]):
                raise ValueError(f"Orphan foreign key in baseline: {table}")

    observed = profile_directory(data_dir)
    if not observed["complete"]:
        raise ValueError("CSV profiling failed: " + "; ".join(observed["errors"]))
    fields = (
        "exists", "rows", "sha256", "headers", "columns",
        "candidate_keys", "foreign_keys", "malformed_rows",
    )
    differences = [
        f"{table}.{field}"
        for table in TABLES
        for field in fields
        if observed["tables"][table].get(field) != expected_tables[table].get(field)
    ]
    if differences:
        raise ValueError("CSV differs from baseline profile: " + ", ".join(differences))
    return expected_tables


def connection_options(env_file: Path) -> dict:
    """Read local .env; explicitly exported variables take precedence."""
    from dotenv import dotenv_values

    if not env_file.is_file():
        raise ValueError(f"Missing environment file: {env_file}")
    values = {key: value for key, value in dotenv_values(env_file).items()
              if value is not None}
    for key in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD",
                "POSTGRES_HOST", "POSTGRES_PORT"):
        if key in os.environ:
            values[key] = os.environ[key]
    missing = [key for key in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
               if not values.get(key)]
    if missing:
        raise ValueError("Missing PostgreSQL settings: " + ", ".join(missing))
    return {
        "dbname": values["POSTGRES_DB"],
        "user": values["POSTGRES_USER"],
        "password": values["POSTGRES_PASSWORD"],
        "host": values.get("POSTGRES_HOST") or "127.0.0.1",
        "port": values.get("POSTGRES_PORT") or "5433",
    }


def qualified(sql, table: str):
    return sql.Identifier("olist", table)


def verify_schema(cur) -> None:
    for table in TABLES:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            ("olist", table),
        )
        found = [row[0] for row in cur.fetchall()]
        if found != list(SPECS[table]["columns"]):
            raise ValueError(f"PostgreSQL table/column mismatch: olist.{table}")


def require_empty_tables(cur, sql) -> None:
    occupied = []
    for table in TABLES:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(qualified(sql, table)))
        count = cur.fetchone()[0]
        if count:
            occupied.append(f"{table}={count}")
    if occupied:
        raise ValueError("Source tables must all be empty before load: "
                         + ", ".join(occupied))


def normalized_csv_chunks(path: Path, columns: list[str]):
    """Stream CSV with profiler-style blank cells encoded as COPY NULLs."""
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        if next(reader, None) != columns:
            raise ValueError(f"CSV header changed while loading: {path.name}")
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(columns)
        for row in reader:
            if len(row) != len(columns):
                raise ValueError(f"Malformed CSV row while loading: {path.name}")
            # The profiler treats whitespace-only cells as empty. In CSV COPY,
            # an unquoted empty field becomes SQL NULL.
            writer.writerow([None if not value.strip() else value for value in row])
            if buffer.tell() >= 1024 * 1024:
                yield buffer.getvalue().encode("utf-8")
                buffer.seek(0)
                buffer.truncate(0)
        if buffer.tell():
            yield buffer.getvalue().encode("utf-8")


def copy_table(cur, sql, table: str, data_dir: Path) -> None:
    columns = list(SPECS[table]["columns"])
    statement = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
    ).format(
        qualified(sql, table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
    )
    with cur.copy(statement) as sink:
        for chunk in normalized_csv_chunks(data_dir / SPECS[table]["file"], columns):
            sink.write(chunk)

def reconcile_table(cur, sql, table: str, expected: dict) -> int:
    columns = list(SPECS[table]["columns"])
    key = PRIMARY_KEYS[table]
    key_expr = (sql.Identifier(key[0]) if len(key) == 1 else
                sql.SQL("({})").format(sql.SQL(", ").join(map(sql.Identifier, key))))
    bounded = [column for column in columns
               if SPECS[table]["columns"][column] != "text"]
    metrics = [sql.SQL("COUNT(*)"),
               sql.SQL("COUNT(DISTINCT {})").format(key_expr)]
    metrics += [
        sql.SQL("COUNT(*) FILTER (WHERE {} IS NULL)").format(sql.Identifier(column))
        for column in columns
    ]
    for column in bounded:
        metrics.extend([
            sql.SQL("MIN({})").format(sql.Identifier(column)),
            sql.SQL("MAX({})").format(sql.Identifier(column)),
        ])
    cur.execute(
        sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(metrics), qualified(sql, table)
        )
    )
    values = cur.fetchone()
    row_count, distinct_keys = values[:2]
    null_counts = values[2:2 + len(columns)]
    bounds = values[2 + len(columns):]
    if row_count != expected["rows"]:
        raise ValueError(f"olist.{table}: rows {row_count} != CSV {expected['rows']}")
    if distinct_keys != row_count:
        raise ValueError(f"olist.{table}: primary key count differs from row count")
    for column, count in zip(columns, null_counts):
        expected_nulls = expected["columns"][column]["empty"]
        if count != expected_nulls:
            raise ValueError(
                f"olist.{table}.{column}: NULL count {count} != CSV empty {expected_nulls}"
            )
    for index, column in enumerate(bounded):
        actual = tuple(str(value) for value in bounds[2 * index:2 * index + 2])
        expected_bounds = (expected["columns"][column]["min"],
                           expected["columns"][column]["max"])
        if actual != expected_bounds:
            raise ValueError(
                f"olist.{table}.{column}: min/max {actual} != CSV {expected_bounds}"
            )
    return row_count

def reconcile_relations(cur, sql, table: str, expected: dict) -> None:
    for local, parent, parent_field in SPECS[table]["references"]:
        relation = next(
            (item for item in expected["foreign_keys"]
             if (item["column"], item["parent_table"], item["parent_column"])
             == (local, parent, parent_field)),
            None,
        )
        if relation is None:
            raise ValueError(f"Missing baseline relation: {table}.{local}")
        cur.execute(
            sql.SQL(
                "SELECT COUNT(c.{local}), "
                "COUNT(*) FILTER (WHERE c.{local} IS NOT NULL AND p.{parent_field} IS NULL) "
                "FROM {child} AS c LEFT JOIN {parent} AS p "
                "ON c.{local} = p.{parent_field}"
            ).format(
                local=sql.Identifier(local),
                parent_field=sql.Identifier(parent_field),
                child=qualified(sql, table),
                parent=qualified(sql, parent),
            )
        )
        non_empty, orphans = cur.fetchone()
        if (non_empty != relation["non_empty_rows"]
                or orphans != relation["orphan_rows"]):
            raise ValueError(
                f"olist.{table}.{local}: relation counts "
                f"({non_empty}, {orphans}) != CSV "
                f"({relation['non_empty_rows']}, {relation['orphan_rows']})"
            )


def load(data_dir: Path, env_file: Path, expected_tables: dict) -> dict[str, int]:
    import psycopg
    from psycopg import sql

    counts = {}
    with psycopg.connect(**connection_options(env_file)) as conn:
        with conn.cursor() as cur:
            verify_schema(cur)
            cur.execute(
                sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(
                    sql.SQL(", ").join(qualified(sql, table) for table in TABLES)
                )
            )
            require_empty_tables(cur, sql)
            for table in TABLES:
                copy_table(cur, sql, table, data_dir)
            for table in TABLES:
                counts[table] = reconcile_table(cur, sql, table, expected_tables[table])
                reconcile_relations(cur, sql, table, expected_tables[table])
    return counts  # Connection context has committed successfully.


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "load"), nargs="?", default="check")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_ROOT / "data" / "raw")
    parser.add_argument("--profile", type=Path,
                        default=PROJECT_ROOT / "data" / "olist_profile.json")
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    args = parser.parse_args()
    try:
        expected = check_sources(args.data_dir, args.profile)
        if args.action == "check":
            print("CSV check passed; no database connection was made.")
            for table in TABLES:
                print(f"  {table}: {expected[table]['rows']} rows")
            return 0
        counts = load(args.data_dir, args.env_file, expected)
    except Exception as exc:
        print(f"Olist {args.action} failed: {exc}", file=sys.stderr)
        return 2
    print("Olist load committed; all eight tables reconciled:")
    for table, count in counts.items():
        print(f"  {table}: {count} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
