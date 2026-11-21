#!/usr/bin/env python3
"""Read-only status and reconciliation for the Gold star schema."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from decimal import Decimal
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
TABLES = (
    "dim_date", "dim_customer", "dim_seller", "dim_category", "dim_product",
    "dim_order_status", "dim_payment_type", "fct_orders", "fct_order_items",
    "fct_order_payments", "fct_order_reviews", "mart_orders_monthly",
    "mart_orders_by_state", "mart_order_status", "mart_product_sales",
    "mart_category_share_monthly", "mart_seller_sales", "mart_customer_value",
    "mart_delivery_performance", "mart_delivery_stages", "mart_delivery_review",
)


class GoldError(Exception):
    pass


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


def scalar(reader: SparkReader, sql: str):
    rows = reader.query(sql)
    if not rows:
        raise GoldError("Query returned no scalar row")
    return rows[0][0]


def validate() -> None:
    if not TABLES or any(not name.isidentifier() for name in TABLES):
        raise GoldError("Invalid Gold table registry")
    print("Gold checker configuration valid; no Spark connection opened.")


def status(reader: SparkReader) -> None:
    for name in TABLES:
        rows = scalar(reader, f"SELECT count(*) FROM lakehouse.gold.{name}")
        print(json.dumps({"table": name, "rows": int(rows)}, sort_keys=True))


def verify(reader: SparkReader) -> bool:
    checks = {
        "fct_orders_duplicate": scalar(reader, """
            SELECT count(*) FROM (
              SELECT order_id FROM lakehouse.gold.fct_orders
              GROUP BY order_id HAVING count(*) > 1
            )
        """),
        "fct_order_items_duplicate": scalar(reader, """
            SELECT count(*) FROM (
              SELECT order_item_key FROM lakehouse.gold.fct_order_items
              GROUP BY order_item_key HAVING count(*) > 1
            )
        """),
        "fct_order_payments_duplicate": scalar(reader, """
            SELECT count(*) FROM (
              SELECT payment_key FROM lakehouse.gold.fct_order_payments
              GROUP BY payment_key HAVING count(*) > 1
            )
        """),
        "fct_order_reviews_duplicate": scalar(reader, """
            SELECT count(*) FROM (
              SELECT review_key FROM lakehouse.gold.fct_order_reviews
              GROUP BY review_key HAVING count(*) > 1
            )
        """),
        "fct_orders_value_formula": scalar(reader, """
            SELECT count(*) FROM lakehouse.gold.fct_orders
            WHERE order_value <> item_value + freight_value
        """),
        "monthly_order_count_delta": scalar(reader, """
            SELECT abs((SELECT count(*) FROM lakehouse.gold.fct_orders)
                     - (SELECT coalesce(sum(order_count), 0) FROM lakehouse.gold.mart_orders_monthly))
        """),
        "state_order_count_delta": scalar(reader, """
            SELECT abs((SELECT count(*) FROM lakehouse.gold.fct_orders)
                     - (SELECT coalesce(sum(order_count), 0) FROM lakehouse.gold.mart_orders_by_state))
        """),
        "status_order_count_delta": scalar(reader, """
            SELECT abs((SELECT count(*) FROM lakehouse.gold.fct_orders)
                     - (SELECT coalesce(sum(order_count), 0) FROM lakehouse.gold.mart_order_status))
        """),
        "monthly_value_delta": scalar(reader, """
            SELECT abs((SELECT coalesce(sum(order_value), 0) FROM lakehouse.gold.fct_orders)
                     - (SELECT coalesce(sum(order_value), 0) FROM lakehouse.gold.mart_orders_monthly))
        """),
        "state_value_delta": scalar(reader, """
            SELECT abs((SELECT coalesce(sum(order_value), 0) FROM lakehouse.gold.fct_orders)
                     - (SELECT coalesce(sum(order_value), 0) FROM lakehouse.gold.mart_orders_by_state))
        """),
        "status_value_delta": scalar(reader, """
            SELECT abs((SELECT coalesce(sum(order_value), 0) FROM lakehouse.gold.fct_orders)
                     - (SELECT coalesce(sum(order_value), 0) FROM lakehouse.gold.mart_order_status))
        """),
        "status_share_delta": scalar(reader, """
            SELECT abs(coalesce(sum(order_share), 0) - cast(1 as decimal(9,6)))
            FROM lakehouse.gold.mart_order_status
        """),
        "category_share_bad_months": scalar(reader, """
            SELECT count(*) FROM (
              SELECT order_purchase_month
              FROM lakehouse.gold.mart_category_share_monthly
              GROUP BY order_purchase_month
              HAVING abs(sum(category_share) - cast(1 as decimal(9,6))) > cast(0.000010 as decimal(9,6))
            )
        """),
    }
    normalized = {key: str(value) for key, value in checks.items()}
    bad = {}
    for key, value in checks.items():
        try:
            bad[key] = Decimal(str(value)) != 0
        except Exception as exc:
            raise GoldError(f"Non-numeric check result for {key}: {value}") from exc
    report = {"contract_version": 1, "checks": normalized, "passed": not any(bad.values())}
    print(json.dumps(report, ensure_ascii=False, sort_keys=True), flush=True)
    return report["passed"]


@contextmanager
def spark_connection(host: str, port: int):
    from pyhive import hive
    connection = hive.Connection(
        host=host, port=port, username="dbt", auth="NOSASL", database="default",
        configuration={"spark.sql.defaultCatalog": "lakehouse", "spark.sql.session.timeZone": "UTC"},
    )
    try:
        reader = SparkReader(connection)
        if reader.query("SELECT current_catalog(), current_timezone()")[0] != ("lakehouse", "UTC"):
            raise GoldError("Checker requires lakehouse catalog and UTC session")
        yield reader
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "status", "verify"))
    parser.add_argument("--host", default=os.environ.get("DBT_SPARK_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DBT_SPARK_PORT", "10000")))
    args = parser.parse_args()
    try:
        if not args.host or not 1 <= args.port <= 65535:
            raise GoldError("Invalid host/port")
        if args.action == "validate":
            validate()
            return 0
        with spark_connection(args.host, args.port) as reader:
            if args.action == "status":
                status(reader)
                return 0
            return 0 if verify(reader) else 1
    except Exception as exc:
        print(f"Gold {args.action} failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
