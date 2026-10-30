#!/usr/bin/env python3
"""Profile the eight Olist CSV files selected for the CĐTN source database.

This command reads CSVs only. It never changes source files or PostgreSQL.
Duplicate key and foreign-key counts are observations, not silent row filters.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


SPECS = {
    "product_category_name_translation": {
        "file": "product_category_name_translation.csv",
        "columns": {
            "product_category_name": "text",
            "product_category_name_english": "text",
        },
        "keys": [("product_category_name",)],
        "references": [],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "columns": {
            "customer_id": "text",
            "customer_unique_id": "text",
            "customer_zip_code_prefix": "text",
            "customer_city": "text",
            "customer_state": "text",
        },
        "keys": [("customer_id",)],
        "references": [],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "columns": {
            "seller_id": "text",
            "seller_zip_code_prefix": "text",
            "seller_city": "text",
            "seller_state": "text",
        },
        "keys": [("seller_id",)],
        "references": [],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "columns": {
            "product_id": "text",
            "product_category_name": "text",
            "product_name_lenght": "integer",
            "product_description_lenght": "integer",
            "product_photos_qty": "integer",
            "product_weight_g": "integer",
            "product_length_cm": "integer",
            "product_height_cm": "integer",
            "product_width_cm": "integer",
        },
        "keys": [("product_id",)],
        "references": [("product_category_name", "product_category_name_translation", "product_category_name")],
    },
    "orders": {
        "file": "olist_orders_dataset.csv",
        "columns": {
            "order_id": "text",
            "customer_id": "text",
            "order_status": "text",
            "order_purchase_timestamp": "timestamp",
            "order_approved_at": "timestamp",
            "order_delivered_carrier_date": "timestamp",
            "order_delivered_customer_date": "timestamp",
            "order_estimated_delivery_date": "timestamp",
        },
        "keys": [("order_id",)],
        "references": [("customer_id", "customers", "customer_id")],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "columns": {
            "order_id": "text",
            "order_item_id": "integer",
            "product_id": "text",
            "seller_id": "text",
            "shipping_limit_date": "timestamp",
            "price": "decimal",
            "freight_value": "decimal",
        },
        "keys": [("order_id", "order_item_id")],
        "references": [
            ("order_id", "orders", "order_id"),
            ("product_id", "products", "product_id"),
            ("seller_id", "sellers", "seller_id"),
        ],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "columns": {
            "order_id": "text",
            "payment_sequential": "integer",
            "payment_type": "text",
            "payment_installments": "integer",
            "payment_value": "decimal",
        },
        "keys": [("order_id", "payment_sequential")],
        "references": [("order_id", "orders", "order_id")],
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "columns": {
            "review_id": "text",
            "order_id": "text",
            "review_score": "integer",
            "review_comment_title": "text",
            "review_comment_message": "text",
            "review_creation_date": "timestamp",
            "review_answer_timestamp": "timestamp",
        },
        "keys": [("review_id",), ("review_id", "order_id")],
        "references": [("order_id", "orders", "order_id")],
    },
}

REFERENCE_FIELDS = {
    parent: {parent_field for spec in SPECS.values()
             for _, parent_table, parent_field in spec["references"]
             if parent_table == parent}
    for parent in SPECS
}


def parse_value(value: str, kind: str):
    if kind == "integer":
        return int(value)
    if kind == "decimal":
        number = Decimal(value)
        if not number.is_finite():
            raise ValueError("non-finite decimal")
        return number
    if kind == "timestamp":
        return datetime.fromisoformat(value)
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def profile_directory(data_dir: Path) -> dict:
    report = {
        "source": "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(data_dir.resolve()),
        "tables": {},
        "errors": [],
    }
    parent_values = {table: {field: set() for field in fields}
                     for table, fields in REFERENCE_FIELDS.items()}
    foreign_counts = {}

    for table, spec in SPECS.items():
        path = data_dir / spec["file"]
        result = {
            "file": spec["file"],
            "exists": path.is_file(),
            "rows": 0,
            "sha256": None,
            "headers": [],
            "columns": {
                column: {"expected_kind": kind, "empty": 0, "invalid_kind": 0}
                for column, kind in spec["columns"].items()
            },
            "candidate_keys": [],
            "foreign_keys": [],
            "malformed_rows": 0,
        }
        report["tables"][table] = result
        if not path.is_file():
            report["errors"].append(f"Missing file: {path}")
            continue

        seen_keys = [set() for _ in spec["keys"]]
        key_counts = [{"columns": list(key), "empty_rows": 0, "duplicate_rows": 0}
                      for key in spec["keys"]]
        result["candidate_keys"] = key_counts
        bounds = {}
        relation_counts = {relation: Counter() for relation in spec["references"]}
        foreign_counts[table] = relation_counts

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as source:
                reader = csv.DictReader(source)
                headers = reader.fieldnames or []
                result["headers"] = headers
                missing = sorted(set(spec["columns"]) - set(headers))
                extra = sorted(set(headers) - set(spec["columns"]))
                if missing or extra or len(headers) != len(set(headers)):
                    report["errors"].append(
                        f"{path.name}: header mismatch; missing={missing}, extra={extra}, "
                        f"duplicate_headers={len(headers) != len(set(headers))}"
                    )
                    continue

                for row in reader:
                    result["rows"] += 1
                    if None in row or any(value is None for value in row.values()):
                        result["malformed_rows"] += 1
                        continue
                    clean = {column: value.strip() for column, value in row.items()}

                    for column, kind in spec["columns"].items():
                        value = clean[column]
                        stats = result["columns"][column]
                        if not value:
                            stats["empty"] += 1
                            continue
                        if kind == "text":
                            stats["max_length"] = max(stats.get("max_length", 0), len(value))
                        else:
                            try:
                                parsed = parse_value(value, kind)
                            except (ValueError, InvalidOperation, OverflowError):
                                stats["invalid_kind"] += 1
                                continue
                            low, high = bounds.get(column, (parsed, parsed))
                            bounds[column] = (min(low, parsed), max(high, parsed))
                            if kind == "decimal":
                                _, digits, exponent = parsed.as_tuple()
                                stats["max_integer_digits"] = max(
                                    stats.get("max_integer_digits", 0),
                                    max(1, len(digits) + exponent),
                                )
                                stats["max_scale"] = max(
                                    stats.get("max_scale", 0), max(0, -exponent)
                                )

                    for position, key in enumerate(spec["keys"]):
                        values = tuple(clean[column] for column in key)
                        if any(not value for value in values):
                            key_counts[position]["empty_rows"] += 1
                        elif values in seen_keys[position]:
                            key_counts[position]["duplicate_rows"] += 1
                        else:
                            seen_keys[position].add(values)

                    for field in parent_values[table]:
                        if clean[field]:
                            parent_values[table][field].add(clean[field])
                    for relation in spec["references"]:
                        value = clean[relation[0]]
                        if value:
                            relation_counts[relation][value] += 1
        except (UnicodeError, csv.Error, OSError) as exc:
            report["errors"].append(f"{path.name}: {exc}")
        result["sha256"] = sha256_file(path)
        for column, (low, high) in bounds.items():
            result["columns"][column]["min"] = str(low)
            result["columns"][column]["max"] = str(high)

    for table, spec in SPECS.items():
        result = report["tables"][table]
        if not result["exists"] or table not in foreign_counts:
            continue
        for local, parent, parent_field in spec["references"]:
            counts = foreign_counts[table][(local, parent, parent_field)]
            known = parent_values[parent][parent_field]
            result["foreign_keys"].append({
                "column": local,
                "parent_table": parent,
                "parent_column": parent_field,
                "non_empty_rows": sum(counts.values()),
                "orphan_rows": sum(count for value, count in counts.items() if value not in known),
            })

    report["complete"] = not report["errors"]
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True,
                        help="Directory containing the eight Olist CSV files")
    parser.add_argument("--output", type=Path,
                        help="Write JSON report here; default: stdout")
    args = parser.parse_args()
    report = profile_directory(args.data_dir)
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
        print(f"Profile written to {args.output}; complete={report['complete']}")
    else:
        sys.stdout.write(rendered)
    return 0 if report["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())