"""Tests for the source profiler using tiny, local CSV fixtures."""

import csv
import tempfile
import unittest
from pathlib import Path

from scripts.profile_olist import SPECS, profile_directory


BASE_ROWS = {
    "product_category_name_translation": {
        "product_category_name": "cama_mesa_banho",
        "product_category_name_english": "bed_bath_table",
    },
    "customers": {"customer_id": "c1", "customer_unique_id": "u1"},
    "sellers": {"seller_id": "s1"},
    "products": {"product_id": "p1", "product_category_name": "cama_mesa_banho"},
    "orders": {
        "order_id": "o1",
        "customer_id": "c1",
        "order_status": "delivered",
        "order_purchase_timestamp": "2018-01-01 12:00:00",
    },
    "order_items": {
        "order_id": "o1", "order_item_id": "1", "product_id": "p1",
        "seller_id": "s1", "price": "10.50", "freight_value": "2.25",
    },
    "order_payments": {
        "order_id": "o1", "payment_sequential": "1",
        "payment_type": "credit_card", "payment_value": "12.75",
    },
    "order_reviews": {"review_id": "r1", "order_id": "o1", "review_score": "5"},
}


def write_fixture(directory: Path, table: str, rows: list[dict]) -> None:
    spec = SPECS[table]
    with (directory / spec["file"]).open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(spec["columns"]))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in spec["columns"]})


def write_all(directory: Path) -> None:
    for table, row in BASE_ROWS.items():
        write_fixture(directory, table, [row])


class ProfileOlistTests(unittest.TestCase):
    def test_complete_sources_and_relationships(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            write_all(directory)
            report = profile_directory(directory)
            self.assertTrue(report["complete"], report["errors"])
            self.assertEqual(len(report["tables"]), 8)
            for result in report["tables"].values():
                self.assertEqual(result["rows"], 1)
                self.assertEqual(result["candidate_keys"][0]["duplicate_rows"], 0)
                self.assertTrue(result["sha256"])
                for relation in result["foreign_keys"]:
                    self.assertEqual(relation["orphan_rows"], 0)
            self.assertEqual(
                report["tables"]["order_items"]["columns"]["price"]["min"], "10.50"
            )
            self.assertEqual(
                report["tables"]["order_items"]["columns"]["price"]["max_scale"], 2
            )
            self.assertEqual(
                report["tables"]["customers"]["columns"]["customer_id"]["max_length"], 2
            )

    def test_duplicate_and_orphan_are_reported_without_dropping_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            write_all(directory)
            write_fixture(directory, "order_reviews", [
                BASE_ROWS["order_reviews"],
                BASE_ROWS["order_reviews"],
            ])
            write_fixture(directory, "order_items", [{
                **BASE_ROWS["order_items"], "product_id": "missing-product"
            }])
            report = profile_directory(directory)
            self.assertTrue(report["complete"], report["errors"])
            reviews = report["tables"]["order_reviews"]
            self.assertEqual(reviews["rows"], 2)
            self.assertEqual(reviews["candidate_keys"][0]["duplicate_rows"], 1)
            items = report["tables"]["order_items"]
            product_fk = next(item for item in items["foreign_keys"]
                              if item["column"] == "product_id")
            self.assertEqual(product_fk["orphan_rows"], 1)

    def test_missing_file_prevents_complete_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            write_all(directory)
            (directory / SPECS["orders"]["file"]).unlink()
            report = profile_directory(directory)
            self.assertFalse(report["complete"])
            self.assertTrue(any("olist_orders_dataset.csv" in message
                                for message in report["errors"]))


if __name__ == "__main__":
    unittest.main()