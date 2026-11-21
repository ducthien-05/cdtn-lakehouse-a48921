#!/usr/bin/env python3
"""Load the static Olist category translation into an Iceberg reference table.

The validate action is offline. The run action reads PostgreSQL over JDBC and
replaces only lakehouse.reference.product_category_name_translation.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

CATALOG = "lakehouse"
NAMESPACE = "reference"
TABLE = "product_category_name_translation"
DEFAULTS = {
    "POSTGRES_SOURCE_HOST": "postgres-source",
    "POSTGRES_SOURCE_PORT": "5432",
    "POSTGRES_SOURCE_DB": "olist",
    "POSTGRES_SOURCE_USER": "postgres",
    "POSTGRES_SOURCE_PASSWORD": "",
}


class ReferenceError(Exception):
    pass


ROOT = Path(__file__).resolve().parents[1]


def settings(env_file: Path | None = None) -> dict[str, str]:
    values = dict(DEFAULTS)
    candidate = env_file or (ROOT / ".env")
    if candidate.is_file():
        try:
            from dotenv import dotenv_values
            loaded = dotenv_values(candidate)
            mapping = {
                "POSTGRES_DB": "POSTGRES_SOURCE_DB",
                "POSTGRES_USER": "POSTGRES_SOURCE_USER",
                "POSTGRES_PASSWORD": "POSTGRES_SOURCE_PASSWORD",
            }
            for source, target in mapping.items():
                if loaded.get(source) is not None:
                    values[target] = str(loaded[source])
        except ImportError:
            pass
    values.update({key: os.environ[key] for key in DEFAULTS if key in os.environ})
    missing = [key for key in ("POSTGRES_SOURCE_DB", "POSTGRES_SOURCE_USER", "POSTGRES_SOURCE_PASSWORD")
               if not values[key]]
    if missing:
        raise ReferenceError("Missing reference JDBC settings: " + ", ".join(missing))
    if not values["POSTGRES_SOURCE_HOST"] or not values["POSTGRES_SOURCE_PORT"].isdigit():
        raise ReferenceError("POSTGRES_SOURCE_HOST/POSTGRES_SOURCE_PORT are invalid")
    return values


def run(env_file: Path | None = None) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import SparkSession

    cfg = settings(env_file)
    spark = (SparkSession.builder.appName("olist-reference-jdbc")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        url = (f"jdbc:postgresql://{cfg['POSTGRES_SOURCE_HOST']}:{cfg['POSTGRES_SOURCE_PORT']}"
               f"/{cfg['POSTGRES_SOURCE_DB']}")
        source = (spark.read.format("jdbc").option("url", url)
                  .option("dbtable", "olist.product_category_name_translation")
                  .option("user", cfg["POSTGRES_SOURCE_USER"])
                  .option("password", cfg["POSTGRES_SOURCE_PASSWORD"])
                  .option("driver", "org.postgresql.Driver").load())
        expected = {"product_category_name", "product_category_name_english"}
        if set(source.columns) != expected:
            raise ReferenceError(f"Unexpected reference columns: {source.columns}")
        if source.filter(F.col("product_category_name").isNull()).limit(1).count():
            raise ReferenceError("Reference key product_category_name contains NULL")
        if source.groupBy("product_category_name").count().filter(F.col("count") > 1).limit(1).count():
            raise ReferenceError("Reference key product_category_name is duplicated")
        target = f"{CATALOG}.{NAMESPACE}.{TABLE}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        output = source.select(
            F.col("product_category_name").cast("string"),
            F.col("product_category_name_english").cast("string"),
            F.current_timestamp().alias("reference_loaded_at"),
        )
        (output.writeTo(target).using("iceberg")
         .tableProperty("format-version", "2")
         .tableProperty("write.format.default", "parquet")
         .tableProperty("write.parquet.compression-codec", "zstd")
         .tableProperty("write.target-file-size-bytes", "134217728")
         .tableProperty("reference.contract.version", "1")
         .createOrReplace())
        print(f"Loaded {output.count()} reference rows into {target}", flush=True)
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "run"))
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    args = parser.parse_args()
    try:
        cfg = settings(args.env_file)
        if args.action == "validate":
            print({key: ("***" if "PASSWORD" in key else value) for key, value in cfg.items()})
            print("Reference configuration valid; no Spark or database connection opened.")
        else:
            run(args.env_file)
    except (ReferenceError, OSError, ValueError, KeyError) as exc:
        print(f"Reference {args.action} failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
