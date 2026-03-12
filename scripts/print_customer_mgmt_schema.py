#!/usr/bin/env python3
"""
Print the inferred schema of CustomerMgmt.xml so you can pass it explicitly next time (avoids inference time).

Usage (run where Spark + spark-xml are available, e.g. Databricks notebook or spark-submit):
  python scripts/print_customer_mgmt_schema.py --path gs://bucket/tpcdi/sf=10/Batch1/CustomerMgmt.xml

Or in a Databricks notebook:
  %run /path/to/scripts/print_customer_mgmt_schema --path gs://bucket/tpcdi/sf=10/Batch1/CustomerMgmt.xml

Or paste the logic into a notebook cell (spark and path already defined).
"""

import argparse
import sys
from pathlib import Path

# Add project root for benchmark imports if needed
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Print CustomerMgmt.xml schema (JSON + DDL)")
    parser.add_argument(
        "--path",
        "-p",
        required=True,
        help="Full path to CustomerMgmt.xml (e.g. gs://bucket/tpcdi/sf=10/Batch1/CustomerMgmt.xml)",
    )
    parser.add_argument(
        "--row-tag",
        default="TPCDI:Action",
        help="XML rowTag (default: TPCDI:Action)",
    )
    parser.add_argument(
        "--root-tag",
        default="TPCDI:Actions",
        help="XML rootTag (default: TPCDI:Actions)",
    )
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("Error: PySpark not found. Run this script with spark-submit or in a Databricks notebook.", file=sys.stderr)
        sys.exit(1)

    spark = SparkSession.builder.getOrCreate()
    path = args.path.strip()

    # Same options as benchmark/etl/bronze/customer_mgmt.py
    df = (
        spark.read.format("xml")
        .option("rowTag", args.row_tag)
        .option("rootTag", args.root_tag)
        .load(path)
    )
    # Trigger read
    df.limit(1).count()

    schema_json = df.schema.json()
    schema_ddl = df.schema.simpleString()

    print("\n" + "=" * 80)
    print("CustomerMgmt.xml inferred schema (pass this next time to skip inference)")
    print("=" * 80)
    print("\n# JSON (use with StructType.fromJson):")
    print(schema_json)
    print("\n# DDL (simpleString):")
    print(schema_ddl)
    print("\n# Usage: schema = StructType.fromJson(<json string above>)")
    print("#        then pass schema=schema to read_raw_file(..., format='xml', ...)")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
