#!/usr/bin/env python3
"""Export the `sales_db.Orders` table to CSV for Excel/Google Sheets.

Usage:
  python scripts/export_orders.py --limit 10000 --out data/orders.csv

Defaults to writing `data/orders.csv` with a 10k row limit to avoid OOM when converting to pandas.
"""
import os
import sys
import argparse

from pyspark.sql import SparkSession


def main(argv=None):
    parser = argparse.ArgumentParser(description="Export sales_db.Orders to CSV")
    parser.add_argument("--limit", type=int, default=10000, help="Max rows to export (use small values for large tables)")
    parser.add_argument("--out", default="data/orders.csv", help="Output CSV path")
    args = parser.parse_args(argv)

    out_dir = os.path.dirname(args.out) or "."
    os.makedirs(out_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("export_orders") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Connected to Spark, exporting table sales_db.Orders")
    try:
        df = spark.table("sales_db.Orders")
    except Exception as e:
        print("Failed to read table sales_db.Orders:", e)
        spark.stop()
        sys.exit(2)

    limited = df.limit(args.limit)

    # convert safely to pandas and write csv
    try:
        pdf = limited.toPandas()
        pdf.to_csv(args.out, index=False)
        print(f"Wrote {len(pdf)} rows to {args.out}")
    except Exception as e:
        print("Failed to convert/write CSV:", e)
        spark.stop()
        sys.exit(3)

    spark.stop()


if __name__ == "__main__":
    main()
