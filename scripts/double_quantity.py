#!/usr/bin/env python3
"""Multiply the Quantity column by 2 for a table and write the result to a target table.

Defaults:
  source: sales_db_staging.Orders
  target: sales_db_staging.Orders_doubled

Usage:
  python scripts/double_quantity.py --source-db sales_db_staging --source-table Orders \
      --target-db sales_db_staging --target-table Orders_doubled --overwrite
"""
import sys
import argparse
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv=None):
    parser = argparse.ArgumentParser(description="Double the Quantity column in a table")
    parser.add_argument("--source-db", default="sales_db_staging")
    parser.add_argument("--source-table", default="Orders")
    parser.add_argument("--target-db", default="sales_db_staging")
    parser.add_argument("--target-table", default="Orders_doubled")
    parser.add_argument("--overwrite", action="store_true", default=True, help="Overwrite target table if exists")
    args = parser.parse_args(argv)

    spark = SparkSession.builder \
        .appName("double_quantity") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    source = f"{args.source_db}.{args.source_table}"
    target = f"{args.target_db}.{args.target_table}"
    print(f"Reading source table: {source}")
    try:
        df = spark.table(source)
    except Exception as e:
        print(f"Failed to read source table {source}: {e}")
        spark.stop()
        sys.exit(2)

    # Ensure Quantity column exists
    if "Quantity" not in df.columns:
        print(f"Source table {source} has no 'Quantity' column. Columns: {df.columns}")
        spark.stop()
        sys.exit(3)

    # Multiply Quantity by 2, coalesce nulls to 0, keep other columns unchanged
    df2 = df.withColumn("Quantity", (F.coalesce(F.col("Quantity"), F.lit(0)) * F.lit(2)).cast("int"))

    mode = "overwrite" if args.overwrite else "errorifexists"
    print(f"Writing transformed table to {target} (mode={mode})")
    try:
        # saveAsTable will write into the Hive metastore
        df2.write.mode(mode).saveAsTable(target)
        print("Write complete")
    except Exception:
        print("Failed to write target table:")
        traceback.print_exc()
        spark.stop()
        sys.exit(4)

    # Show a sample
    try:
        print("Sample rows from target table:")
        spark.table(target).show(10, truncate=False)
    except Exception:
        print("Could not read back target table:")
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
