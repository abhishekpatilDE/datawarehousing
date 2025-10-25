#!/usr/bin/env python3
"""Export all tables in the `sales_db_staging` database to CSV files.

Writes one CSV per table to `data/sales_db_staging/<table>.csv` by default.
Converts a limited number of rows to pandas to produce a single CSV file per table (safer for Excel/Sheets).

Usage:
  python scripts/export_db_staging.py --limit 10000 --out-dir data/sales_db_staging
"""
import os
import sys
import argparse

from pyspark.sql import SparkSession


def export_table_to_csv(spark, db_name, table_name, out_dir, limit):
    out_path = os.path.join(out_dir, f"{table_name}.csv")
    print(f"Exporting {db_name}.{table_name} -> {out_path} (limit={limit})")
    try:
        df = spark.table(f"{db_name}.{table_name}")
    except Exception as e:
        print(f"  Failed to read table {db_name}.{table_name}: {e}")
        return False

    limited = df.limit(limit)
    try:
        pdf = limited.toPandas()
    except Exception as e:
        print(f"  Failed to convert to pandas for table {table_name}: {e}")
        return False

    try:
        os.makedirs(out_dir, exist_ok=True)
        pdf.to_csv(out_path, index=False)
        print(f"  Wrote {len(pdf)} rows to {out_path}")
        return True
    except Exception as e:
        print(f"  Failed to write CSV for {table_name}: {e}")
        return False


def main(argv=None):
    parser = argparse.ArgumentParser(description="Export sales_db_staging tables to CSV")
    parser.add_argument("--limit", type=int, default=10000, help="Max rows per table to export")
    parser.add_argument("--out-dir", default="data/sales_db_staging", help="Output directory for CSV files")
    parser.add_argument("--db", default="sales_db_staging", help="Database name to export")
    args = parser.parse_args(argv)

    spark = SparkSession.builder \
        .appName("export_db_staging") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    db = args.db
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {db}")
        tables = [r.tableName for r in tables_df.collect()]
    except Exception as e:
        print(f"Failed to list tables in {db}: {e}")
        spark.stop()
        sys.exit(2)

    if not tables:
        print(f"No tables found in database {db}")
        spark.stop()
        return

    success = True
    for t in tables:
        ok = export_table_to_csv(spark, db, t, args.out_dir, args.limit)
        success = success and ok

    spark.stop()
    if not success:
        sys.exit(3)


if __name__ == "__main__":
    main()
