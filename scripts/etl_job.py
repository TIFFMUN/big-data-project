"""
PySpark ETL Job – reads raw data from S3, transforms it, writes to /processed/

This script is uploaded to s3://<bucket>/scripts/ and submitted to EMR
as a Spark step by the Airflow DAG.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    if len(sys.argv) < 3:
        print("Usage: etl_job.py <input_path> <output_path>")
        print("  e.g. etl_job.py s3://bucket/raw/ s3://bucket/processed/")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("BigDataProject-ETL")
        .getOrCreate()
    )

    # ── Extract ────────────────────────────────────────────────────────────
    # Adjust the read format to match your raw data (csv, json, parquet…)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print(f"Read {df.count()} rows from {input_path}")
    df.printSchema()

    # ── Transform ──────────────────────────────────────────────────────────
    # Drop rows where every column is null
    df_clean = df.dropna(how="all")

    # Add ingestion metadata
    df_transformed = (
        df_clean
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    # ── Load ───────────────────────────────────────────────────────────────
    (
        df_transformed.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Wrote {df_transformed.count()} rows to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()

