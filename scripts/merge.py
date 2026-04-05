"""
PySpark merge job for Question 1 holiday enrichment.

Flow:
1. Read the curated airline dataset from S3.
2. Read the holiday reference CSV from S3.
3. Assign each flight to the nearest holiday within a configured window.
4. Write the merged Parquet output back to S3.

Usage:
    spark-submit merge.py s3://bucket/processed/airline_data/ \
        s3://bucket/raw/reference/holiday_reference.csv \
        s3://bucket/processed/question_1/merged/
"""

import argparse
import sys
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge curated flights with holiday reference data."
    )
    parser.add_argument(
        "curated_input_path",
        help="Curated Parquet input path, for example s3://bucket/processed/airline_data/",
    )
    parser.add_argument(
        "holiday_reference_path",
        help="Holiday CSV input path, for example s3://bucket/raw/reference/holiday_reference.csv",
    )
    parser.add_argument(
        "output_path",
        help="Merged Parquet output path, for example s3://bucket/processed/question_1/merged/",
    )
    parser.add_argument(
        "--days-before",
        type=int,
        default=7,
        help="Number of days before a holiday to include in the holiday window.",
    )
    parser.add_argument(
        "--days-after",
        type=int,
        default=7,
        help="Number of days after a holiday to include in the holiday window.",
    )
    parser.add_argument(
        "--app-name",
        default="BigDataProject-HolidayMerge",
        help="Spark application name.",
    )
    return parser.parse_args(argv)


def holiday_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("holiday_date", T.StringType(), False),
            T.StructField("holiday_name", T.StringType(), False),
        ]
    )


def read_curated_flights(spark: SparkSession, input_path: str) -> DataFrame:
    df = spark.read.parquet(input_path)
    required_columns = {"flight_date", "year", "month"}
    missing_columns = sorted(required_columns.difference(df.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Curated input is missing required columns: {missing}")
    return df


def read_holiday_reference(spark: SparkSession, input_path: str) -> DataFrame:
    holidays = (
        spark.read.option("header", "true")
        .schema(holiday_schema())
        .csv(input_path)
        .withColumn("holiday_date", F.to_date("holiday_date"))
        .withColumn("holiday_name", F.trim(F.col("holiday_name")))
        .filter(F.col("holiday_date").isNotNull() & F.col("holiday_name").isNotNull())
        .dropDuplicates(["holiday_date", "holiday_name"])
    )
    if holidays.rdd.isEmpty():
        raise ValueError(f"No holiday reference rows were found at {input_path}")
    return holidays


def merge_with_holidays(
    flights_df: DataFrame,
    holidays_df: DataFrame,
    days_before: int,
    days_after: int,
) -> DataFrame:
    original_columns = flights_df.columns
    flights_with_row_id = flights_df.withColumn(
        "_merge_row_id",
        F.monotonically_increasing_id(),
    )

    joined = (
        flights_with_row_id.alias("f")
        .join(
            F.broadcast(holidays_df).alias("h"),
            F.col("f.flight_date").isNotNull()
            & (
                F.datediff(F.col("f.flight_date"), F.col("h.holiday_date"))
                >= F.lit(-days_before)
            )
            & (
                F.datediff(F.col("f.flight_date"), F.col("h.holiday_date"))
                <= F.lit(days_after)
            ),
            "left",
        )
        .withColumn(
            "days_from_holiday",
            F.datediff(F.col("f.flight_date"), F.col("h.holiday_date")),
        )
    )

    ranking_window = Window.partitionBy("f._merge_row_id").orderBy(
        F.when(F.col("h.holiday_date").isNull(), 1).otherwise(0).asc(),
        F.abs(F.col("days_from_holiday")).asc_nulls_last(),
        F.col("h.holiday_date").asc_nulls_last(),
    )

    ranked = joined.withColumn("_holiday_match_rank", F.row_number().over(ranking_window))
    selected = ranked.filter(F.col("_holiday_match_rank") == 1)

    holiday_period = (
        F.when(F.col("h.holiday_date").isNull(), F.lit("non_holiday"))
        .when(F.col("days_from_holiday") < 0, F.lit("before"))
        .when(F.col("days_from_holiday") == 0, F.lit("during"))
        .otherwise(F.lit("after"))
    )

    result = selected.select(
        *[F.col(f"f.{column_name}").alias(column_name) for column_name in original_columns],
        F.col("h.holiday_name").alias("holiday_name"),
        F.col("h.holiday_date").alias("holiday_date"),
        F.col("days_from_holiday"),
        holiday_period.alias("holiday_period"),
        F.coalesce(F.col("days_from_holiday") == 0, F.lit(False)).alias("is_holiday"),
        F.col("h.holiday_date").isNotNull().alias("is_holiday_window"),
    )
    return result


def write_output(df: DataFrame, output_path: str) -> None:
    partition_columns = [name for name in ("year", "month") if name in df.columns]
    writer = df.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(output_path)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()

    try:
        flights_df = read_curated_flights(spark, args.curated_input_path)
        holidays_df = read_holiday_reference(spark, args.holiday_reference_path)
        merged_df = merge_with_holidays(
            flights_df=flights_df,
            holidays_df=holidays_df,
            days_before=args.days_before,
            days_after=args.days_after,
        )
        merged_df.printSchema()
        write_output(merged_df, args.output_path)
        print(f"Wrote merged holiday Parquet output to {args.output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
