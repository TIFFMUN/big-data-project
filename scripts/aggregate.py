"""
PySpark aggregate job for Question 1 holiday airport analytics.

Flow:
1. Read the merged holiday-enriched flight dataset from S3.
2. Aggregate holiday-window metrics at the airport and date level.
3. Build non-holiday airport/day-of-week baselines.
4. Join baselines onto the holiday metrics and write the analytical mart.

Usage:
    spark-submit aggregate.py s3://bucket/processed/question_1/merged/ \
        s3://bucket/processed/question_1/aggregated/
"""

import argparse
import sys
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate holiday-enriched flights into an airport analytics mart."
    )
    parser.add_argument(
        "merged_input_path",
        help="Merged Parquet input path, for example s3://bucket/processed/question_1/merged/",
    )
    parser.add_argument(
        "output_path",
        help="Aggregated Parquet output path, for example s3://bucket/processed/question_1/aggregated/",
    )
    parser.add_argument(
        "--delay-threshold",
        type=int,
        default=15,
        help="Delay threshold in minutes for severe-delay rate metrics.",
    )
    parser.add_argument(
        "--app-name",
        default="BigDataProject-HolidayAggregate",
        help="Spark application name.",
    )
    return parser.parse_args(argv)


def read_merged_flights(spark: SparkSession, input_path: str) -> DataFrame:
    df = spark.read.parquet(input_path)
    required_columns = {
        "flight_date",
        "year",
        "month",
        "day_of_week",
        "origin",
        "holiday_name",
        "holiday_date",
        "days_from_holiday",
        "holiday_period",
        "is_holiday",
        "is_holiday_window",
        "is_cancelled",
        "is_diverted",
        "usable_for_dep_delay_metrics",
        "usable_for_arr_delay_metrics",
        "dep_delay",
        "arr_delay",
    }
    missing_columns = sorted(required_columns.difference(df.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Merged input is missing required columns: {missing}")
    return df


def safe_rate_expr(numerator: str, denominator: str):
    return F.when(F.col(denominator) > 0, F.col(numerator) / F.col(denominator))


def build_holiday_metrics(df: DataFrame, delay_threshold: int) -> DataFrame:
    holiday_df = df.filter(
        F.col("is_holiday_window")
        & F.col("flight_date").isNotNull()
        & F.col("origin").isNotNull()
    )

    grouped = holiday_df.groupBy(
        "flight_date",
        "year",
        "month",
        "day_of_week",
        "origin",
        "holiday_name",
        "holiday_date",
        "days_from_holiday",
        "holiday_period",
        "is_holiday",
    ).agg(
        F.count(F.lit(1)).alias("flight_count"),
        F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias("cancel_count"),
        F.sum(F.when(F.col("is_diverted"), 1).otherwise(0)).alias("diverted_count"),
        F.sum(F.when(F.col("usable_for_dep_delay_metrics"), 1).otherwise(0)).alias(
            "dep_delay_flight_count"
        ),
        F.sum(F.when(F.col("usable_for_arr_delay_metrics"), 1).otherwise(0)).alias(
            "arr_delay_flight_count"
        ),
        F.avg(F.when(F.col("usable_for_dep_delay_metrics"), F.col("dep_delay"))).alias(
            "avg_dep_delay"
        ),
        F.avg(F.when(F.col("usable_for_arr_delay_metrics"), F.col("arr_delay"))).alias(
            "avg_arr_delay"
        ),
        F.sum(
            F.when(
                F.col("usable_for_dep_delay_metrics")
                & (F.col("dep_delay") >= F.lit(delay_threshold)),
                1,
            ).otherwise(0)
        ).alias("dep_delay_threshold_count"),
        F.sum(
            F.when(
                F.col("usable_for_arr_delay_metrics")
                & (F.col("arr_delay") >= F.lit(delay_threshold)),
                1,
            ).otherwise(0)
        ).alias("arr_delay_threshold_count"),
    )

    return (
        grouped.withColumn("cancel_rate", safe_rate_expr("cancel_count", "flight_count"))
        .withColumn(
            "dep_delay_threshold_rate",
            safe_rate_expr("dep_delay_threshold_count", "dep_delay_flight_count"),
        )
        .withColumn(
            "arr_delay_threshold_rate",
            safe_rate_expr("arr_delay_threshold_count", "arr_delay_flight_count"),
        )
    )


def build_baseline_metrics(df: DataFrame, delay_threshold: int) -> DataFrame:
    baseline_df = df.filter(
        (~F.col("is_holiday_window"))
        & F.col("flight_date").isNotNull()
        & F.col("origin").isNotNull()
    )

    grouped = baseline_df.groupBy("origin", "day_of_week").agg(
        F.count(F.lit(1)).alias("baseline_flight_count"),
        F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias("baseline_cancel_count"),
        F.sum(F.when(F.col("usable_for_dep_delay_metrics"), 1).otherwise(0)).alias(
            "baseline_dep_delay_flight_count"
        ),
        F.sum(F.when(F.col("usable_for_arr_delay_metrics"), 1).otherwise(0)).alias(
            "baseline_arr_delay_flight_count"
        ),
        F.avg(F.when(F.col("usable_for_dep_delay_metrics"), F.col("dep_delay"))).alias(
            "baseline_avg_dep_delay"
        ),
        F.avg(F.when(F.col("usable_for_arr_delay_metrics"), F.col("arr_delay"))).alias(
            "baseline_avg_arr_delay"
        ),
        F.sum(
            F.when(
                F.col("usable_for_dep_delay_metrics")
                & (F.col("dep_delay") >= F.lit(delay_threshold)),
                1,
            ).otherwise(0)
        ).alias("baseline_dep_delay_threshold_count"),
        F.sum(
            F.when(
                F.col("usable_for_arr_delay_metrics")
                & (F.col("arr_delay") >= F.lit(delay_threshold)),
                1,
            ).otherwise(0)
        ).alias("baseline_arr_delay_threshold_count"),
    )

    return (
        grouped.withColumn(
            "baseline_cancel_rate",
            safe_rate_expr("baseline_cancel_count", "baseline_flight_count"),
        )
        .withColumn(
            "baseline_dep_delay_threshold_rate",
            safe_rate_expr(
                "baseline_dep_delay_threshold_count",
                "baseline_dep_delay_flight_count",
            ),
        )
        .withColumn(
            "baseline_arr_delay_threshold_rate",
            safe_rate_expr(
                "baseline_arr_delay_threshold_count",
                "baseline_arr_delay_flight_count",
            ),
        )
    )


def join_baselines(holiday_metrics_df: DataFrame, baseline_df: DataFrame) -> DataFrame:
    joined = holiday_metrics_df.join(
        baseline_df,
        on=["origin", "day_of_week"],
        how="left",
    )

    return (
        joined.withColumn(
            "dep_delay_uplift_vs_baseline",
            F.col("avg_dep_delay") - F.col("baseline_avg_dep_delay"),
        )
        .withColumn(
            "arr_delay_uplift_vs_baseline",
            F.col("avg_arr_delay") - F.col("baseline_avg_arr_delay"),
        )
        .withColumn(
            "cancel_rate_uplift_vs_baseline",
            F.col("cancel_rate") - F.col("baseline_cancel_rate"),
        )
        .withColumn(
            "dep_delay_threshold_rate_uplift_vs_baseline",
            F.col("dep_delay_threshold_rate")
            - F.col("baseline_dep_delay_threshold_rate"),
        )
        .withColumn(
            "arr_delay_threshold_rate_uplift_vs_baseline",
            F.col("arr_delay_threshold_rate")
            - F.col("baseline_arr_delay_threshold_rate"),
        )
        .withColumn("_aggregated_at", F.current_timestamp())
    )


def add_airport_holiday_rank(df: DataFrame) -> DataFrame:
    ranking_window = Window.partitionBy(
        "year",
        "holiday_name",
        "days_from_holiday",
    ).orderBy(
        F.col("dep_delay_uplift_vs_baseline").desc_nulls_last(),
        F.col("cancel_rate_uplift_vs_baseline").desc_nulls_last(),
        F.col("arr_delay_uplift_vs_baseline").desc_nulls_last(),
        F.col("origin").asc(),
    )

    return df.withColumn(
        "airport_holiday_rank",
        F.row_number().over(ranking_window),
    )


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
        merged_df = read_merged_flights(spark, args.merged_input_path)
        holiday_metrics_df = build_holiday_metrics(merged_df, args.delay_threshold)
        baseline_df = build_baseline_metrics(merged_df, args.delay_threshold)
        aggregated_df = join_baselines(holiday_metrics_df, baseline_df)
        aggregated_df = add_airport_holiday_rank(aggregated_df)
        aggregated_df.printSchema()
        write_output(aggregated_df, args.output_path)
        print(f"Wrote aggregated holiday mart output to {args.output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
