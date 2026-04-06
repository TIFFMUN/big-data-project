"""
Build the Q2 feature dataset used to answer:

"Given delays of flights arriving at the destination airport and the airport's
current congestion, what is the expected in-air delay for a flight before it lands?"
"""

import argparse
import sys
from typing import Sequence

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Q2 feature dataset.")
    parser.add_argument("input_path", help="Curated input parquet path")
    parser.add_argument("output_path", help="Output parquet path for Q2 features")
    parser.add_argument("--model-version", default="manual")
    parser.add_argument(
        "--latest-month-only",
        action="store_true",
        help="Limit the feature build to the latest available year-month in the curated dataset.",
    )
    parser.add_argument("--app-name", default="BigDataProject-Q2BuildFeatures")
    return parser.parse_args(argv)


def add_timestamp_columns(df):
    arr_day_offset = F.when(
        F.col("crs_arr_time_minutes").isNull() | F.col("crs_dep_time_minutes").isNull(),
        F.lit(0),
    ).when(
        F.col("crs_arr_time_minutes") < F.col("crs_dep_time_minutes"),
        F.lit(1),
    ).otherwise(F.lit(0))

    df = df.withColumn(
        "scheduled_dep_ts",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.date_format(F.col("flight_date"), "yyyy-MM-dd"),
                F.concat_ws(
                    ":",
                    F.substring(F.col("crs_dep_time_hhmm"), 1, 2),
                    F.substring(F.col("crs_dep_time_hhmm"), 3, 2),
                    F.lit("00"),
                ),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    df = df.withColumn("scheduled_arr_local_date", F.date_add(F.col("flight_date"), arr_day_offset))

    df = df.withColumn(
        "scheduled_arr_ts",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.date_format(F.col("scheduled_arr_local_date"), "yyyy-MM-dd"),
                F.concat_ws(
                    ":",
                    F.substring(F.col("crs_arr_time_hhmm"), 1, 2),
                    F.substring(F.col("crs_arr_time_hhmm"), 3, 2),
                    F.lit("00"),
                ),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    return (
        df.withColumn("scheduled_arr_day_offset", arr_day_offset.cast("int"))
        .withColumn("scheduled_dep_day_offset", F.lit(0).cast("int"))
        .withColumn("scheduled_dep_ts_unix", F.unix_timestamp("scheduled_dep_ts"))
        .withColumn("scheduled_arr_ts_unix", F.unix_timestamp("scheduled_arr_ts"))
    )


def build_features(df, model_version: str):
    filtered = (
        df.filter(~F.col("is_cancelled"))
        .filter(~F.col("is_diverted"))
        .filter(F.col("flight_date").isNotNull())
        .filter(F.col("unique_carrier").isNotNull())
        .filter(F.col("origin").isNotNull())
        .filter(F.col("dest").isNotNull())
        .filter(F.col("distance").isNotNull())
        .filter(F.col("crs_elapsed_time").isNotNull())
        .filter(F.col("taxi_in").isNotNull())
        .filter(F.col("taxi_out").isNotNull())
        .filter(F.col("arr_delay").isNotNull())
        .filter(F.col("dep_delay").isNotNull())
        .filter(F.col("actual_elapsed_time").isNotNull())
        .filter(F.col("crs_dep_time_hhmm").isNotNull())
        .filter(F.col("crs_arr_time_hhmm").isNotNull())
    )

    filtered = add_timestamp_columns(filtered)
    filtered = filtered.filter(F.col("scheduled_arr_ts").isNotNull()).filter(F.col("scheduled_dep_ts").isNotNull())

    filtered = filtered.withColumn(
        "flight_air_time_delay",
        (
            F.col("taxi_in")
            + F.col("actual_elapsed_time")
            - F.col("crs_elapsed_time")
            - F.col("taxi_out")
        ).cast("double"),
    )

    filtered = filtered.withColumn(
        "arr_delay_over_15_flag",
        F.when(F.col("arr_delay") > 15, F.lit(1.0)).otherwise(F.lit(0.0)),
    ).withColumn(
        "arr_delay_positive_flag",
        F.when(F.col("arr_delay") > 0, F.lit(1.0)).otherwise(F.lit(0.0)),
    )

    dest_order_window = (
        Window.partitionBy("dest")
        .orderBy(F.col("scheduled_arr_ts_unix"))
        .rangeBetween(-3600, -1)
    )
    dest_order_window_3h = (
        Window.partitionBy("dest")
        .orderBy(F.col("scheduled_arr_ts_unix"))
        .rangeBetween(-10800, -1)
    )
    dest_order_window_6h = (
        Window.partitionBy("dest")
        .orderBy(F.col("scheduled_arr_ts_unix"))
        .rangeBetween(-21600, -1)
    )
    daily_queue_window = (
        Window.partitionBy("dest", "scheduled_arr_local_date")
        .orderBy(F.col("scheduled_arr_ts_unix"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    enriched = (
        filtered.withColumn("dest_prev_1h_flight_count", F.count("*").over(dest_order_window))
        .withColumn("dest_prev_1h_avg_arr_delay", F.avg("arr_delay").over(dest_order_window))
        .withColumn("dest_prev_1h_delay_rate", F.avg("arr_delay_over_15_flag").over(dest_order_window))
        .withColumn("dest_prev_1h_positive_delay_rate", F.avg("arr_delay_positive_flag").over(dest_order_window))
        .withColumn("dest_prev_1h_avg_distance", F.avg("distance").over(dest_order_window))
        .withColumn("dest_prev_1h_avg_taxi_in", F.avg("taxi_in").over(dest_order_window))
        .withColumn("dest_prev_1h_avg_dep_delay", F.avg("dep_delay").over(dest_order_window))
        .withColumn("dest_prev_1h_avg_air_time_delay", F.avg("flight_air_time_delay").over(dest_order_window))
        .withColumn("dest_prev_3h_flight_count", F.count("*").over(dest_order_window_3h))
        .withColumn("dest_prev_3h_avg_arr_delay", F.avg("arr_delay").over(dest_order_window_3h))
        .withColumn("dest_prev_3h_delay_rate", F.avg("arr_delay_over_15_flag").over(dest_order_window_3h))
        .withColumn("dest_prev_3h_avg_taxi_in", F.avg("taxi_in").over(dest_order_window_3h))
        .withColumn("dest_prev_3h_avg_air_time_delay", F.avg("flight_air_time_delay").over(dest_order_window_3h))
        .withColumn("dest_prev_6h_flight_count", F.count("*").over(dest_order_window_6h))
        .withColumn("dest_prev_6h_avg_arr_delay", F.avg("arr_delay").over(dest_order_window_6h))
        .withColumn("dest_prev_6h_delay_rate", F.avg("arr_delay_over_15_flag").over(dest_order_window_6h))
        .withColumn("dest_prev_6h_avg_taxi_in", F.avg("taxi_in").over(dest_order_window_6h))
        .withColumn("dest_same_day_prior_arrivals", F.count("*").over(daily_queue_window))
    )

    enriched = enriched.fillna(
        {
            "dest_prev_1h_flight_count": 0.0,
            "dest_prev_1h_avg_arr_delay": 0.0,
            "dest_prev_1h_delay_rate": 0.0,
            "dest_prev_1h_positive_delay_rate": 0.0,
            "dest_prev_1h_avg_distance": 0.0,
            "dest_prev_1h_avg_taxi_in": 0.0,
            "dest_prev_1h_avg_dep_delay": 0.0,
            "dest_prev_1h_avg_air_time_delay": 0.0,
            "dest_prev_3h_flight_count": 0.0,
            "dest_prev_3h_avg_arr_delay": 0.0,
            "dest_prev_3h_delay_rate": 0.0,
            "dest_prev_3h_avg_taxi_in": 0.0,
            "dest_prev_3h_avg_air_time_delay": 0.0,
            "dest_prev_6h_flight_count": 0.0,
            "dest_prev_6h_avg_arr_delay": 0.0,
            "dest_prev_6h_delay_rate": 0.0,
            "dest_prev_6h_avg_taxi_in": 0.0,
            "dest_same_day_prior_arrivals": 0.0,
        }
    )

    return (
        enriched.withColumn("is_weekend_int", F.when(F.col("is_weekend"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("model_version", F.lit(model_version))
        .select(
            "flight_id",
            "flight_date",
            "year",
            "month",
            "day_of_month",
            "day_of_week",
            "is_weekend_int",
            "unique_carrier",
            "flight_num",
            "tail_num",
            "origin",
            "dest",
            "route_key",
            "distance",
            "crs_elapsed_time",
            "air_time",
            "dep_delay",
            "arr_delay",
            "taxi_in",
            "taxi_out",
            "scheduled_dep_hour",
            "scheduled_arr_hour",
            "scheduled_dep_day_offset",
            "scheduled_arr_day_offset",
            "scheduled_dep_ts",
            "scheduled_arr_ts",
            "scheduled_dep_ts_unix",
            "scheduled_arr_ts_unix",
            "dest_prev_1h_flight_count",
            "dest_prev_1h_avg_arr_delay",
            "dest_prev_1h_delay_rate",
            "dest_prev_1h_positive_delay_rate",
            "dest_prev_1h_avg_distance",
            "dest_prev_1h_avg_taxi_in",
            "dest_prev_1h_avg_dep_delay",
            "dest_prev_1h_avg_air_time_delay",
            "dest_prev_3h_flight_count",
            "dest_prev_3h_avg_arr_delay",
            "dest_prev_3h_delay_rate",
            "dest_prev_3h_avg_taxi_in",
            "dest_prev_3h_avg_air_time_delay",
            "dest_prev_6h_flight_count",
            "dest_prev_6h_avg_arr_delay",
            "dest_prev_6h_delay_rate",
            "dest_prev_6h_avg_taxi_in",
            "dest_same_day_prior_arrivals",
            "flight_air_time_delay",
            "model_version",
        )
    )


def filter_to_latest_month(df):
    latest_flight_date = df.select(F.max("flight_date").alias("latest_flight_date")).collect()[0][
        "latest_flight_date"
    ]
    if latest_flight_date is None:
        raise ValueError("Curated dataset is empty; unable to determine the latest month.")

    latest_year = int(latest_flight_date.year)
    latest_month = int(latest_flight_date.month)
    filtered_df = df.filter(
        (F.col("year") == latest_year)
        & (F.col("month") == latest_month)
    )
    return filtered_df, latest_year, latest_month


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        curated_df = spark.read.parquet(args.input_path)
        if args.latest_month_only:
            curated_df, latest_year, latest_month = filter_to_latest_month(curated_df)
            print(
                "Limiting Q2 feature build to the latest available month: "
                f"{latest_year}-{latest_month:02d}"
            )
        feature_df = build_features(curated_df, args.model_version)

        (
            feature_df.write.mode("overwrite")
            .partitionBy("model_version", "year", "month")
            .parquet(args.output_path)
        )

        print(f"Wrote Q2 feature dataset to {args.output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
