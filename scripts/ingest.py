"""
PySpark ingest job for the Kaggle airline dataset.

Flow:
1. Read raw airline files from an S3 landing prefix.
2. Standardize the source schema into a curated flight-level dataset.
3. Add date, time, route, and data-quality helper fields.
4. Write partitioned Parquet output back to S3.

Usage:
    spark-submit ingest.py s3://bucket/raw/airline_data/ \
        s3://bucket/processed/airline_data/
"""

import argparse
import re
import sys
from typing import List, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


RAW_COLUMNS = [
    "Year",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "DepTime",
    "CRSDepTime",
    "ArrTime",
    "CRSArrTime",
    "UniqueCarrier",
    "FlightNum",
    "TailNum",
    "ActualElapsedTime",
    "CRSElapsedTime",
    "AirTime",
    "ArrDelay",
    "DepDelay",
    "Origin",
    "Dest",
    "Distance",
    "TaxiIn",
    "TaxiOut",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]
KNOWN_COLUMN_RENAMES = {
    "Year": "year",
    "Month": "month",
    "DayofMonth": "day_of_month",
    "DayOfWeek": "day_of_week",
    "DepTime": "dep_time",
    "CRSDepTime": "crs_dep_time",
    "ArrTime": "arr_time",
    "CRSArrTime": "crs_arr_time",
    "UniqueCarrier": "unique_carrier",
    "FlightNum": "flight_num",
    "TailNum": "tail_num",
    "ActualElapsedTime": "actual_elapsed_time",
    "CRSElapsedTime": "crs_elapsed_time",
    "AirTime": "air_time",
    "ArrDelay": "arr_delay",
    "DepDelay": "dep_delay",
    "Origin": "origin",
    "Dest": "dest",
    "Distance": "distance",
    "TaxiIn": "taxi_in",
    "TaxiOut": "taxi_out",
    "Cancelled": "cancelled",
    "CancellationCode": "cancellation_code",
    "Diverted": "diverted",
    "CarrierDelay": "carrier_delay",
    "WeatherDelay": "weather_delay",
    "NASDelay": "nas_delay",
    "SecurityDelay": "security_delay",
    "LateAircraftDelay": "late_aircraft_delay",
}
INTEGER_COLUMNS = {
    "year",
    "month",
    "day_of_month",
    "day_of_week",
    "dep_time",
    "crs_dep_time",
    "arr_time",
    "crs_arr_time",
    "flight_num",
    "actual_elapsed_time",
    "crs_elapsed_time",
    "air_time",
    "arr_delay",
    "dep_delay",
    "distance",
    "taxi_in",
    "taxi_out",
    "cancelled",
    "diverted",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
}
UPPERCASE_COLUMNS = {
    "unique_carrier",
    "tail_num",
    "origin",
    "dest",
    "cancellation_code",
}
TIME_COLUMNS = (
    "crs_dep_time",
    "dep_time",
    "crs_arr_time",
    "arr_time",
)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read raw airline files from S3 and write curated Parquet."
    )
    parser.add_argument(
        "input_path",
        help="Raw S3 prefix, for example s3://bucket/raw/airline_data/",
    )
    parser.add_argument(
        "output_path",
        help="Processed S3 prefix, for example s3://bucket/processed/airline_data/",
    )
    parser.add_argument(
        "--app-name",
        default="BigDataProject-AirlineIngest",
        help="Spark application name.",
    )
    return parser.parse_args(argv)


def raw_schema() -> T.StructType:
    return T.StructType(
        [T.StructField(column_name, T.StringType(), True) for column_name in RAW_COLUMNS]
    )


def to_snake_case(name: str) -> str:
    first_pass = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    second_pass = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first_pass)
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", second_pass)
    return normalized.strip("_").lower()


def normalize_columns(df: DataFrame) -> DataFrame:
    normalized_names = [
        KNOWN_COLUMN_RENAMES.get(column_name, to_snake_case(column_name))
        for column_name in df.columns
    ]
    return df.toDF(*normalized_names)


def trim_string_columns(df: DataFrame) -> DataFrame:
    for column_name in df.columns:
        df = df.withColumn(
            column_name,
            F.when(F.trim(F.col(column_name)) == "", None).otherwise(
                F.trim(F.col(column_name))
            ),
        )
    return df


def drop_embedded_header_rows(df: DataFrame) -> DataFrame:
    header_match = (
        (F.lower(F.coalesce(F.col("Year"), F.lit(""))) == F.lit("year"))
        & (F.lower(F.coalesce(F.col("Month"), F.lit(""))) == F.lit("month"))
        & (F.lower(F.coalesce(F.col("DayofMonth"), F.lit(""))) == F.lit("dayofmonth"))
        & (F.lower(F.coalesce(F.col("DayOfWeek"), F.lit(""))) == F.lit("dayofweek"))
    )
    return df.filter(~header_match)


def cast_integer_columns(df: DataFrame) -> DataFrame:
    for column_name in sorted(INTEGER_COLUMNS.intersection(df.columns)):
        df = df.withColumn(column_name, F.col(column_name).cast("int"))
    return df


def uppercase_string_columns(df: DataFrame) -> DataFrame:
    for column_name in sorted(UPPERCASE_COLUMNS.intersection(df.columns)):
        df = df.withColumn(column_name, F.upper(F.col(column_name)))
    return df


def normalize_hhmm_expr(column_name: str):
    padded = F.when(
        F.col(column_name).isNull(),
        None,
    ).otherwise(F.lpad(F.col(column_name).cast("string"), 4, "0"))
    hours = F.substring(padded, 1, 2).cast("int")
    minutes = F.substring(padded, 3, 2).cast("int")
    is_valid = (
        padded.rlike(r"^\d{4}$")
        & (
            ((hours >= 0) & (hours <= 23) & (minutes >= 0) & (minutes <= 59))
            | ((hours == 24) & (minutes == 0))
        )
    )
    return F.when(is_valid, padded)


def hhmm_to_minutes_expr(hhmm_column_name: str):
    hours = F.substring(F.col(hhmm_column_name), 1, 2).cast("int")
    minutes = F.substring(F.col(hhmm_column_name), 3, 2).cast("int")
    return F.when(
        F.col(hhmm_column_name).isNull(),
        None,
    ).when(
        (hours == 24) & (minutes == 0),
        F.lit(24 * 60),
    ).otherwise(hours * 60 + minutes)


def add_time_features(df: DataFrame) -> DataFrame:
    for column_name in TIME_COLUMNS:
        hhmm_column = f"{column_name}_hhmm"
        minutes_column = f"{column_name}_minutes"
        hour_column = f"{column_name}_hour"
        df = df.withColumn(hhmm_column, normalize_hhmm_expr(column_name))
        df = df.withColumn(minutes_column, hhmm_to_minutes_expr(hhmm_column))
        df = df.withColumn(
            hour_column,
            F.when(
                F.col(minutes_column).isNull(),
                None,
            ).otherwise(F.pmod(F.floor(F.col(minutes_column) / 60), F.lit(24))),
        )
    return df


def add_flight_level_features(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "flight_date",
        F.to_date(
            F.format_string(
                "%04d-%02d-%02d",
                F.col("year"),
                F.col("month"),
                F.col("day_of_month"),
            )
        ),
    )
    df = df.withColumn("has_valid_flight_date", F.col("flight_date").isNotNull())
    df = df.withColumn(
        "route_key",
        F.concat_ws("-", F.col("origin"), F.col("dest")),
    )
    df = df.withColumn("is_cancelled", F.coalesce(F.col("cancelled") == 1, F.lit(False)))
    df = df.withColumn("is_diverted", F.coalesce(F.col("diverted") == 1, F.lit(False)))
    df = df.withColumn(
        "has_valid_scheduled_times",
        F.col("crs_dep_time_minutes").isNotNull() & F.col("crs_arr_time_minutes").isNotNull(),
    )
    df = df.withColumn(
        "has_valid_actual_times",
        F.col("dep_time_minutes").isNotNull() & F.col("arr_time_minutes").isNotNull(),
    )
    df = df.withColumn("scheduled_dep_hour", F.col("crs_dep_time_hour"))
    df = df.withColumn("scheduled_arr_hour", F.col("crs_arr_time_hour"))
    df = df.withColumn("is_weekend", F.col("day_of_week").isin(6, 7))
    df = df.withColumn(
        "usable_for_dep_delay_metrics",
        (~F.col("is_cancelled"))
        & (~F.col("is_diverted"))
        & F.col("has_valid_flight_date")
        & F.col("crs_dep_time_minutes").isNotNull()
        & F.col("dep_delay").isNotNull(),
    )
    df = df.withColumn(
        "usable_for_arr_delay_metrics",
        (~F.col("is_cancelled"))
        & (~F.col("is_diverted"))
        & F.col("has_valid_flight_date")
        & F.col("crs_arr_time_minutes").isNotNull()
        & F.col("arr_delay").isNotNull(),
    )
    df = df.withColumn(
        "usable_for_turnaround_analysis",
        (~F.col("is_cancelled"))
        & (~F.col("is_diverted"))
        & F.col("tail_num").isNotNull()
        & F.col("arr_time_minutes").isNotNull()
        & F.col("dep_time_minutes").isNotNull(),
    )
    df = df.withColumn(
        "flight_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("flight_date").cast("string"), F.lit("")),
                F.coalesce(F.col("unique_carrier"), F.lit("")),
                F.coalesce(F.col("flight_num").cast("string"), F.lit("")),
                F.coalesce(F.col("origin"), F.lit("")),
                F.coalesce(F.col("dest"), F.lit("")),
                F.coalesce(F.col("crs_dep_time_hhmm"), F.lit("")),
            ),
            256,
        ),
    )
    return df


def transform_airline_dataframe(df: DataFrame) -> DataFrame:
    transformed = trim_string_columns(df)
    transformed = drop_embedded_header_rows(transformed)
    transformed = normalize_columns(transformed)
    transformed = transformed.dropna(how="all")
    transformed = uppercase_string_columns(transformed)
    transformed = cast_integer_columns(transformed)
    transformed = add_time_features(transformed)
    transformed = add_flight_level_features(transformed)
    transformed = transformed.withColumn("_ingested_at", F.current_timestamp())
    transformed = transformed.withColumn("_source_file", F.input_file_name())
    return transformed


def list_input_files(spark: SparkSession, input_path: str) -> List[str]:
    jvm = spark._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path_class = jvm.org.apache.hadoop.fs.Path
    file_system = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI.create(input_path),
        hadoop_conf,
    )
    base_path = path_class(input_path)

    if not file_system.exists(base_path):
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    data_files = []
    yearly_data_files = []
    iterator = file_system.listFiles(base_path, True)
    while iterator.hasNext():
        file_status = iterator.next()
        name = file_status.getPath().getName()
        path_string = file_status.getPath().toString()
        lower_name = name.lower()
        if not (lower_name.endswith(".csv") or lower_name.endswith(".csv.shuffle")):
            continue
        data_files.append(path_string)
        if re.fullmatch(r"(19|20)\d{2}\.csv(?:\.shuffle)?", lower_name):
            yearly_data_files.append(path_string)

    if not data_files:
        raise FileNotFoundError(f"No airline source files found under {input_path}")

    selected_files = sorted(yearly_data_files) if yearly_data_files else sorted(data_files)
    print(
        f"Found {len(data_files)} airline source files under {input_path}; "
        f"processing {len(selected_files)} files."
    )
    return selected_files


def write_processed_data(df: DataFrame, output_path: str) -> None:
    partition_columns = [name for name in ("year", "month") if name in df.columns]
    writer = df.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(output_path)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()

    try:
        input_files = list_input_files(spark, args.input_path)
        raw_df = (
            spark.read.schema(raw_schema())
            .option("header", "false")
            .option("mode", "PERMISSIVE")
            .csv(input_files)
        )

        processed_df = transform_airline_dataframe(raw_df)
        processed_df.printSchema()
        write_processed_data(processed_df, args.output_path)
        print(f"Wrote curated partitioned Parquet output to {args.output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
