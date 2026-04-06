"""
Train the lite Q2 Spark ML model for expected in-air delay.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence
from urllib.parse import urlparse

import boto3
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATEGORICAL_COLUMNS = ["unique_carrier", "origin", "dest"]

NUMERIC_COLUMNS = [
    "distance",
    "crs_elapsed_time",
    "dep_delay",
    "taxi_out",
    "scheduled_dep_hour",
    "scheduled_arr_hour",
    "day_of_week",
    "month",
    "is_weekend_int",
    "dest_prev_1h_flight_count",
    "dest_prev_1h_avg_arr_delay",
    "dest_prev_1h_delay_rate",
    "dest_prev_1h_positive_delay_rate",
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
]


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the lite Q2 Spark ML model.")
    parser.add_argument("input_path")
    parser.add_argument("model_path")
    parser.add_argument("metrics_path")
    parser.add_argument("evaluation_output_path")
    parser.add_argument("--train-start-year", type=int, default=2004)
    parser.add_argument("--train-end-year", type=int, default=2006)
    parser.add_argument("--validation-year", type=int, default=2007)
    parser.add_argument("--test-year", type=int, default=2008)
    parser.add_argument("--app-name", default="BigDataProject-Q2TrainModelLite")
    return parser.parse_args(argv)


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def build_pipeline():
    indexers = [
        StringIndexer(inputCol=column_name, outputCol=f"{column_name}_idx", handleInvalid="keep")
        for column_name in CATEGORICAL_COLUMNS
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{name}_idx" for name in CATEGORICAL_COLUMNS],
        outputCols=[f"{name}_ohe" for name in CATEGORICAL_COLUMNS],
        handleInvalid="keep",
    )

    assembler = VectorAssembler(
        inputCols=NUMERIC_COLUMNS + [f"{name}_ohe" for name in CATEGORICAL_COLUMNS],
        outputCol="features",
        handleInvalid="keep",
    )

    regressor = GBTRegressor(
        labelCol="flight_air_time_delay",
        featuresCol="features",
        predictionCol="prediction",
        maxIter=60,
        maxDepth=5,
        maxBins=128,
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=42,
    )

    return Pipeline(stages=indexers + [encoder, assembler, regressor])


def compute_metrics(predictions):
    rmse = RegressionEvaluator(
        labelCol="flight_air_time_delay",
        predictionCol="prediction",
        metricName="rmse",
    ).evaluate(predictions)
    mae = RegressionEvaluator(
        labelCol="flight_air_time_delay",
        predictionCol="prediction",
        metricName="mae",
    ).evaluate(predictions)
    r2 = RegressionEvaluator(
        labelCol="flight_air_time_delay",
        predictionCol="prediction",
        metricName="r2",
    ).evaluate(predictions)

    return {"rmse": rmse, "mae": mae, "r2": r2}


def build_evaluation_output(predictions, dataset_split: str):
    return (
        predictions.select(
            "flight_id",
            "flight_date",
            "year",
            "month",
            "model_version",
            F.lit(dataset_split).alias("dataset_split"),
            F.col("flight_air_time_delay").alias("actual_in_air_delay"),
            F.col("prediction").alias("predicted_in_air_delay"),
        )
        .withColumn(
            "absolute_error",
            F.abs(F.col("actual_in_air_delay") - F.col("predicted_in_air_delay")),
        )
        .withColumn(
            "squared_error",
            F.pow(F.col("actual_in_air_delay") - F.col("predicted_in_air_delay"), 2),
        )
    )


def validate_year_split(args: argparse.Namespace) -> list[int]:
    if args.train_start_year > args.train_end_year:
        raise ValueError("--train-start-year cannot be greater than --train-end-year.")

    train_years = list(range(args.train_start_year, args.train_end_year + 1))
    held_out_years = [args.validation_year, args.test_year]

    if args.validation_year == args.test_year:
        raise ValueError("--validation-year and --test-year must be different.")
    if args.validation_year in train_years or args.test_year in train_years:
        raise ValueError("Validation/test years must not overlap with the training years.")

    return train_years + held_out_years


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    relevant_years = validate_year_split(args)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        print("Reading lite Q2 feature dataset...")
        df = spark.read.parquet(args.input_path)
        required_columns = CATEGORICAL_COLUMNS + NUMERIC_COLUMNS + [
            "flight_air_time_delay",
            "scheduled_arr_ts_unix",
            "flight_id",
            "flight_date",
            "year",
            "model_version",
        ]
        missing_columns = [name for name in required_columns if name not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns for lite training: {missing_columns}")

        model_version_rows = df.select("model_version").limit(1).collect()
        if not model_version_rows:
            raise ValueError("Lite Q2 feature dataset is empty; no model_version found.")
        model_version = model_version_rows[0]["model_version"]

        print(
            "Preparing lite training slice for years "
            f"{args.train_start_year}-{args.train_end_year}, "
            f"validation={args.validation_year}, test={args.test_year}."
        )
        modelling_df = (
            df.select(*required_columns)
            .dropna(subset=["flight_air_time_delay", "scheduled_arr_ts_unix"] + CATEGORICAL_COLUMNS)
            .fillna(0, subset=NUMERIC_COLUMNS)
            .withColumn("flight_air_time_delay", F.col("flight_air_time_delay").cast("double"))
            .withColumn("scheduled_arr_ts_unix", F.col("scheduled_arr_ts_unix").cast("double"))
            .filter(F.col("year").isin(*relevant_years))
            .cache()
        )

        total_rows = modelling_df.count()
        if total_rows == 0:
            raise ValueError("No rows available for lite Q2 model training after filtering.")
        print(f"Prepared lite modelling dataset with {total_rows} rows.")

        train_df = modelling_df.filter(
            F.col("year").between(args.train_start_year, args.train_end_year)
        ).cache()
        validation_df = modelling_df.filter(F.col("year") == args.validation_year).cache()
        test_df = modelling_df.filter(F.col("year") == args.test_year).cache()

        train_rows = train_df.count()
        validation_rows = validation_df.count()
        test_rows = test_df.count()
        print(
            json.dumps(
                {
                    "train_rows": train_rows,
                    "validation_rows": validation_rows,
                    "test_rows": test_rows,
                },
                sort_keys=True,
            )
        )

        if train_rows == 0 or validation_rows == 0 or test_rows == 0:
            raise ValueError(
                "Lite year-based split produced an empty train, validation, or test set."
            )

        pipeline = build_pipeline()
        print("Fitting lite Q2 pipeline model...")
        model = pipeline.fit(train_df)

        print("Scoring validation split...")
        validation_predictions = model.transform(validation_df)
        print("Scoring test split...")
        test_predictions = model.transform(test_df)

        validation_metrics = compute_metrics(validation_predictions)
        test_metrics = compute_metrics(test_predictions)
        metrics = {
            "model_version": model_version,
            "train_years": list(range(args.train_start_year, args.train_end_year + 1)),
            "validation_year": args.validation_year,
            "test_year": args.test_year,
            "train_rows": train_rows,
            "validation_rows": validation_rows,
            "test_rows": test_rows,
            "feature_count": len(NUMERIC_COLUMNS) + len(CATEGORICAL_COLUMNS),
            "model_type": "SparkML GBTRegressor",
            "validation_metrics": validation_metrics,
            "test_metrics": test_metrics,
        }

        print(f"Writing lite Q2 model artifact to {args.model_path}")
        model.write().overwrite().save(args.model_path)

        print(f"Writing lite evaluation dataset to {args.evaluation_output_path}")
        evaluation_output = build_evaluation_output(
            validation_predictions,
            "validation",
        ).unionByName(
            build_evaluation_output(
                test_predictions,
                "test",
            )
        )

        (
            evaluation_output.write.mode("overwrite")
            .partitionBy("model_version", "dataset_split", "year", "month")
            .parquet(args.evaluation_output_path)
        )

        s3_client = boto3.client("s3")
        metrics_bucket, metrics_key = parse_s3_uri(args.metrics_path)
        s3_client.put_object(
            Bucket=metrics_bucket,
            Key=metrics_key,
            Body=json.dumps(metrics, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        print(json.dumps(metrics, indent=2))
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception:
        import traceback

        traceback.print_exc()
        raise
