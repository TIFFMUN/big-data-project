"""
Train the Q2 Spark ML model for expected in-air delay.

Model choice:
    GBTRegressor over a tabular feature set, trained entirely on EMR Spark.

Usage:
    spark-submit q2_train_model.py \
        s3://bucket/processed/q2_features/model_version=.../ \
        s3://bucket/models/q2_in_air_delay/model_version=.../model \
        s3://bucket/models/q2_in_air_delay/model_version=.../metrics/metrics.json \
        s3://bucket/processed/q2_model_eval/model_version=.../
"""

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


CATEGORICAL_COLUMNS = [
    "unique_carrier",
    "origin",
    "dest",
]

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
    parser = argparse.ArgumentParser(description="Train the Q2 Spark ML model.")
    parser.add_argument("input_path", help="Input feature parquet path")
    parser.add_argument("model_path", help="Output model path")
    parser.add_argument("metrics_path", help="S3 JSON path for evaluation metrics")
    parser.add_argument("evaluation_output_path", help="Parquet path for holdout predictions")
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=0.80,
        help="Time-based train split ratio",
    )
    parser.add_argument(
        "--app-name",
        default="BigDataProject-Q2TrainModel",
        help="Spark application name",
    )
    return parser.parse_args(argv)


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def build_pipeline():
    indexers = [
        StringIndexer(
            inputCol=column_name,
            outputCol=f"{column_name}_idx",
            handleInvalid="keep",
        )
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

    return {
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
    }


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        df = spark.read.parquet(args.input_path)

        required_columns = CATEGORICAL_COLUMNS + NUMERIC_COLUMNS + [
            "flight_air_time_delay",
            "scheduled_arr_ts_unix",
            "flight_id",
            "flight_date",
            "year",
            "month",
            "model_version",
        ]
        missing_columns = [name for name in required_columns if name not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns for training: {missing_columns}")

        model_version = df.select("model_version").limit(1).collect()[0]["model_version"]

        modelling_df = (
            df.select(*required_columns)
            .dropna(subset=["flight_air_time_delay", "scheduled_arr_ts_unix"] + CATEGORICAL_COLUMNS)
            .fillna(0, subset=NUMERIC_COLUMNS)
            .withColumn("flight_air_time_delay", F.col("flight_air_time_delay").cast("double"))
            .withColumn("scheduled_arr_ts_unix", F.col("scheduled_arr_ts_unix").cast("double"))
        )

        if modelling_df.rdd.isEmpty():
            raise ValueError("No rows available for Q2 model training after filtering.")

        split_cutoff = modelling_df.approxQuantile(
            "scheduled_arr_ts_unix",
            [args.train_ratio],
            0.01,
        )[0]

        train_df = modelling_df.filter(F.col("scheduled_arr_ts_unix") <= F.lit(split_cutoff))
        test_df = modelling_df.filter(F.col("scheduled_arr_ts_unix") > F.lit(split_cutoff))

        if train_df.rdd.isEmpty() or test_df.rdd.isEmpty():
            raise ValueError(
                "Time-based split produced an empty train or test set. "
                "Check feature generation and train_ratio."
            )

        pipeline = build_pipeline()
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)

        metrics = compute_metrics(predictions)
        metrics.update(
            {
                "model_version": model_version,
                "train_ratio": args.train_ratio,
                "train_rows": train_df.count(),
                "test_rows": test_df.count(),
                "feature_count": len(NUMERIC_COLUMNS) + len(CATEGORICAL_COLUMNS),
                "model_type": "SparkML GBTRegressor",
            }
        )

        model.write().overwrite().save(args.model_path)

        evaluation_output = (
            predictions.select(
                "flight_id",
                "flight_date",
                "year",
                "month",
                "model_version",
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

        (
            evaluation_output.write.mode("overwrite")
            .partitionBy("model_version", "year", "month")
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

        print("Training complete.")
        print(json.dumps(metrics, indent=2))
        print(f"Saved model to {args.model_path}")
        print(f"Saved metrics to {args.metrics_path}")
        print(f"Saved holdout predictions to {args.evaluation_output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
