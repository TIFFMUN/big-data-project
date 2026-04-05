"""
Run batch inference for Q2 using a previously saved Spark ML model.

Usage:
    spark-submit q2_batch_inference.py \
        s3://bucket/processed/q2_features/inference_run=.../ \
        s3://bucket/models/q2_in_air_delay/model_version=.../model \
        s3://bucket/processed/q2_predictions/inference_run=.../ \
        --model-version 20240401T010101 \
        --inference-batch-id 20240420T050501
"""

import argparse
import sys
from typing import Sequence

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Q2 batch inference.")
    parser.add_argument("input_path", help="Input feature parquet path")
    parser.add_argument("model_path", help="Saved Spark ML model path")
    parser.add_argument("output_path", help="Output parquet path for predictions")
    parser.add_argument(
        "--model-version",
        required=True,
        help="Trained model version used to produce the predictions",
    )
    parser.add_argument(
        "--inference-batch-id",
        default="manual",
        help="Unique identifier for this inference batch run",
    )
    parser.add_argument(
        "--app-name",
        default="BigDataProject-Q2BatchInference",
        help="Spark application name",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        features_df = spark.read.parquet(args.input_path)
        model = PipelineModel.load(args.model_path)

        predictions = model.transform(features_df)

        prediction_output = (
            predictions.select(
                "flight_id",
                "flight_date",
                "year",
                "month",
                "day_of_month",
                "unique_carrier",
                "origin",
                "dest",
                "route_key",
                "scheduled_dep_ts",
                "scheduled_arr_ts",
                "dep_delay",
                "arr_delay",
                "flight_air_time_delay",
                "dest_prev_1h_flight_count",
                "dest_prev_1h_avg_arr_delay",
                "dest_prev_1h_delay_rate",
                "dest_prev_3h_flight_count",
                "dest_prev_3h_avg_arr_delay",
                "dest_prev_6h_flight_count",
                "dest_prev_6h_avg_arr_delay",
                F.lit(args.model_version).alias("model_version"),
                F.lit(args.inference_batch_id).alias("inference_batch_id"),
                F.col("prediction").alias("predicted_in_air_delay"),
            )
            .withColumn("prediction_generated_at", F.current_timestamp())
            .withColumn(
                "absolute_error",
                F.abs(F.col("flight_air_time_delay") - F.col("predicted_in_air_delay")),
            )
        )

        (
            prediction_output.write.mode("overwrite")
            .partitionBy("model_version", "year", "month")
            .parquet(args.output_path)
        )

        print(f"Wrote batch predictions to {args.output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
