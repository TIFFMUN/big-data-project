"""
Run batch inference for Q2 using the saved Spark ML model.
"""

import argparse
import sys
from typing import Sequence

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Q2 batch inference.")
    parser.add_argument("input_path")
    parser.add_argument("model_path")
    parser.add_argument("output_path")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--inference-batch-id", required=True)
    parser.add_argument("--app-name", default="BigDataProject-Q2BatchInference")
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
            .withColumn("absolute_error", F.abs(F.col("flight_air_time_delay") - F.col("predicted_in_air_delay")))
        )

        (
            prediction_output.write.mode("overwrite")
            .partitionBy("inference_batch_id", "model_version", "year", "month")
            .parquet(args.output_path)
        )

        print(f"Wrote batch predictions to {args.output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
