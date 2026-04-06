"""
Run batch inference for Q2 using the provided XGBoost model bundle.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tarfile
import tempfile
from typing import Sequence

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    import numpy as np
except ImportError as exc:
    np = None
    NUMPY_IMPORT_ERROR = exc
else:
    NUMPY_IMPORT_ERROR = None

try:
    import xgboost as xgb
except ImportError as exc:
    xgb = None
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None

MODEL_ARCHIVE_MEMBER_NAME = "xgboost-model"
DEST_ENCODER_MEMBER_NAME = "dest_encoder_classes.json"
UNKNOWN_DEST_INDEX = -1.0
MODEL_INPUT_COLUMNS = [
    "dest",
    "scheduled_arr_hour",
    "dest_prev_1h_flight_count",
    "dest_prev_1h_avg_air_time_delay",
    "dest_prev_1h_avg_arr_delay",
    "dest_prev_1h_avg_dep_delay",
    "dest_prev_1h_avg_taxi_in",
    "dest_prev_1h_avg_distance",
]
SOURCE_COLUMNS = [
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
    "scheduled_arr_hour",
    "dest_prev_1h_avg_dep_delay",
    "dest_prev_1h_avg_taxi_in",
    "dest_prev_1h_avg_air_time_delay",
    "dest_prev_1h_avg_distance",
]
_PREDICTOR_CACHE = {}


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Q2 batch inference with XGBoost.")
    parser.add_argument("input_path")
    parser.add_argument("model_path")
    parser.add_argument("output_path")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--inference-batch-id", required=True)
    parser.add_argument("--app-name", default="BigDataProject-Q2BatchInferenceXGBoost")
    return parser.parse_args(argv)


def ensure_runtime_dependencies() -> None:
    if NUMPY_IMPORT_ERROR is not None:
        raise RuntimeError(
            "numpy is required for XGBoost batch inference but is not available."
        ) from NUMPY_IMPORT_ERROR
    if XGBOOST_IMPORT_ERROR is not None:
        raise RuntimeError(
            "xgboost is required for XGBoost batch inference but is not available. "
            "On EMR, install xgboost==1.6.2 before running this step."
        ) from XGBOOST_IMPORT_ERROR


def _safe_float(value) -> float:
    if value is None:
        return 0.0
    return float(value)


def load_predictor_bundle(distributed_archive_name: str) -> dict:
    cached = _PREDICTOR_CACHE.get(distributed_archive_name)
    if cached is not None:
        return cached

    ensure_runtime_dependencies()
    local_archive_path = SparkFiles.get(distributed_archive_name)
    extract_dir = tempfile.mkdtemp(prefix="q2_xgb_model_")
    model_output_path = os.path.join(extract_dir, MODEL_ARCHIVE_MEMBER_NAME)
    encoder_output_path = os.path.join(extract_dir, DEST_ENCODER_MEMBER_NAME)

    with tarfile.open(local_archive_path, "r:gz") as archive:
        model_member = archive.extractfile(MODEL_ARCHIVE_MEMBER_NAME)
        if model_member is None:
            raise ValueError(
                f"{MODEL_ARCHIVE_MEMBER_NAME} was not found in {distributed_archive_name}."
            )
        encoder_member = archive.extractfile(DEST_ENCODER_MEMBER_NAME)
        if encoder_member is None:
            raise ValueError(
                f"{DEST_ENCODER_MEMBER_NAME} was not found in {distributed_archive_name}."
            )

        with open(model_output_path, "wb") as output_file:
            output_file.write(model_member.read())
        with open(encoder_output_path, "wb") as output_file:
            output_file.write(encoder_member.read())

    with open(encoder_output_path, "r", encoding="utf-8") as encoder_file:
        dest_encoder_classes = json.load(encoder_file)
    if not isinstance(dest_encoder_classes, list):
        raise ValueError(
            f"Expected {DEST_ENCODER_MEMBER_NAME} to contain a JSON array of destination classes."
        )

    booster = xgb.Booster()
    booster.load_model(model_output_path)
    predictor = {
        "booster": booster,
        "dest_encoder": {
            str(destination): float(index)
            for index, destination in enumerate(dest_encoder_classes)
        },
    }
    _PREDICTOR_CACHE[distributed_archive_name] = predictor
    return predictor


def build_model_row(row, dest_encoder: dict) -> list[float]:
    dest_value = row["dest"]
    return [
        dest_encoder.get(str(dest_value), UNKNOWN_DEST_INDEX),
        _safe_float(row["scheduled_arr_hour"]),
        _safe_float(row["dest_prev_1h_flight_count"]),
        _safe_float(row["dest_prev_1h_avg_air_time_delay"]),
        _safe_float(row["dest_prev_1h_avg_arr_delay"]),
        _safe_float(row["dest_prev_1h_avg_dep_delay"]),
        _safe_float(row["dest_prev_1h_avg_taxi_in"]),
        _safe_float(row["dest_prev_1h_avg_distance"]),
    ]


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    ensure_runtime_dependencies()

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        features_df = spark.read.parquet(args.input_path)
        required_columns = SOURCE_COLUMNS + MODEL_INPUT_COLUMNS
        missing_columns = [column_name for column_name in required_columns if column_name not in features_df.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns for XGBoost inference: {sorted(set(missing_columns))}"
            )

        distributed_archive_name = os.path.basename(args.model_path)
        source_df = features_df.select(*SOURCE_COLUMNS)
        source_column_names = source_df.columns
        output_schema = source_df.schema.add(
            T.StructField("predicted_in_air_delay", T.DoubleType(), nullable=False)
        )

        def predict_partition(rows):
            predictor = load_predictor_bundle(distributed_archive_name)
            buffered_rows = []
            model_rows = []
            for row in rows:
                buffered_rows.append(tuple(row[column_name] for column_name in source_column_names))
                model_rows.append(build_model_row(row, predictor["dest_encoder"]))

            if not buffered_rows:
                return iter(())

            dmatrix = xgb.DMatrix(np.asarray(model_rows, dtype="float32"))
            predictions = predictor["booster"].predict(dmatrix)
            return (
                base_row + (float(prediction),)
                for base_row, prediction in zip(buffered_rows, predictions)
            )

        predictions_df = spark.createDataFrame(
            source_df.rdd.mapPartitions(predict_partition),
            schema=output_schema,
        )

        prediction_output = (
            predictions_df.select(
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
                F.col("predicted_in_air_delay"),
            )
            .withColumn("prediction_generated_at", F.current_timestamp())
            .withColumn("absolute_error", F.abs(F.col("flight_air_time_delay") - F.col("predicted_in_air_delay")))
        )

        (
            prediction_output.write.mode("overwrite")
            .partitionBy("inference_batch_id", "model_version", "year", "month")
            .parquet(args.output_path)
        )

        print(f"Wrote XGBoost batch predictions to {args.output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
