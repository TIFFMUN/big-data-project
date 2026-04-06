"""
Build the lite Q2 feature dataset for a smaller training slice.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from datetime import datetime
from typing import Sequence

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

LITE_FEATURE_OUTPUT_COLUMNS = [
    "flight_id",
    "flight_date",
    "year",
    *CATEGORICAL_COLUMNS,
    *NUMERIC_COLUMNS,
    "flight_air_time_delay",
    "model_version",
]


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the lite Q2 feature dataset.")
    parser.add_argument("input_path", help="Curated input parquet path")
    parser.add_argument("output_path", help="Output parquet path for lite Q2 features")
    parser.add_argument("--model-version", default="manual")
    parser.add_argument("--min-year", type=int, default=2004)
    parser.add_argument("--max-year", type=int, default=2008)
    parser.add_argument("--progress-output-path", default=None)
    parser.add_argument("--app-name", default="BigDataProject-Q2BuildFeaturesLite")
    return parser.parse_args(argv)


def write_text_to_uri(spark: SparkSession, output_path: str, text: str) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(output_path)
    output_stream = path.getFileSystem(hadoop_conf).create(path, True)
    writer = jvm.java.io.OutputStreamWriter(output_stream, "UTF-8")
    try:
        writer.write(text)
        writer.flush()
    finally:
        writer.close()


def emit_progress(
    spark: SparkSession,
    progress_output_path: str | None,
    phase: str,
    percent_complete: float,
    message: str,
    extra: dict | None = None,
    spark_status: dict | None = None,
) -> None:
    if not progress_output_path:
        return

    payload = {
        "phase": phase,
        "percent_complete": round(max(0.0, min(percent_complete, 100.0)), 1),
        "message": message,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    if extra:
        payload.update(extra)
    if spark_status:
        payload["spark_status"] = spark_status

    write_text_to_uri(
        spark,
        progress_output_path,
        json.dumps(payload, sort_keys=True),
    )


def collect_spark_status(spark: SparkSession) -> dict:
    try:
        tracker = spark.sparkContext.statusTracker()
        stage_ids = list(tracker.getActiveStageIds())
        completed_task_count = 0
        total_task_count = 0
        active_task_count = 0
        failed_task_count = 0
        for stage_id in stage_ids:
            stage_info = tracker.getStageInfo(stage_id)
            if stage_info is None:
                continue
            completed_task_count += int(getattr(stage_info, "numCompletedTasks", 0) or 0)
            total_task_count += int(getattr(stage_info, "numTasks", 0) or 0)
            active_task_count += int(getattr(stage_info, "numActiveTasks", 0) or 0)
            failed_task_count += int(getattr(stage_info, "numFailedTasks", 0) or 0)
        return {
            "active_stage_count": len(stage_ids),
            "completed_task_count": completed_task_count,
            "total_task_count": total_task_count,
            "active_task_count": active_task_count,
            "failed_task_count": failed_task_count,
        }
    except Exception as exc:
        return {"error": str(exc)}


class SparkProgressReporter:
    def __init__(
        self,
        spark: SparkSession,
        progress_output_path: str | None,
        phase: str,
        start_percent: float,
        end_percent: float,
        message: str,
        extra: dict | None = None,
        interval_seconds: int = 15,
    ) -> None:
        self.spark = spark
        self.progress_output_path = progress_output_path
        self.phase = phase
        self.start_percent = start_percent
        self.end_percent = end_percent
        self.message = message
        self.extra = extra or {}
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_percent = start_percent

    def __enter__(self):
        if not self.progress_output_path:
            return self
        emit_progress(
            self.spark,
            self.progress_output_path,
            self.phase,
            self.start_percent,
            self.message,
            extra=self.extra,
        )
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        if not self.progress_output_path:
            return False
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        return False

    def _run(self) -> None:
        while not self._stop_event.is_set():
            spark_status = collect_spark_status(self.spark)
            total_task_count = spark_status.get("total_task_count", 0)
            completed_task_count = spark_status.get("completed_task_count", 0)
            if total_task_count:
                candidate = self.start_percent + (
                    float(completed_task_count) / float(total_task_count)
                ) * (self.end_percent - self.start_percent)
                self._last_percent = max(self._last_percent, candidate)

            emit_progress(
                self.spark,
                self.progress_output_path,
                self.phase,
                self._last_percent,
                self.message,
                extra=self.extra,
                spark_status=spark_status,
            )
            self._stop_event.wait(self.interval_seconds)


def add_timestamp_columns(df):
    arr_day_offset = F.when(
        F.col("crs_arr_time_minutes").isNull() | F.col("crs_dep_time_minutes").isNull(),
        F.lit(0),
    ).when(
        F.col("crs_arr_time_minutes") < F.col("crs_dep_time_minutes"),
        F.lit(1),
    ).otherwise(F.lit(0))

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
    filtered = filtered.filter(F.col("scheduled_arr_ts").isNotNull())

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
        .select(*LITE_FEATURE_OUTPUT_COLUMNS)
    )


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    if args.min_year > args.max_year:
        raise ValueError("--min-year cannot be greater than --max-year.")

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        progress_context = {
            "model_version": args.model_version,
            "min_year": args.min_year,
            "max_year": args.max_year,
        }
        emit_progress(
            spark,
            args.progress_output_path,
            "starting",
            1,
            "Starting lite Q2 feature build.",
            extra=progress_context,
        )
        print("Reading curated dataset for lite Q2 feature build...")
        curated_df = spark.read.parquet(args.input_path)
        emit_progress(
            spark,
            args.progress_output_path,
            "read_input",
            10,
            "Loaded curated dataset for lite Q2 feature build.",
            extra=progress_context,
        )
        print(
            f"Filtering curated dataset to years {args.min_year}-{args.max_year} "
            f"for model_version={args.model_version}."
        )
        curated_slice_df = curated_df.filter(
            F.col("year").between(args.min_year, args.max_year)
        )
        emit_progress(
            spark,
            args.progress_output_path,
            "filter_years",
            25,
            "Filtered lite Q2 feature slice to the requested year range.",
            extra=progress_context,
        )
        feature_df = build_features(curated_slice_df, args.model_version)
        emit_progress(
            spark,
            args.progress_output_path,
            "build_features",
            50,
            "Prepared lite Q2 feature transformations and starting write.",
            extra=progress_context,
        )

        print(f"Writing lite Q2 feature dataset to {args.output_path}")
        with SparkProgressReporter(
            spark,
            args.progress_output_path,
            "write_features",
            55,
            95,
            "Writing lite Q2 feature dataset.",
            extra=progress_context,
        ):
            (
                feature_df.write.mode("overwrite")
                .partitionBy("model_version", "year", "month")
                .parquet(args.output_path)
            )
        emit_progress(
            spark,
            args.progress_output_path,
            "completed",
            100,
            "Completed lite Q2 feature build.",
            extra=progress_context,
        )
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
