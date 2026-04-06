"""
Shared Airflow configuration and helper functions for the lite Q2 training DAG.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Sequence

import boto3
from botocore.exceptions import ClientError

from emr_config import (
    CORE_TYPE,
    DATASET_SUBDIR,
    EMR_EC2_PROFILE,
    EMR_RELEASE,
    EMR_SERVICE_ROLE,
    MASTER_TYPE,
    PROCESSED_DATASET_PATH,
    TASK_TYPE,
    find_local_scripts_asset,
    join_s3_key,
    parse_s3_uri,
    sanitize_s3_key_segment,
    terminate_emr_cluster,
    wait_for_cluster,
)

S3_BUCKET = os.getenv("S3_BUCKET", "bigdata-project-data-lake")
REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

LITE_CORE_COUNT = 2
LITE_TASK_COUNT = 3

Q2_BUILD_FEATURES_LITE_KEY = join_s3_key("scripts", "q2_build_features_lite.py")
Q2_TRAIN_MODEL_LITE_KEY = join_s3_key("scripts", "q2_train_model_lite.py")

Q2_FEATURES_LITE_PREFIX = join_s3_key("processed", "q2_features_lite")
Q2_EVAL_LITE_PREFIX = join_s3_key("processed", "q2_model_eval_lite")
Q2_MODEL_LITE_PREFIX = join_s3_key("models", "q2_in_air_delay_lite")
LITE_LOG_PREFIX = join_s3_key("emr-logs", "lite")
LITE_PROGRESS_PREFIX = join_s3_key("metrics", DATASET_SUBDIR, "q2_lite_progress")

Q2_BUILD_FEATURES_LITE_S3_URI = f"s3://{S3_BUCKET}/{Q2_BUILD_FEATURES_LITE_KEY}"
Q2_TRAIN_MODEL_LITE_S3_URI = f"s3://{S3_BUCKET}/{Q2_TRAIN_MODEL_LITE_KEY}"
Q2_FEATURES_LITE_PATH = f"s3://{S3_BUCKET}/{Q2_FEATURES_LITE_PREFIX}/"
Q2_EVAL_LITE_PATH = f"s3://{S3_BUCKET}/{Q2_EVAL_LITE_PREFIX}/"
Q2_MODEL_LITE_PATH = f"s3://{S3_BUCKET}/{Q2_MODEL_LITE_PREFIX}/"
LOG_URI = f"s3://{S3_BUCKET}/{LITE_LOG_PREFIX}/"

emr_client = boto3.client("emr", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)


def upload_local_asset(asset_name: str, s3_key: str) -> None:
    local_asset = find_local_scripts_asset(asset_name)
    s3_client.upload_file(local_asset, S3_BUCKET, s3_key)
    print(f"Uploaded {local_asset} to s3://{S3_BUCKET}/{s3_key}")


def upload_q2_build_features_lite_script() -> None:
    upload_local_asset("q2_build_features_lite.py", Q2_BUILD_FEATURES_LITE_KEY)


def upload_q2_train_model_lite_script() -> None:
    upload_local_asset("q2_train_model_lite.py", Q2_TRAIN_MODEL_LITE_KEY)


def upload_q2_lite_scripts() -> None:
    upload_q2_build_features_lite_script()
    upload_q2_train_model_lite_script()


def build_lite_progress_path(run_id: str, step_name: str) -> str:
    safe_run_id = sanitize_s3_key_segment(run_id)
    safe_step_name = sanitize_s3_key_segment(step_name)
    return f"s3://{S3_BUCKET}/{join_s3_key(LITE_PROGRESS_PREFIX, safe_run_id, safe_step_name)}.json"


def create_emr_cluster() -> str:
    instances = {
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": MASTER_TYPE,
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": CORE_TYPE,
                "InstanceCount": LITE_CORE_COUNT,
            },
            {
                "Name": "Task",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": TASK_TYPE,
                "InstanceCount": LITE_TASK_COUNT,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    }

    response = emr_client.run_job_flow(
        Name="bigdata-project-emr-lite-train",
        ReleaseLabel=EMR_RELEASE,
        Applications=[{"Name": "Spark"}],
        Instances=instances,
        LogUri=LOG_URI,
        ServiceRole=EMR_SERVICE_ROLE,
        JobFlowRole=EMR_EC2_PROFILE,
        VisibleToAllUsers=True,
        AutoTerminationPolicy={"IdleTimeout": 3600},
        Tags=[
            {"Key": "Project", "Value": "big-data-project"},
            {"Key": "ManagedBy", "Value": "airflow"},
            {"Key": "Pipeline", "Value": "q2-lite-train"},
        ],
    )

    cluster_id = response["JobFlowId"]
    print(
        f"Created lite EMR cluster: {cluster_id} "
        f"(core_nodes={LITE_CORE_COUNT}, task_nodes={LITE_TASK_COUNT})"
    )
    return cluster_id


def submit_spark_script_step(
    cluster_id: str,
    step_name: str,
    script_s3_uri: str,
    script_args: Sequence[str],
) -> str:
    step = {
        "Name": step_name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                script_s3_uri,
                *script_args,
            ],
        },
    }

    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    step_id = response["StepIds"][0]
    print(f"Submitted lite step {step_id} to cluster {cluster_id}")
    return step_id


def submit_q2_build_features_lite_step(
    cluster_id: str,
    output_path: str,
    model_version: str,
    min_year: int = 2004,
    max_year: int = 2008,
    progress_output_path: str | None = None,
) -> str:
    script_args = [
        PROCESSED_DATASET_PATH,
        output_path,
        "--model-version",
        model_version,
        "--min-year",
        str(min_year),
        "--max-year",
        str(max_year),
    ]
    if progress_output_path:
        script_args.extend(["--progress-output-path", progress_output_path])

    return submit_spark_script_step(
        cluster_id=cluster_id,
        step_name="Q2-Build-Features-Lite-PySpark-Step",
        script_s3_uri=Q2_BUILD_FEATURES_LITE_S3_URI,
        script_args=script_args,
    )


def submit_q2_train_model_lite_step(
    cluster_id: str,
    input_path: str,
    model_path: str,
    metrics_path: str,
    evaluation_output_path: str,
    train_start_year: int = 2004,
    train_end_year: int = 2006,
    validation_year: int = 2007,
    test_year: int = 2008,
    progress_output_path: str | None = None,
) -> str:
    script_args = [
        input_path,
        model_path,
        metrics_path,
        evaluation_output_path,
        "--train-start-year",
        str(train_start_year),
        "--train-end-year",
        str(train_end_year),
        "--validation-year",
        str(validation_year),
        "--test-year",
        str(test_year),
    ]
    if progress_output_path:
        script_args.extend(["--progress-output-path", progress_output_path])

    return submit_spark_script_step(
        cluster_id=cluster_id,
        step_name="Q2-Train-Model-Lite-PySpark-Step",
        script_s3_uri=Q2_TRAIN_MODEL_LITE_S3_URI,
        script_args=script_args,
    )


def read_text_payload_from_s3_uri(s3_uri: str) -> tuple[str, str]:
    bucket, key = parse_s3_uri(s3_uri)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    etag = response.get("ETag", "").strip('"')
    return body.decode("utf-8").strip(), etag


def format_lite_progress_payload(payload: str) -> str:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return f"Lite progress | payload={payload}"

    parts = [f"phase={data.get('phase', 'unknown')}"]

    percent_complete = data.get("percent_complete")
    if percent_complete is not None:
        parts.append(f"percent={percent_complete}%")

    message = data.get("message")
    if message:
        parts.append(f"message={message}")

    train_rows = data.get("train_rows")
    if train_rows is not None:
        parts.append(f"train_rows={train_rows}")

    validation_rows = data.get("validation_rows")
    if validation_rows is not None:
        parts.append(f"validation_rows={validation_rows}")

    test_rows = data.get("test_rows")
    if test_rows is not None:
        parts.append(f"test_rows={test_rows}")

    spark_status = data.get("spark_status", {})
    completed_task_count = spark_status.get("completed_task_count")
    total_task_count = spark_status.get("total_task_count")
    if total_task_count:
        parts.append(f"tasks={completed_task_count}/{total_task_count}")

    active_task_count = spark_status.get("active_task_count")
    if active_task_count is not None:
        parts.append(f"active_tasks={active_task_count}")

    failed_task_count = spark_status.get("failed_task_count")
    if failed_task_count:
        parts.append(f"failed_tasks={failed_task_count}")

    active_stage_count = spark_status.get("active_stage_count")
    if active_stage_count is not None:
        parts.append(f"active_stages={active_stage_count}")

    updated_at = data.get("updated_at")
    if updated_at:
        parts.append(f"updated_at={updated_at}")

    return "Lite progress | " + " | ".join(parts)


def log_lite_progress_from_s3_uri(
    s3_uri: str,
    last_seen_etag: str | None = None,
) -> str | None:
    try:
        payload, etag = read_text_payload_from_s3_uri(s3_uri)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404"}:
            return last_seen_etag
        raise

    if etag and etag == last_seen_etag:
        return last_seen_etag

    print(format_lite_progress_payload(payload))
    return etag or last_seen_etag


def wait_for_step(
    cluster_id: str,
    step_id: str,
    progress_output_path: str | None = None,
) -> bool:
    last_progress_etag = None
    while True:
        desc = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        status = desc["Step"]["Status"]
        state = status["State"]
        timeline = status.get("Timeline", {})
        started_at = timeline.get("StartDateTime") or timeline.get("CreationDateTime")
        ended_at = timeline.get("EndDateTime")
        reason = status.get("StateChangeReason", {}).get("Message")
        failure_details = status.get("FailureDetails", {})

        elapsed = None
        if started_at:
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            reference_time = ended_at or datetime.now(timezone.utc)
            if reference_time.tzinfo is None:
                reference_time = reference_time.replace(tzinfo=timezone.utc)
            elapsed = f"{int((reference_time - started_at).total_seconds())}s"

        log_parts = [f"Step {step_id} on cluster {cluster_id} state: {state}"]
        if elapsed:
            log_parts.append(f"elapsed={elapsed}")
        if reason:
            log_parts.append(f"reason={reason}")

        failure_reason = failure_details.get("Reason")
        failure_message = failure_details.get("Message")
        failure_log_file = failure_details.get("LogFile")
        if failure_reason:
            log_parts.append(f"failure_reason={failure_reason}")
        if failure_message:
            log_parts.append(f"failure_message={failure_message}")
        if failure_log_file:
            log_parts.append(f"failure_log={failure_log_file}")

        log_message = " | ".join(log_parts)
        print(log_message)
        if progress_output_path:
            try:
                last_progress_etag = log_lite_progress_from_s3_uri(
                    progress_output_path,
                    last_seen_etag=last_progress_etag,
                )
            except Exception as exc:
                print(
                    f"Unable to load lite progress payload from "
                    f"{progress_output_path}: {exc}"
                )
        if state == "COMPLETED":
            return True
        if state in ("CANCELLED", "FAILED", "INTERRUPTED"):
            raise RuntimeError(log_message)
        time.sleep(30)
