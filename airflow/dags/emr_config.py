"""
Shared Airflow configuration and helper functions for the current project DAGs.
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Sequence

import boto3


def join_s3_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part)


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {s3_uri}")
    without_scheme = s3_uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket:
        raise ValueError(f"Missing S3 bucket in URI: {s3_uri}")
    return bucket, key.rstrip("/")


def sanitize_s3_key_segment(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._=-]+", "_", value)
    return sanitized.strip("_") or "unknown"


def build_ingest_metrics_path(run_id: str) -> str:
    safe_run_id = sanitize_s3_key_segment(run_id)
    return f"s3://{S3_BUCKET}/{join_s3_key(INGEST_METRICS_PREFIX, safe_run_id)}/"


S3_BUCKET = os.getenv("S3_BUCKET", "bigdata-project-data-lake")
REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
EMR_RELEASE = "emr-6.15.0"
MASTER_TYPE = "m5.xlarge"
CORE_TYPE = "m5.xlarge"
TASK_TYPE = "m5.xlarge"
EMR_SERVICE_ROLE = "bigdata-project-emr-service-role"
EMR_EC2_PROFILE = "bigdata-project-emr-ec2-profile"
GLUE_CRAWLER = "bigdata-project-processed-crawler"
DATASET_SUBDIR = os.getenv("DATASET_SUBDIR", "airline_data")

SCRIPT_KEY = join_s3_key("scripts", "ingest.py")
MERGE_SCRIPT_KEY = join_s3_key("scripts", "merge.py")
AGGREGATE_SCRIPT_KEY = join_s3_key("scripts", "aggregate.py")
HOLIDAY_REFERENCE_KEY = join_s3_key("raw", "reference", "holiday_reference.csv")
RAW_DATASET_PREFIX = join_s3_key("raw", DATASET_SUBDIR)
PROCESSED_DATASET_PREFIX = join_s3_key("processed", DATASET_SUBDIR)
Q1_MERGED_PREFIX = join_s3_key("processed", "question_1", "merged")
Q1_AGGREGATED_PREFIX = join_s3_key("processed", "question_1", "aggregated")
METRICS_PREFIX = join_s3_key("metrics", DATASET_SUBDIR)
INGEST_METRICS_PREFIX = join_s3_key(METRICS_PREFIX, "ingest")
LOG_PREFIX = join_s3_key("emr-logs")

LOG_URI = f"s3://{S3_BUCKET}/{LOG_PREFIX}/"
SCRIPT_S3_URI = f"s3://{S3_BUCKET}/{SCRIPT_KEY}"
MERGE_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/{MERGE_SCRIPT_KEY}"
AGGREGATE_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/{AGGREGATE_SCRIPT_KEY}"
HOLIDAY_REFERENCE_S3_URI = f"s3://{S3_BUCKET}/{HOLIDAY_REFERENCE_KEY}"
RAW_DATASET_PATH = f"s3://{S3_BUCKET}/{RAW_DATASET_PREFIX}/"
PROCESSED_DATASET_PATH = f"s3://{S3_BUCKET}/{PROCESSED_DATASET_PREFIX}/"
Q1_MERGED_PATH = f"s3://{S3_BUCKET}/{Q1_MERGED_PREFIX}/"
Q1_AGGREGATED_PATH = f"s3://{S3_BUCKET}/{Q1_AGGREGATED_PREFIX}/"
INGEST_METRICS_PATH = f"s3://{S3_BUCKET}/{INGEST_METRICS_PREFIX}/"

emr_client = boto3.client("emr", region_name=REGION)
glue_client = boto3.client("glue", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)


def find_local_scripts_asset(asset_name: str) -> str:
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join("/opt/airflow/scripts", asset_name),
        os.path.join(dag_dir, "..", "..", "scripts", asset_name),
        os.path.join("/home/ec2-user/big-data-project/scripts", asset_name),
    ]
    local_asset = next((path for path in candidates if os.path.exists(path)), None)
    if local_asset is None:
        raise FileNotFoundError(f"{asset_name} not found in any of: {candidates}")
    return local_asset


def find_local_ingest_script() -> str:
    return find_local_scripts_asset("ingest.py")


def upload_local_asset(asset_name: str, s3_key: str) -> None:
    local_asset = find_local_scripts_asset(asset_name)
    s3_client.upload_file(local_asset, S3_BUCKET, s3_key)
    print(f"Uploaded {local_asset} to s3://{S3_BUCKET}/{s3_key}")


def upload_ingest_script() -> None:
    upload_local_asset("ingest.py", SCRIPT_KEY)


def upload_merge_script() -> None:
    upload_local_asset("merge.py", MERGE_SCRIPT_KEY)


def upload_aggregate_script() -> None:
    upload_local_asset("aggregate.py", AGGREGATE_SCRIPT_KEY)


def upload_holiday_reference() -> None:
    upload_local_asset("holiday_reference.csv", HOLIDAY_REFERENCE_KEY)


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
                "InstanceCount": 2,
            },
            {
                "Name": "Task",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": TASK_TYPE,
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    }

    response = emr_client.run_job_flow(
        Name="bigdata-project-emr-transient",
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
        ],
    )

    cluster_id = response["JobFlowId"]
    print(f"Created EMR cluster: {cluster_id}")
    return cluster_id


def wait_for_cluster(cluster_id: str) -> bool:
    while True:
        desc = emr_client.describe_cluster(ClusterId=cluster_id)
        state = desc["Cluster"]["Status"]["State"]
        print(f"Cluster {cluster_id} state: {state}")
        if state == "WAITING":
            return True
        if state in ("TERMINATED", "TERMINATED_WITH_ERRORS"):
            raise RuntimeError(f"Cluster entered {state}")
        time.sleep(30)


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
    print(f"Submitted step {step_id} to cluster {cluster_id}")
    return step_id


def submit_spark_step(
    cluster_id: str,
    metrics_output_path: str | None = None,
) -> str:
    script_args = [RAW_DATASET_PATH, PROCESSED_DATASET_PATH]
    if metrics_output_path:
        script_args.extend(["--metrics-output-path", metrics_output_path])

    return submit_spark_script_step(
        cluster_id=cluster_id,
        step_name="Airline-Ingest-PySpark-Step",
        script_s3_uri=SCRIPT_S3_URI,
        script_args=script_args,
    )


def submit_merge_spark_step(
    cluster_id: str,
    days_before: int = 7,
    days_after: int = 7,
) -> str:
    return submit_spark_script_step(
        cluster_id=cluster_id,
        step_name="Airline-Holiday-Merge-PySpark-Step",
        script_s3_uri=MERGE_SCRIPT_S3_URI,
        script_args=[
            PROCESSED_DATASET_PATH,
            HOLIDAY_REFERENCE_S3_URI,
            Q1_MERGED_PATH,
            "--days-before",
            str(days_before),
            "--days-after",
            str(days_after),
        ],
    )


def submit_aggregate_spark_step(
    cluster_id: str,
    delay_threshold: int = 15,
) -> str:
    return submit_spark_script_step(
        cluster_id=cluster_id,
        step_name="Airline-Holiday-Aggregate-PySpark-Step",
        script_s3_uri=AGGREGATE_SCRIPT_S3_URI,
        script_args=[
            Q1_MERGED_PATH,
            Q1_AGGREGATED_PATH,
            "--delay-threshold",
            str(delay_threshold),
        ],
    )


def wait_for_step(cluster_id: str, step_id: str) -> bool:
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
        if state == "COMPLETED":
            return True
        if state in ("CANCELLED", "FAILED", "INTERRUPTED"):
            raise RuntimeError(log_message)
        time.sleep(30)


def read_latest_text_payload_from_s3_prefix(s3_uri: str) -> tuple[str, str]:
    bucket, key_prefix = parse_s3_uri(s3_uri)
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{key_prefix.rstrip('/')}/",
    )
    candidates = [
        item
        for item in response.get("Contents", [])
        if not item["Key"].endswith("/")
        and not item["Key"].endswith("_SUCCESS")
        and not item["Key"].endswith(".crc")
    ]
    if not candidates:
        raise FileNotFoundError(f"No payload files found under {s3_uri}")

    latest = max(candidates, key=lambda item: item["LastModified"])
    body = s3_client.get_object(Bucket=bucket, Key=latest["Key"])["Body"].read()
    return body.decode("utf-8").strip(), f"s3://{bucket}/{latest['Key']}"


def log_metrics_from_s3_prefix(s3_uri: str) -> str:
    payload, payload_uri = read_latest_text_payload_from_s3_prefix(s3_uri)
    print(f"Loaded metrics payload from {payload_uri}")
    try:
        print(json.dumps(json.loads(payload), indent=2, sort_keys=True))
    except json.JSONDecodeError:
        print(payload)
    return payload


def terminate_emr_cluster(cluster_id: str) -> None:
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f"Terminated cluster {cluster_id}")


def trigger_glue_crawler() -> None:
    glue_client.start_crawler(Name=GLUE_CRAWLER)
    print(f"Started Glue crawler: {GLUE_CRAWLER}")


def check_crawler_status() -> bool:
    response = glue_client.get_crawler(Name=GLUE_CRAWLER)
    state = response["Crawler"]["State"]
    print(f"Crawler state: {state}")
    return state != "RUNNING"
