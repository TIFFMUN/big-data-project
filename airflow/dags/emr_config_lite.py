"""
Shared Airflow configuration and helper functions for the lite Q2 training DAG.
"""

from __future__ import annotations

import os
from typing import Sequence

import boto3

from emr_config import (
    CORE_TYPE,
    EMR_EC2_PROFILE,
    EMR_RELEASE,
    EMR_SERVICE_ROLE,
    MASTER_TYPE,
    PROCESSED_DATASET_PATH,
    TASK_TYPE,
    find_local_scripts_asset,
    join_s3_key,
    terminate_emr_cluster,
    wait_for_cluster,
    wait_for_step,
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
) -> str:
    return submit_spark_script_step(
        cluster_id=cluster_id,
        step_name="Q2-Build-Features-Lite-PySpark-Step",
        script_s3_uri=Q2_BUILD_FEATURES_LITE_S3_URI,
        script_args=[
            PROCESSED_DATASET_PATH,
            output_path,
            "--model-version",
            model_version,
            "--min-year",
            str(min_year),
            "--max-year",
            str(max_year),
        ],
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
) -> str:
    return submit_spark_script_step(
        cluster_id=cluster_id,
        step_name="Q2-Train-Model-Lite-PySpark-Step",
        script_s3_uri=Q2_TRAIN_MODEL_LITE_S3_URI,
        script_args=[
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
        ],
    )
