"""
Shared Airflow configuration and helper functions for the current project DAGs.
"""

import os
import time

import boto3


def join_s3_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part)


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

INGEST_SCRIPT_KEY = join_s3_key("scripts", "ingest.py")
Q2_BUILD_FEATURES_KEY = join_s3_key("scripts", "q2_build_features.py")
Q2_TRAIN_MODEL_KEY = join_s3_key("scripts", "q2_train_model.py")
Q2_BATCH_INFERENCE_KEY = join_s3_key("scripts", "q2_batch_inference.py")

RAW_DATASET_PREFIX = join_s3_key("raw", DATASET_SUBDIR)
PROCESSED_DATASET_PREFIX = join_s3_key("processed", DATASET_SUBDIR)
Q2_FEATURES_PREFIX = join_s3_key("processed", "q2_features")
Q2_EVAL_PREFIX = join_s3_key("processed", "q2_model_eval")
Q2_PREDICTIONS_PREFIX = join_s3_key("processed", "q2_predictions")
Q2_MODEL_PREFIX = join_s3_key("models", "q2_in_air_delay")
LOG_PREFIX = join_s3_key("emr-logs")

LOG_URI = f"s3://{S3_BUCKET}/{LOG_PREFIX}/"
INGEST_SCRIPT_S3_URI = f"s3://{S3_BUCKET}/{INGEST_SCRIPT_KEY}"
Q2_BUILD_FEATURES_S3_URI = f"s3://{S3_BUCKET}/{Q2_BUILD_FEATURES_KEY}"
Q2_TRAIN_MODEL_S3_URI = f"s3://{S3_BUCKET}/{Q2_TRAIN_MODEL_KEY}"
Q2_BATCH_INFERENCE_S3_URI = f"s3://{S3_BUCKET}/{Q2_BATCH_INFERENCE_KEY}"

RAW_DATASET_PATH = f"s3://{S3_BUCKET}/{RAW_DATASET_PREFIX}/"
PROCESSED_DATASET_PATH = f"s3://{S3_BUCKET}/{PROCESSED_DATASET_PREFIX}/"
Q2_FEATURES_PATH = f"s3://{S3_BUCKET}/{Q2_FEATURES_PREFIX}/"
Q2_EVAL_PATH = f"s3://{S3_BUCKET}/{Q2_EVAL_PREFIX}/"
Q2_PREDICTIONS_PATH = f"s3://{S3_BUCKET}/{Q2_PREDICTIONS_PREFIX}/"
Q2_MODEL_PATH = f"s3://{S3_BUCKET}/{Q2_MODEL_PREFIX}/"

emr_client = boto3.client("emr", region_name=REGION)
glue_client = boto3.client("glue", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)


def find_local_script(script_name: str) -> str:
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join("/opt/airflow/scripts", script_name),
        os.path.join(dag_dir, "..", "..", "scripts", script_name),
        os.path.join("/home/ec2-user/big-data-project/scripts", script_name),
    ]
    local_script = next((path for path in candidates if os.path.exists(path)), None)
    if local_script is None:
        raise FileNotFoundError(f"{script_name} not found in any of: {candidates}")
    return local_script


def upload_script(script_name: str, s3_key: str) -> str:
    local_script = find_local_script(script_name)
    s3_client.upload_file(local_script, S3_BUCKET, s3_key)
    script_s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    print(f"Uploaded {local_script} to {script_s3_uri}")
    return script_s3_uri


def upload_ingest_script() -> str:
    return upload_script("ingest.py", INGEST_SCRIPT_KEY)


def upload_q2_scripts() -> None:
    upload_script("q2_build_features.py", Q2_BUILD_FEATURES_KEY)
    upload_script("q2_train_model.py", Q2_TRAIN_MODEL_KEY)
    upload_script("q2_batch_inference.py", Q2_BATCH_INFERENCE_KEY)


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


def submit_spark_step(
    cluster_id: str,
    step_name: str,
    script_s3_uri: str,
    script_args: list[str] | None = None,
) -> str:
    args = [
        "spark-submit",
        "--deploy-mode",
        "cluster",
        script_s3_uri,
    ]
    if script_args:
        args.extend(script_args)

    step = {
        "Name": step_name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": args,
        },
    }

    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    step_id = response["StepIds"][0]
    print(f"Submitted step {step_id} ({step_name}) to cluster {cluster_id}")
    return step_id


def submit_ingest_spark_step(cluster_id: str) -> str:
    return submit_spark_step(
        cluster_id=cluster_id,
        step_name="Airline-Ingest-PySpark-Step",
        script_s3_uri=INGEST_SCRIPT_S3_URI,
        script_args=[RAW_DATASET_PATH, PROCESSED_DATASET_PATH],
    )


def wait_for_step(cluster_id: str, step_id: str) -> bool:
    while True:
        desc = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = desc["Step"]["Status"]["State"]
        print(f"Step {step_id} state: {state}")
        if state == "COMPLETED":
            return True
        if state in ("CANCELLED", "FAILED", "INTERRUPTED"):
            raise RuntimeError(f"Step {step_id} ended with state {state}")
        time.sleep(30)


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
