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

SCRIPT_KEY = join_s3_key("scripts", "ingest.py")
RAW_DATASET_PREFIX = join_s3_key("raw", DATASET_SUBDIR)
PROCESSED_DATASET_PREFIX = join_s3_key("processed", DATASET_SUBDIR)
LOG_PREFIX = join_s3_key("emr-logs")

LOG_URI = f"s3://{S3_BUCKET}/{LOG_PREFIX}/"
SCRIPT_S3_URI = f"s3://{S3_BUCKET}/{SCRIPT_KEY}"
RAW_DATASET_PATH = f"s3://{S3_BUCKET}/{RAW_DATASET_PREFIX}/"
PROCESSED_DATASET_PATH = f"s3://{S3_BUCKET}/{PROCESSED_DATASET_PREFIX}/"

emr_client = boto3.client("emr", region_name=REGION)
glue_client = boto3.client("glue", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)


def find_local_ingest_script() -> str:
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        "/opt/airflow/scripts/ingest.py",
        os.path.join(dag_dir, "..", "..", "scripts", "ingest.py"),
        "/home/ec2-user/big-data-project/scripts/ingest.py",
    ]
    local_script = next((path for path in candidates if os.path.exists(path)), None)
    if local_script is None:
        raise FileNotFoundError(f"ingest.py not found in any of: {candidates}")
    return local_script


def upload_ingest_script() -> None:
    local_script = find_local_ingest_script()
    s3_client.upload_file(local_script, S3_BUCKET, SCRIPT_KEY)
    print(f"Uploaded {local_script} to {SCRIPT_S3_URI}")


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


def submit_spark_step(cluster_id: str) -> str:
    step = {
        "Name": "Airline-Ingest-PySpark-Step",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                SCRIPT_S3_URI,
                RAW_DATASET_PATH,
                PROCESSED_DATASET_PATH,
            ],
        },
    }

    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    step_id = response["StepIds"][0]
    print(f"Submitted step {step_id} to cluster {cluster_id}")
    return step_id


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
