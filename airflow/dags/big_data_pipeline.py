"""
Airflow DAG – Big Data Pipeline

Orchestration flow:
  1. Upload PySpark script to S3 /scripts/
  2. Create an EMR cluster (1 Master + 2 Core + 1 Task)
  3. Submit PySpark ETL step to EMR
  4. Wait for step completion
  5. Terminate EMR cluster
  6. Trigger Glue Crawler on /processed/
  7. Wait for crawler completion

Requires boto3 on the Airflow EC2 instance.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
import boto3
import time
import os

# ---------------------------------------------------------------------------
# Configuration – update these to match your terraform outputs
# ---------------------------------------------------------------------------
S3_BUCKET        = os.getenv("S3_BUCKET", "bigdata-project-data-lake")
REGION           = os.getenv("AWS_REGION", "us-east-1")
EMR_RELEASE      = "emr-6.15.0"
MASTER_TYPE      = "m5.xlarge"
CORE_TYPE        = "m5.xlarge"
TASK_TYPE        = "m5.xlarge"
EMR_SERVICE_ROLE = "bigdata-project-emr-service-role"
EMR_EC2_PROFILE  = "bigdata-project-emr-ec2-profile"
GLUE_CRAWLER     = "bigdata-project-processed-crawler"
LOG_URI          = f"s3://{S3_BUCKET}/emr-logs/"
SCRIPT_S3_URI    = f"s3://{S3_BUCKET}/scripts/etl_job.py"
RAW_PATH         = f"s3://{S3_BUCKET}/raw/"
PROCESSED_PATH   = f"s3://{S3_BUCKET}/processed/"

# ---------------------------------------------------------------------------
# Helper clients
# ---------------------------------------------------------------------------
emr_client  = boto3.client("emr", region_name=REGION)
glue_client = boto3.client("glue", region_name=REGION)
s3_client   = boto3.client("s3", region_name=REGION)


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def upload_script(**context):
    """Upload the PySpark script to S3 /scripts/."""
    candidates = [
        "/opt/airflow/scripts/etl_job.py",                              # Docker volume mount
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "..", "..", "scripts", "etl_job.py"),         # relative path
        "/home/ec2-user/big-data-project/scripts/etl_job.py",           # bare EC2 fallback
    ]
    local_script = next((p for p in candidates if os.path.exists(p)), None)
    if local_script is None:
        raise FileNotFoundError(
            f"etl_job.py not found in any of: {candidates}"
        )

    s3_client.upload_file(local_script, S3_BUCKET, "scripts/etl_job.py")
    print(f"Uploaded {local_script} → s3://{S3_BUCKET}/scripts/etl_job.py")


def create_emr_cluster(**context):
    """Create a transient EMR cluster and push cluster_id to XCom."""
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
    context["ti"].xcom_push(key="cluster_id", value=cluster_id)


def wait_for_cluster(**context):
    """Wait until the EMR cluster is in WAITING state."""
    cluster_id = context["ti"].xcom_pull(key="cluster_id")
    while True:
        desc = emr_client.describe_cluster(ClusterId=cluster_id)
        state = desc["Cluster"]["Status"]["State"]
        print(f"Cluster {cluster_id} state: {state}")
        if state == "WAITING":
            return True
        if state in ("TERMINATED", "TERMINATED_WITH_ERRORS"):
            raise RuntimeError(f"Cluster entered {state}")
        time.sleep(30)


def submit_spark_step(**context):
    """Submit the PySpark ETL step to the running EMR cluster."""
    cluster_id = context["ti"].xcom_pull(key="cluster_id")

    step = {
        "Name": "ETL-PySpark-Step",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                SCRIPT_S3_URI,
                RAW_PATH,
                PROCESSED_PATH,
            ],
        },
    }

    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id, Steps=[step]
    )
    step_id = response["StepIds"][0]
    print(f"Submitted step {step_id} to cluster {cluster_id}")
    context["ti"].xcom_push(key="step_id", value=step_id)


def wait_for_step(**context):
    """Wait until the Spark step completes."""
    cluster_id = context["ti"].xcom_pull(key="cluster_id")
    step_id = context["ti"].xcom_pull(key="step_id")
    while True:
        desc = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = desc["Step"]["Status"]["State"]
        print(f"Step {step_id} state: {state}")
        if state == "COMPLETED":
            return True
        if state in ("CANCELLED", "FAILED", "INTERRUPTED"):
            raise RuntimeError(f"Step {step_id} ended with state {state}")
        time.sleep(30)


def terminate_emr_cluster(**context):
    """Terminate the EMR cluster after job completion."""
    cluster_id = context["ti"].xcom_pull(key="cluster_id")
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f"Terminated cluster {cluster_id}")


def trigger_glue_crawler(**context):
    """Start the Glue Crawler to catalog /processed/ data."""
    glue_client.start_crawler(Name=GLUE_CRAWLER)
    print(f"Started Glue Crawler: {GLUE_CRAWLER}")


def check_crawler_status(**context):
    """Return True when the crawler is no longer RUNNING."""
    response = glue_client.get_crawler(Name=GLUE_CRAWLER)
    state = response["Crawler"]["State"]
    print(f"Crawler state: {state}")
    return state != "RUNNING"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="big_data_pipeline",
    default_args=default_args,
    description="End-to-end big data pipeline: EMR → PySpark → Glue Crawler",
    schedule_interval=None,  # trigger manually or set a cron
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["big-data", "emr", "glue", "pyspark"],
) as dag:

    t_upload = PythonOperator(
        task_id="upload_pyspark_script",
        python_callable=upload_script,
    )

    t_create = PythonOperator(
        task_id="create_emr_cluster",
        python_callable=create_emr_cluster,
    )

    t_wait_cluster = PythonOperator(
        task_id="wait_for_cluster_ready",
        python_callable=wait_for_cluster,
    )

    t_submit = PythonOperator(
        task_id="submit_spark_step",
        python_callable=submit_spark_step,
    )

    t_wait_step = PythonOperator(
        task_id="wait_for_step_completion",
        python_callable=wait_for_step,
    )

    t_terminate = PythonOperator(
        task_id="terminate_emr_cluster",
        python_callable=terminate_emr_cluster,
    )

    t_crawl = PythonOperator(
        task_id="trigger_glue_crawler",
        python_callable=trigger_glue_crawler,
    )

    t_crawl_wait = PythonSensor(
        task_id="wait_for_crawler",
        python_callable=check_crawler_status,
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    # Pipeline flow
    (
        t_upload
        >> t_create
        >> t_wait_cluster
        >> t_submit
        >> t_wait_step
        >> t_terminate
        >> t_crawl
        >> t_crawl_wait
    )


