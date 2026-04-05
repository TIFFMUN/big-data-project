"""
Aggregate DAG for the Question 1 airport holiday mart stage.

Flow:
1. Upload the PySpark aggregate script to S3 /scripts/
2. Create an EMR cluster or reuse one passed in via dag_run.conf.
3. Submit the Spark aggregate step to EMR.
4. Wait for the step to complete.
5. Terminate the EMR cluster when this DAG owns the cluster lifecycle.
6. Trigger the Glue crawler on /processed/
7. Wait for crawler completion.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from emr_config import (
    check_crawler_status,
    create_emr_cluster,
    submit_aggregate_spark_step,
    terminate_emr_cluster,
    trigger_glue_crawler,
    upload_aggregate_script,
    wait_for_cluster,
    wait_for_step,
)


DEFAULT_DELAY_THRESHOLD = 15


def get_dag_run_conf(context) -> dict:
    dag_run = context.get("dag_run")
    if dag_run is None or dag_run.conf is None:
        return {}
    return dict(dag_run.conf)


def parse_manage_cluster(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"false", "0", "no"}:
            return False
        if normalized in {"true", "1", "yes"}:
            return True
    return bool(value)


def get_manage_cluster(context) -> bool:
    conf = get_dag_run_conf(context)
    return parse_manage_cluster(conf.get("manage_cluster", True))


def get_external_cluster_id(context) -> str | None:
    conf = get_dag_run_conf(context)
    cluster_id = conf.get("cluster_id")
    if cluster_id is None:
        return None
    cluster_id = str(cluster_id).strip()
    return cluster_id or None


def task_upload_aggregate_script(**context):
    upload_aggregate_script()


def task_create_emr_cluster(**context):
    if not get_manage_cluster(context):
        cluster_id = get_external_cluster_id(context)
        if not cluster_id:
            raise ValueError(
                "cluster_id is required in dag_run.conf when manage_cluster is False."
            )
        print(f"Using existing EMR cluster from dag_run.conf: {cluster_id}")
        context["ti"].xcom_push(key="cluster_id", value=cluster_id)
        return cluster_id

    cluster_id = create_emr_cluster()
    context["ti"].xcom_push(key="cluster_id", value=cluster_id)
    return cluster_id


def task_wait_for_cluster_ready(**context):
    if not get_manage_cluster(context):
        cluster_id = context["ti"].xcom_pull(
            task_ids="create_emr_cluster",
            key="cluster_id",
        )
        print(
            f"Cluster lifecycle is managed by the parent DAG; "
            f"skipping local readiness wait for {cluster_id}."
        )
        return True

    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    return wait_for_cluster(cluster_id)


def task_submit_aggregate_spark_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    if not cluster_id:
        raise ValueError("EMR cluster_id was not found for submit_aggregate_spark_step.")

    step_id = submit_aggregate_spark_step(
        cluster_id=cluster_id,
        delay_threshold=DEFAULT_DELAY_THRESHOLD,
    )
    context["ti"].xcom_push(key="step_id", value=step_id)
    return step_id


def task_wait_for_step_completion(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(
        task_ids="submit_aggregate_spark_step",
        key="step_id",
    )
    return wait_for_step(cluster_id, step_id)


def task_terminate_emr_cluster(**context):
    if not get_manage_cluster(context):
        cluster_id = context["ti"].xcom_pull(
            task_ids="create_emr_cluster",
            key="cluster_id",
        )
        print(
            f"Cluster lifecycle is managed by the parent DAG; "
            f"skipping local termination for {cluster_id}."
        )
        return

    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    terminate_emr_cluster(cluster_id)


def task_trigger_glue_crawler(**context):
    trigger_glue_crawler()


def task_check_crawler_status(**context):
    return check_crawler_status()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="project_dag_aggregate",
    default_args=default_args,
    description="Run the Question 1 aggregate holiday mart job on EMR",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["big-data", "aggregate", "holiday", "emr", "glue", "pyspark"],
) as dag:
    t_upload_aggregate_script = PythonOperator(
        task_id="upload_aggregate_script",
        python_callable=task_upload_aggregate_script,
    )

    t_create = PythonOperator(
        task_id="create_emr_cluster",
        python_callable=task_create_emr_cluster,
    )

    t_wait_cluster = PythonOperator(
        task_id="wait_for_cluster_ready",
        python_callable=task_wait_for_cluster_ready,
    )

    t_submit = PythonOperator(
        task_id="submit_aggregate_spark_step",
        python_callable=task_submit_aggregate_spark_step,
    )

    t_wait_step = PythonOperator(
        task_id="wait_for_step_completion",
        python_callable=task_wait_for_step_completion,
    )

    t_terminate = PythonOperator(
        task_id="terminate_emr_cluster",
        python_callable=task_terminate_emr_cluster,
    )

    t_crawl = PythonOperator(
        task_id="trigger_glue_crawler",
        python_callable=task_trigger_glue_crawler,
    )

    t_crawl_wait = PythonSensor(
        task_id="wait_for_crawler",
        python_callable=task_check_crawler_status,
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    (
        t_upload_aggregate_script
        >> t_create
        >> t_wait_cluster
        >> t_submit
        >> t_wait_step
        >> t_terminate
        >> t_crawl
        >> t_crawl_wait
    )
