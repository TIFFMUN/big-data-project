"""
Merge DAG for the Question 1 holiday-enrichment stage.

Flow:
1. Upload the holiday reference CSV to S3.
2. Upload the PySpark merge script to S3 /scripts/
3. Create an EMR cluster or reuse one passed in via dag_run.conf.
4. Submit the Spark merge step to EMR.
5. Wait for the step to complete.
6. Terminate the EMR cluster when this DAG owns the cluster lifecycle.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from emr_config import (
    create_emr_cluster,
    submit_merge_spark_step,
    terminate_emr_cluster,
    upload_holiday_reference,
    upload_merge_script,
    wait_for_cluster,
    wait_for_step,
)


DEFAULT_DAYS_BEFORE = 7
DEFAULT_DAYS_AFTER = 7


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


def task_upload_holiday_reference(**context):
    upload_holiday_reference()


def task_upload_merge_script(**context):
    upload_merge_script()


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


def task_submit_merge_spark_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    if not cluster_id:
        raise ValueError("EMR cluster_id was not found for submit_merge_spark_step.")

    step_id = submit_merge_spark_step(
        cluster_id=cluster_id,
        days_before=DEFAULT_DAYS_BEFORE,
        days_after=DEFAULT_DAYS_AFTER,
    )
    context["ti"].xcom_push(key="step_id", value=step_id)
    return step_id


def task_wait_for_step_completion(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(task_ids="submit_merge_spark_step", key="step_id")
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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="project_dag_merge",
    default_args=default_args,
    description="Run the Question 1 holiday merge job on EMR",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["big-data", "merge", "holiday", "emr", "pyspark"],
) as dag:
    t_upload_holiday_reference = PythonOperator(
        task_id="upload_holiday_reference",
        python_callable=task_upload_holiday_reference,
    )

    t_upload_merge_script = PythonOperator(
        task_id="upload_merge_script",
        python_callable=task_upload_merge_script,
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
        task_id="submit_merge_spark_step",
        python_callable=task_submit_merge_spark_step,
    )

    t_wait_step = PythonOperator(
        task_id="wait_for_step_completion",
        python_callable=task_wait_for_step_completion,
    )

    t_terminate = PythonOperator(
        task_id="terminate_emr_cluster",
        python_callable=task_terminate_emr_cluster,
    )

    (
        t_upload_holiday_reference
        >> t_upload_merge_script
        >> t_create
        >> t_wait_cluster
        >> t_submit
        >> t_wait_step
        >> t_terminate
    )
