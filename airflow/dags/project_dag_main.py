"""
Main orchestration DAG for the current project.

This DAG owns the shared EMR cluster lifecycle and passes the cluster_id
to child DAGs through TriggerDagRunOperator conf.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from emr_config import create_emr_cluster, terminate_emr_cluster, wait_for_cluster


def task_create_emr_cluster(**context):
    return create_emr_cluster()


def task_wait_for_cluster_ready(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster")
    if not cluster_id:
        raise ValueError("EMR cluster_id was not found in XCom.")
    return wait_for_cluster(cluster_id)


def task_terminate_emr_cluster(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster")
    if not cluster_id:
        print("No EMR cluster_id found; skipping termination.")
        return
    terminate_emr_cluster(cluster_id)


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
    description="Main orchestration DAG for the airline data pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["big-data", "orchestration"],
) as dag:
    t_create_emr_cluster = PythonOperator(
        task_id="create_emr_cluster",
        python_callable=task_create_emr_cluster,
    )

    t_wait_for_cluster_ready = PythonOperator(
        task_id="wait_for_cluster_ready",
        python_callable=task_wait_for_cluster_ready,
    )

    t_run_ingest_pipeline = TriggerDagRunOperator(
        task_id="run_ingest_pipeline",
        trigger_dag_id="project_dag_ingest",
        conf={
            "cluster_id": "{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
            "manage_cluster": False,
        },
        wait_for_completion=True,
        poke_interval=30,
    )

    t_terminate_emr_cluster = PythonOperator(
        task_id="terminate_emr_cluster",
        python_callable=task_terminate_emr_cluster,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        t_create_emr_cluster
        >> t_wait_for_cluster_ready
        >> t_run_ingest_pipeline
        >> t_terminate_emr_cluster
    )
