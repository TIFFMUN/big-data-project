"""
Main orchestration DAG for the current project.

At the moment it delegates to the ingest pipeline DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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
    t_run_ingest_pipeline = TriggerDagRunOperator(
        task_id="run_ingest_pipeline",
        trigger_dag_id="project_dag_ingest",
        wait_for_completion=True,
        poke_interval=30,
    )

    t_run_ingest_pipeline
