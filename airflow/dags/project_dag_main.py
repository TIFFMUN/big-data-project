"""
Main orchestration DAG for the current project.

Regular pipeline behavior:
1. Refresh the curated airline dataset
2. Run Q2 inference using the currently active trained model

Training is intentionally separate and must be triggered via project_dag_q2_train.
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

    t_run_q2_inference_pipeline = TriggerDagRunOperator(
        task_id="run_q2_inference_pipeline",
        trigger_dag_id="project_dag_q2_inference",
        wait_for_completion=True,
        poke_interval=30,
    )

    t_run_ingest_pipeline >> t_run_q2_inference_pipeline
