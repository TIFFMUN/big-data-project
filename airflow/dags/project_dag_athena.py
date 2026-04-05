"""
Athena Analysis DAG for querying processed flight data.

This DAG runs after the Glue Crawler completes and executes
analytical queries on the cataloged data.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="project_dag_athena",
    default_args=default_args,
    description="Run Athena queries on processed flight data",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["big-data", "athena", "analysis"],
) as dag:

    # Simple query to verify data is accessible
    # TODO: Team member will add actual analytical queries here
    query_processed_data = AthenaOperator(
        task_id="query_processed_data",
        query="""
            SELECT *
            FROM bigdata_project_db.processed
            LIMIT 100
        """,
        database="bigdata_project_db",
        output_location="s3://{{ var.value.s3_bucket | default('bigdata-i767935-1775365227') }}/athena-results/",
    )

    # Task runs independently
    query_processed_data
