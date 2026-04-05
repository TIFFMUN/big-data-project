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

    q1_holiday_impact = AthenaOperator(
        task_id="q1_holiday_impact",
        query="""
        SELECT
            daysfromholiday,
            COUNT(*) AS total_flights,
            AVG(depdelay) AS avg_depdelay,
            AVG(arrdelay) AS avg_arrdelay,
            AVG(totaldelay) AS avg_totaldelay,
            SUM(cancelled) AS total_cancelled,
            SUM(diverted) AS total_diverted,
            AVG(airportdelayedflights) AS avg_airport_delayed_flights,
            AVG(airportcancelledflights) AS avg_airport_cancelled_flights,
            AVG(airporttotalflights) AS avg_airport_total_flights
        FROM bigdata_project_db.processed
        WHERE daysfromholiday IS NOT NULL
        GROUP BY daysfromholiday
        ORDER BY daysfromholiday
        """,
        database="bigdata_project_db",
        output_location="s3://{{ var.value.s3_bucket | default('bigdata-i767935-1775365227') }}/athena-results/",
    )