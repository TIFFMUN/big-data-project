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

# Q1 - Query 1: Main persistence output
    q1_holiday_impact = AthenaOperator(
        task_id="q1_holiday_impact",
        query="""
        SELECT
            holiday_name,
            days_from_holiday,
            SUM(flight_count) AS total_flights,
            SUM(cancel_count) AS total_cancellations,
            SUM(diverted_count) AS total_diversions,
            AVG(avg_dep_delay) AS avg_dep_delay,
            AVG(avg_arr_delay) AS avg_arr_delay,
            AVG(cancel_rate) AS avg_cancel_rate,
            AVG(dep_delay_uplift_vs_baseline) AS avg_dep_delay_uplift,
            AVG(arr_delay_uplift_vs_baseline) AS avg_arr_delay_uplift,
            AVG(cancel_rate_uplift_vs_baseline) AS avg_cancel_rate_uplift
        FROM bigdata_project_db.aggregated
        GROUP BY holiday_name, days_from_holiday
        ORDER BY holiday_name, days_from_holiday
        """,
        database="bigdata_project_db",
        output_location="s3://{{ var.value.s3_bucket | default('bigdata-i767935-1775365227') }}/athena-results/",
    )

    # Q1 - Query 2: Holiday period summary
    q1_holiday_period_summary = AthenaOperator(
        task_id="q1_holiday_period_summary",
        query="""
        SELECT
            holiday_period,
            SUM(flight_count) AS total_flights,
            SUM(cancel_count) AS total_cancellations,
            SUM(diverted_count) AS total_diversions,
            AVG(avg_dep_delay) AS avg_dep_delay,
            AVG(avg_arr_delay) AS avg_arr_delay,
            AVG(cancel_rate) AS avg_cancel_rate,
            AVG(dep_delay_uplift_vs_baseline) AS avg_dep_delay_uplift,
            AVG(arr_delay_uplift_vs_baseline) AS avg_arr_delay_uplift,
            AVG(cancel_rate_uplift_vs_baseline) AS avg_cancel_rate_uplift
        FROM bigdata_project_db.aggregated
        GROUP BY holiday_period
        ORDER BY
            CASE holiday_period
                WHEN 'before' THEN 1
                WHEN 'holiday' THEN 2
                WHEN 'after' THEN 3
                ELSE 4
            END
        """,
        database="bigdata_project_db",
        output_location="s3://{{ var.value.s3_bucket | default('bigdata-i767935-1775365227') }}/athena-results/",
    )

    # Q3 - Query 3: Airport network impact
    q1_airport_network_impact = AthenaOperator(
        task_id="q1_airport_network_impact",
        query="""
        SELECT
            origin,
            holiday_name,
            AVG(avg_dep_delay) AS avg_dep_delay,
            AVG(avg_arr_delay) AS avg_arr_delay,
            AVG(cancel_rate) AS avg_cancel_rate,
            AVG(dep_delay_uplift_vs_baseline) AS avg_dep_delay_uplift,
            AVG(arr_delay_uplift_vs_baseline) AS avg_arr_delay_uplift,
            SUM(flight_count) AS total_flights
        FROM bigdata_project_db.aggregated
        WHERE holiday_period IN ('before', 'holiday', 'after')
        GROUP BY origin, holiday_name
        HAVING SUM(flight_count) >= 100
        ORDER BY avg_arr_delay DESC
        LIMIT 20
        """,
        database="bigdata_project_db",
        output_location="s3://{{ var.value.s3_bucket | default('bigdata-i767935-1775365227') }}/athena-results/",
    )