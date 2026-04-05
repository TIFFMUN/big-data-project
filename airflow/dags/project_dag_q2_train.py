"""
Q2 training DAG.

Run this DAG only when you want to train or retrain the model.
It is intentionally separate from the regular ingest + inference flow.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

from emr_config import (
    Q2_BUILD_FEATURES_S3_URI,
    Q2_EVAL_PATH,
    Q2_FEATURES_PATH,
    Q2_MODEL_PATH,
    Q2_TRAIN_MODEL_S3_URI,
    PROCESSED_DATASET_PATH,
    check_crawler_status,
    create_emr_cluster,
    submit_spark_step,
    terminate_emr_cluster,
    trigger_glue_crawler,
    upload_q2_scripts,
    wait_for_cluster,
    wait_for_step,
)

ACTIVE_MODEL_VARIABLE_NAME = "q2_active_model_version"


def _training_version(context) -> str:
    return context["ts_nodash"]


def task_upload_q2_scripts(**context):
    upload_q2_scripts()


def task_create_emr_cluster(**context):
    cluster_id = create_emr_cluster()
    context["ti"].xcom_push(key="cluster_id", value=cluster_id)


def task_wait_for_cluster_ready(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    return wait_for_cluster(cluster_id)


def task_submit_feature_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    training_version = _training_version(context)
    feature_output_path = f"{Q2_FEATURES_PATH}training_run={training_version}/"

    step_id = submit_spark_step(
        cluster_id=cluster_id,
        step_name="Q2-Train-Build-Features",
        script_s3_uri=Q2_BUILD_FEATURES_S3_URI,
        script_args=[
            PROCESSED_DATASET_PATH,
            feature_output_path,
            "--model-version",
            training_version,
        ],
    )

    context["ti"].xcom_push(key="feature_step_id", value=step_id)
    context["ti"].xcom_push(key="feature_output_path", value=feature_output_path)
    context["ti"].xcom_push(key="training_version", value=training_version)


def task_wait_for_feature_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(task_ids="submit_feature_step", key="feature_step_id")
    return wait_for_step(cluster_id, step_id)


def task_submit_train_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    feature_output_path = context["ti"].xcom_pull(task_ids="submit_feature_step", key="feature_output_path")
    training_version = context["ti"].xcom_pull(task_ids="submit_feature_step", key="training_version")

    model_path = f"{Q2_MODEL_PATH}model_version={training_version}/model"
    metrics_path = f"{Q2_MODEL_PATH}model_version={training_version}/metrics/metrics.json"
    evaluation_output_path = f"{Q2_EVAL_PATH}training_run={training_version}/"

    step_id = submit_spark_step(
        cluster_id=cluster_id,
        step_name="Q2-Train-Model",
        script_s3_uri=Q2_TRAIN_MODEL_S3_URI,
        script_args=[
            feature_output_path,
            model_path,
            metrics_path,
            evaluation_output_path,
            "--train-ratio",
            "0.80",
        ],
    )

    context["ti"].xcom_push(key="train_step_id", value=step_id)
    context["ti"].xcom_push(key="model_path", value=model_path)
    context["ti"].xcom_push(key="metrics_path", value=metrics_path)
    context["ti"].xcom_push(key="evaluation_output_path", value=evaluation_output_path)


def task_wait_for_train_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(task_ids="submit_train_step", key="train_step_id")
    return wait_for_step(cluster_id, step_id)


def task_mark_active_model(**context):
    training_version = context["ti"].xcom_pull(task_ids="submit_feature_step", key="training_version")
    Variable.set(ACTIVE_MODEL_VARIABLE_NAME, training_version)
    print(
        f"Set Airflow Variable {ACTIVE_MODEL_VARIABLE_NAME}={training_version}. "
        "Future inference runs will use this model version."
    )


def task_terminate_emr_cluster(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    if cluster_id:
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
    dag_id="project_dag_q2_train",
    default_args=default_args,
    description="Train or retrain the Q2 in-air delay model on EMR Spark",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["big-data", "q2", "training", "emr", "spark", "ml"],
) as dag:
    t_upload_scripts = PythonOperator(
        task_id="upload_q2_scripts",
        python_callable=task_upload_q2_scripts,
    )

    t_create = PythonOperator(
        task_id="create_emr_cluster",
        python_callable=task_create_emr_cluster,
    )

    t_wait_cluster = PythonOperator(
        task_id="wait_for_cluster_ready",
        python_callable=task_wait_for_cluster_ready,
    )

    t_submit_feature = PythonOperator(
        task_id="submit_feature_step",
        python_callable=task_submit_feature_step,
    )

    t_wait_feature = PythonOperator(
        task_id="wait_for_feature_step",
        python_callable=task_wait_for_feature_step,
    )

    t_submit_train = PythonOperator(
        task_id="submit_train_step",
        python_callable=task_submit_train_step,
    )

    t_wait_train = PythonOperator(
        task_id="wait_for_train_step",
        python_callable=task_wait_for_train_step,
    )

    t_mark_active_model = PythonOperator(
        task_id="mark_active_model",
        python_callable=task_mark_active_model,
    )

    t_terminate = PythonOperator(
        task_id="terminate_emr_cluster",
        python_callable=task_terminate_emr_cluster,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_crawl = PythonOperator(
        task_id="trigger_glue_crawler",
        python_callable=task_trigger_glue_crawler,
    )

    t_crawl_wait = PythonSensor(
        task_id="wait_for_crawler",
        python_callable=task_check_crawler_status,
        poke_interval=30,
        timeout=1200,
        mode="poke",
    )

    (
        t_upload_scripts
        >> t_create
        >> t_wait_cluster
        >> t_submit_feature
        >> t_wait_feature
        >> t_submit_train
        >> t_wait_train
        >> t_mark_active_model
        >> t_terminate
        >> t_crawl
        >> t_crawl_wait
    )
