"""
Q2 inference DAG.

Run this DAG whenever you want a new batch of predictions.
It reuses the currently active trained model version and does not retrain.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

from emr_config import (
    Q2_BATCH_INFERENCE_S3_URI,
    Q2_BUILD_FEATURES_S3_URI,
    Q2_FEATURES_PATH,
    Q2_MODEL_PATH,
    Q2_PREDICTIONS_PATH,
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


def _inference_batch_id(context) -> str:
    return context["ts_nodash"]


def task_validate_active_model(**context):
    active_model_version = Variable.get(ACTIVE_MODEL_VARIABLE_NAME, default_var=None)
    if not active_model_version:
        raise ValueError(
            f"Airflow Variable {ACTIVE_MODEL_VARIABLE_NAME} is not set. "
            "Run project_dag_q2_train once first, or set the variable manually."
        )

    context["ti"].xcom_push(key="active_model_version", value=active_model_version)
    print(f"Using active Q2 model version: {active_model_version}")


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
    active_model_version = context["ti"].xcom_pull(
        task_ids="validate_active_model",
        key="active_model_version",
    )
    inference_batch_id = _inference_batch_id(context)
    feature_output_path = f"{Q2_FEATURES_PATH}inference_run={inference_batch_id}/"

    step_id = submit_spark_step(
        cluster_id=cluster_id,
        step_name="Q2-Inference-Build-Features",
        script_s3_uri=Q2_BUILD_FEATURES_S3_URI,
        script_args=[
            PROCESSED_DATASET_PATH,
            feature_output_path,
            "--model-version",
            active_model_version,
        ],
    )

    context["ti"].xcom_push(key="feature_step_id", value=step_id)
    context["ti"].xcom_push(key="feature_output_path", value=feature_output_path)
    context["ti"].xcom_push(key="inference_batch_id", value=inference_batch_id)


def task_wait_for_feature_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(task_ids="submit_feature_step", key="feature_step_id")
    return wait_for_step(cluster_id, step_id)


def task_submit_inference_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    feature_output_path = context["ti"].xcom_pull(task_ids="submit_feature_step", key="feature_output_path")
    active_model_version = context["ti"].xcom_pull(
        task_ids="validate_active_model",
        key="active_model_version",
    )
    inference_batch_id = context["ti"].xcom_pull(
        task_ids="submit_feature_step",
        key="inference_batch_id",
    )

    model_path = f"{Q2_MODEL_PATH}model_version={active_model_version}/model"
    predictions_output_path = f"{Q2_PREDICTIONS_PATH}inference_run={inference_batch_id}/"

    step_id = submit_spark_step(
        cluster_id=cluster_id,
        step_name="Q2-Batch-Inference",
        script_s3_uri=Q2_BATCH_INFERENCE_S3_URI,
        script_args=[
            feature_output_path,
            model_path,
            predictions_output_path,
            "--model-version",
            active_model_version,
            "--inference-batch-id",
            inference_batch_id,
        ],
    )

    context["ti"].xcom_push(key="inference_step_id", value=step_id)
    context["ti"].xcom_push(key="predictions_output_path", value=predictions_output_path)


def task_wait_for_inference_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(task_ids="submit_inference_step", key="inference_step_id")
    return wait_for_step(cluster_id, step_id)


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
    dag_id="project_dag_q2_inference",
    default_args=default_args,
    description="Run Q2 batch inference using the active trained model version",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["big-data", "q2", "inference", "emr", "spark", "ml"],
) as dag:
    t_validate_model = PythonOperator(
        task_id="validate_active_model",
        python_callable=task_validate_active_model,
    )

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

    t_submit_inference = PythonOperator(
        task_id="submit_inference_step",
        python_callable=task_submit_inference_step,
    )

    t_wait_inference = PythonOperator(
        task_id="wait_for_inference_step",
        python_callable=task_wait_for_inference_step,
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
        t_validate_model
        >> t_upload_scripts
        >> t_create
        >> t_wait_cluster
        >> t_submit_feature
        >> t_wait_feature
        >> t_submit_inference
        >> t_wait_inference
        >> t_terminate
        >> t_crawl
        >> t_crawl_wait
    )
