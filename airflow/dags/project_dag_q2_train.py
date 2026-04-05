"""
Q2 training DAG.

This DAG trains the Q2 in-air delay model on EMR and stores the active model
version in the Airflow Variable `q2_active_model_version`.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_utils import get_external_cluster_id, get_manage_cluster
from emr_config import (
    Q2_EVAL_PATH,
    Q2_FEATURES_PATH,
    Q2_MODEL_PATH,
    create_emr_cluster,
    submit_q2_build_features_step,
    submit_q2_train_model_step,
    terminate_emr_cluster,
    upload_q2_scripts,
    wait_for_cluster,
    wait_for_step,
)

DEFAULT_TRAIN_RATIO = 0.80


def _training_version(context) -> str:
    return context["ts_nodash"]


def task_upload_q2_scripts(**context):
    upload_q2_scripts()


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
        cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
        print(
            f"Cluster lifecycle is managed by the parent DAG; "
            f"skipping local readiness wait for {cluster_id}."
        )
        return True
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    return wait_for_cluster(cluster_id)


def task_submit_feature_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    model_version = _training_version(context)
    feature_output_path = f"{Q2_FEATURES_PATH}model_version={model_version}/"

    step_id = submit_q2_build_features_step(
        cluster_id=cluster_id,
        output_path=feature_output_path,
        model_version=model_version,
    )
    context["ti"].xcom_push(key="feature_step_id", value=step_id)
    context["ti"].xcom_push(key="feature_output_path", value=feature_output_path)
    context["ti"].xcom_push(key="model_version", value=model_version)
    return step_id


def task_wait_for_feature_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(task_ids="submit_feature_step", key="feature_step_id")
    return wait_for_step(cluster_id, step_id)


def task_submit_train_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    feature_output_path = context["ti"].xcom_pull(task_ids="submit_feature_step", key="feature_output_path")
    model_version = context["ti"].xcom_pull(task_ids="submit_feature_step", key="model_version")

    model_path = f"{Q2_MODEL_PATH}model_version={model_version}/model"
    metrics_path = f"{Q2_MODEL_PATH}model_version={model_version}/metrics/metrics.json"
    evaluation_output_path = f"{Q2_EVAL_PATH}model_version={model_version}/"

    step_id = submit_q2_train_model_step(
        cluster_id=cluster_id,
        input_path=feature_output_path,
        model_path=model_path,
        metrics_path=metrics_path,
        evaluation_output_path=evaluation_output_path,
        train_ratio=DEFAULT_TRAIN_RATIO,
    )
    context["ti"].xcom_push(key="train_step_id", value=step_id)
    context["ti"].xcom_push(key="model_path", value=model_path)
    return step_id


def task_wait_for_train_step(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    step_id = context["ti"].xcom_pull(task_ids="submit_train_step", key="train_step_id")
    return wait_for_step(cluster_id, step_id)


def task_set_active_model_version(**context):
    model_version = context["ti"].xcom_pull(task_ids="submit_feature_step", key="model_version")
    if not model_version:
        raise ValueError("model_version was not found; cannot set active model.")
    Variable.set("q2_active_model_version", model_version)
    print(f"Set Airflow Variable q2_active_model_version={model_version}")


def task_terminate_emr_cluster(**context):
    cluster_id = context["ti"].xcom_pull(task_ids="create_emr_cluster", key="cluster_id")
    if not cluster_id:
        print("No EMR cluster_id found; skipping termination.")
        return

    if not get_manage_cluster(context):
        print(
            f"Cluster lifecycle is managed by the parent DAG; "
            f"skipping local termination for {cluster_id}."
        )
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
    dag_id="project_dag_q2_train",
    default_args=default_args,
    description="Train the Q2 expected in-air delay model on EMR",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["big-data", "q2", "train", "emr", "spark-ml"],
) as dag:
    t_upload_q2_scripts = PythonOperator(
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

    t_set_active_model = PythonOperator(
        task_id="set_active_model_version",
        python_callable=task_set_active_model_version,
    )

    t_terminate = PythonOperator(
        task_id="terminate_emr_cluster",
        python_callable=task_terminate_emr_cluster,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        t_upload_q2_scripts
        >> t_create
        >> t_wait_cluster
        >> t_submit_feature
        >> t_wait_feature
        >> t_submit_train
        >> t_wait_train
        >> t_set_active_model
        >> t_terminate
    )
