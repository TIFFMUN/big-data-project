"""
Shared helper utilities for project Airflow DAGs.

These helpers parse dag_run.conf to determine whether the DAG should manage
the EMR cluster lifecycle itself or delegate it to a parent orchestrator.
"""


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
