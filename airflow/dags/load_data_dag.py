import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

IMAGE = "medium-python-engine:latest"
NETWORK = "data-platform"

COMMON_ENVIRONMENT = {
    "DB_HOST": os.environ.get("DB_HOST", "postgres-prod"),
    "DB_PORT": os.environ.get("DB_PORT", "5432"),
    "POSTGRES_DB": os.environ["POSTGRES_DB"],
    "POSTGRES_USER": os.environ["POSTGRES_USER"],
    "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
}

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def python_task(task_id, script):
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        command=f"python {script}",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        environment=COMMON_ENVIRONMENT,
        auto_remove="success",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=5),
    )


with DAG(
    dag_id="production_data_pipeline",
    description="Generate, validate, load and verify production data",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Copenhagen"),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["docker", "python", "postgres"],
) as dag:
    generate_data = python_task("generate_data", "generate_data.py")
    validate_data = python_task("validate_data", "validate_data.py")
    load_data = python_task("load_data", "load_data.py")
    quality_check = python_task("quality_check", "quality_check.py")

    generate_data >> validate_data >> load_data >> quality_check
