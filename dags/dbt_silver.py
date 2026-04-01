"""
DAG: dbt_silver
Executa transformações dbt da camada Silver (staging + marts).

Fluxo: Bronze → Silver (stg_orders → orders)
Schedule: 10,40 * * * *  (10 min após cada ingestão postgres que roda em */30)
SLA PRD: Bronze + 30 min
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_silver",
    description="Transformação Silver via dbt (Bronze → stg_orders → orders)",
    schedule="10,40 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["transformation", "silver", "dbt"],
) as dag:

    run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select tag:silver --profiles-dir $DBT_PROFILES_DIR"
        ),
    )

    test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --select tag:silver --profiles-dir $DBT_PROFILES_DIR"
        ),
    )

    run_silver >> test_silver
