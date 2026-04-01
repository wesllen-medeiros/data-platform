"""
DAG: dbt_gold
Executa transformações dbt da camada Gold (agregações analíticas).

Fluxo: Silver → Gold (orders → orders_metrics)
Schedule: 20,50 * * * *  (10 min após dbt_silver que roda em 10,40)
SLA PRD: Silver + 20 min
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
    "email_on_failure": False,
    "email_on_retry": False,
}

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_gold",
    description="Transformação Gold via dbt (Silver → orders_metrics)",
    schedule="20,50 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["transformation", "gold", "dbt"],
) as dag:

    run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select marts.orders_metrics --profiles-dir $DBT_PROFILES_DIR"
        ),
    )

    test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --select marts.orders_metrics --profiles-dir $DBT_PROFILES_DIR"
        ),
    )

    run_gold >> test_gold
