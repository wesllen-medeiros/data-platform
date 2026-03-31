"""
DAG: ingest_postgres
Orquestra o pipeline de ingestão PostgreSQL → RAW → Bronze (Lakehouse Architecture).

Para adicionar uma nova tabela, basta incluir uma entrada em PIPELINE_CONFIGS.
Nenhuma lógica de ingestão vive aqui — toda transformação está em ingestion/postgres_pipeline.py.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from ingestion.postgres_pipeline import run_pipeline

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração por tabela
# ---------------------------------------------------------------------------

PIPELINE_CONFIGS = [
    {
        "task_id": "ingest_orders",
        "conn_id": Variable.get("postgres_conn_id", default_var="postgres_source"),
        "query": "SELECT * FROM public.orders",
        "source_name": "orders",
        "schema_path": Variable.get(
            "schemas_base_path", default_var="/opt/airflow/schemas"
        ) + "/postgres_orders.yml",
    },
]

# ---------------------------------------------------------------------------
# Parâmetros padrão da DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Wrapper da task
# ---------------------------------------------------------------------------

def _run_pipeline_task(
    conn_id: str,
    query: str,
    source_name: str,
    schema_path: str,
    minio_bucket: str,
    minio_conn_id: str,
    **context,
) -> dict:

    execution_date = context.get("data_interval_start") or datetime.utcnow()

    log.info(
        "DAG task start | source=%s | execution_date=%s",
        source_name,
        execution_date.isoformat(),
    )

    result = run_pipeline(
        conn_id=conn_id,
        query=query,
        source_name=source_name,
        schema_path=schema_path,
        minio_bucket=minio_bucket,
        minio_conn_id=minio_conn_id,
        execution_date=execution_date,
    )

    log.info(
        "DAG task end | source=%s | valid=%d | invalid=%d | batch_id=%s",
        source_name,
        result["valid"],
        result["invalid"],
        result["batch_id"],
    )

    return result


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ingest_postgres",
    description="Ingestão PostgreSQL → RAW → Bronze (Lakehouse)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion", "postgres", "bronze"],
) as dag:

    minio_bucket = Variable.get("minio_bucket", default_var="lakehouse")
    minio_conn_id = Variable.get("minio_conn_id", default_var="minio_default")

    tasks = []

    for config in PIPELINE_CONFIGS:
        task = PythonOperator(
            task_id=config["task_id"],
            python_callable=_run_pipeline_task,
            op_kwargs={
                "conn_id": config["conn_id"],
                "query": config["query"],
                "source_name": config["source_name"],
                "schema_path": config["schema_path"],
                "minio_bucket": minio_bucket,
                "minio_conn_id": minio_conn_id,
            },
        )

        tasks.append(task)