"""
PostgreSQL Ingestion Pipeline
Lakehouse Architecture: Extract → RAW → Normalize → Validate → Bronze → Quarantine
"""

import gzip
import io
import json
import logging
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from airflow.hooks.base import BaseHook
from minio import Minio
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. EXTRACT
# ---------------------------------------------------------------------------

def extract(conn_id: str, query: str) -> list[dict]:
    log.info("Extracting data from PostgreSQL | conn_id=%s", conn_id)

    conn_obj = BaseHook.get_connection(conn_id)
    import psycopg2

    with psycopg2.connect(
        host=conn_obj.host,
        port=conn_obj.port or 5432,
        dbname=conn_obj.schema,
        user=conn_obj.login,
        password=conn_obj.password,
    ) as pg_conn:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            records = [dict(row) for row in cur.fetchall()]

    log.info("Extracted %d records", len(records))
    return records


# ---------------------------------------------------------------------------
# 2. RAW
# ---------------------------------------------------------------------------

def save_raw(
    records: list[dict],
    source_name: str,
    batch_id: str,
    minio_client: Minio,
    bucket: str,
    execution_date: datetime,
) -> str:

    y = execution_date.strftime("%Y")
    m = execution_date.strftime("%m")
    d = execution_date.strftime("%d")

    raw_path = f"raw/source={source_name}/{y}/{m}/{d}/batch_{batch_id}.jsonl.gz"

    log.info("Writing RAW | path=%s", raw_path)

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for record in records:
            gz.write((json.dumps(record, default=str) + "\n").encode("utf-8"))

    buf.seek(0)

    minio_client.put_object(
        bucket_name=bucket,
        object_name=raw_path,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/gzip",
    )

    return raw_path


# ---------------------------------------------------------------------------
# 3. NORMALIZE
# ---------------------------------------------------------------------------

def normalize(records: list[dict]) -> pd.DataFrame:
    log.info("Normalizing records")

    df = pd.DataFrame(records)

    # NÃO forçar conversões aqui
    # Normalize = estrutura, não regra

    return df


# ---------------------------------------------------------------------------
# 4. VALIDATE (ALINHADO COM PRD)
# ---------------------------------------------------------------------------

def load_schema(schema_path: str) -> dict:
    with open(schema_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def validate(df: pd.DataFrame, schema: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info("Validating records against schema")

    fields = schema.get("fields", [])

    required_columns = [f["name"] for f in fields if f.get("required")]
    column_types = {f["name"]: f.get("type") for f in fields}

    errors = []

    for _, row in df.iterrows():
        row_errors = []

        # required
        for col in required_columns:
            if col not in row or pd.isna(row[col]):
                row_errors.append(f"missing_required:{col}")

        # types
        for col, expected_type in column_types.items():
            if col not in row:
                continue

            val = row[col]

            if val is None:
                continue

            if expected_type == "integer" and not isinstance(val, int):
                row_errors.append(f"type_error:{col}:integer")

            elif expected_type == "string" and not isinstance(val, str):
                row_errors.append(f"type_error:{col}:string")

            elif expected_type == "float" and not isinstance(val, (int, float)):
                row_errors.append(f"type_error:{col}:float")

        errors.append("; ".join(row_errors) if row_errors else None)

    df = df.copy()
    df["validation_error_temp"] = errors

    valid_df = df[df["validation_error_temp"].isna()].drop(columns=["validation_error_temp"])

    invalid_df = df[df["validation_error_temp"].notna()].rename(
        columns={"validation_error_temp": "validation_error"}
    )

    return valid_df, invalid_df


# ---------------------------------------------------------------------------
# 5. BRONZE
# ---------------------------------------------------------------------------

def save_bronze(
    df: pd.DataFrame,
    source_name: str,
    batch_id: str,
    raw_path: str,
    ingestion_timestamp: datetime,
    minio_client: Minio,
    bucket: str,
    execution_date: datetime,
) -> str:

    if df.empty:
        log.warning("No valid records for Bronze")
        return ""

    df = df.copy()

    df["ingestion_timestamp"] = ingestion_timestamp
    df["source"] = source_name
    df["batch_id"] = batch_id
    df["raw_path"] = raw_path

    y = execution_date.strftime("%Y")
    m = execution_date.strftime("%m")
    d = execution_date.strftime("%d")

    bronze_path = f"bronze/source={source_name}/year={y}/month={m}/day={d}/batch_{batch_id}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    minio_client.put_object(
        bucket_name=bucket,
        object_name=bronze_path,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    # TODO: integrar Apache Iceberg (fase futura)

    return bronze_path


# ---------------------------------------------------------------------------
# 6. QUARANTINE
# ---------------------------------------------------------------------------

def save_quarantine(
    df: pd.DataFrame,
    source_name: str,
    batch_id: str,
    raw_path: str,
    ingestion_timestamp: datetime,
    minio_client: Minio,
    bucket: str,
    execution_date: datetime,
) -> str:

    if df.empty:
        return ""

    df = df.copy()

    df["ingestion_timestamp"] = ingestion_timestamp
    df["source"] = source_name
    df["batch_id"] = batch_id
    df["raw_path"] = raw_path

    y = execution_date.strftime("%Y")
    m = execution_date.strftime("%m")
    d = execution_date.strftime("%d")

    path = f"bronze/quarantine/source={source_name}/year={y}/month={m}/day={d}/batch_{batch_id}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    minio_client.put_object(
        bucket_name=bucket,
        object_name=path,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    return path


# ---------------------------------------------------------------------------
# MINIO
# ---------------------------------------------------------------------------

def _build_minio_client(conn_id: str = "minio_default") -> Minio:
    conn = BaseHook.get_connection(conn_id)

    extras = conn.extra_dejson if conn.extra else {}

    endpoint = extras.get("endpoint_url", f"{conn.host}:{conn.port or 9000}")
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    return Minio(
        endpoint=endpoint,
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )


def _ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


# ---------------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------------

def run_pipeline(
    conn_id: str,
    query: str,
    source_name: str,
    schema_path: str,
    minio_bucket: str = "lakehouse",
    minio_conn_id: str = "minio_default",
    execution_date: datetime | None = None,
) -> dict[str, Any]:

    execution_date = execution_date or datetime.utcnow()
    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.utcnow()

    log.info("Pipeline start | batch_id=%s", batch_id)

    minio = _build_minio_client(minio_conn_id)
    _ensure_bucket(minio, minio_bucket)

    # Extract
    records = extract(conn_id, query)

    # RAW
    raw_path = save_raw(records, source_name, batch_id, minio, minio_bucket, execution_date)

    # Normalize
    df = normalize(records)

    # Validate
    schema = load_schema(schema_path)
    valid_df, invalid_df = validate(df, schema)

    # Bronze
    bronze_path = save_bronze(
        valid_df,
        source_name,
        batch_id,
        raw_path,
        ingestion_timestamp,
        minio,
        minio_bucket,
        execution_date,
    )

    # Quarantine (não quebra pipeline)
    quarantine_path = ""
    try:
        quarantine_path = save_quarantine(
            invalid_df,
            source_name,
            batch_id,
            raw_path,
            ingestion_timestamp,
            minio,
            minio_bucket,
            execution_date,
        )
    except Exception as e:
        log.error("Quarantine failed: %s", e)

    return {
        "batch_id": batch_id,
        "total": len(records),
        "valid": len(valid_df),
        "invalid": len(invalid_df),
        "raw_path": raw_path,
        "bronze_path": bronze_path,
        "quarantine_path": quarantine_path,
    }