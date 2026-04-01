"""
PostgreSQL Ingestion Pipeline
Lakehouse Architecture: Extract → RAW → Normalize → Validate → Bronze → Quarantine

Suporta dois modos:
  - Full load   : incremental_column=None  (padrão, comportamento original)
  - Incremental : incremental_column="updated_at"  (filtra registros novos/alterados)

Estado incremental é persistido no MinIO em:
  state/source={source_name}/watermark.json
"""
from __future__ import annotations

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
from minio.error import S3Error
from psycopg2 import sql as pg_sql
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

STATE_PATH_TEMPLATE = "state/source={source_name}/watermark.json"


# ---------------------------------------------------------------------------
# STATE — watermark incremental
# ---------------------------------------------------------------------------

def load_state(source_name: str, minio_client: Minio, bucket: str) -> dict | None:
    """
    Lê o estado do último processamento no MinIO.
    Retorna None se não existir (→ full load).
    """
    path = STATE_PATH_TEMPLATE.format(source_name=source_name)
    try:
        response = minio_client.get_object(bucket, path)
        state = json.loads(response.read().decode("utf-8"))
        log.info("State loaded | source=%s | last_value=%s", source_name, state.get("last_value"))
        return state
    except S3Error as e:
        if e.code == "NoSuchKey":
            log.info("No state found for source=%s — will run full load", source_name)
            return None
        raise


def save_state(
    source_name: str,
    incremental_column: str,
    last_value: Any,
    minio_client: Minio,
    bucket: str,
) -> None:
    """Persiste o watermark do lote atual no MinIO."""
    path = STATE_PATH_TEMPLATE.format(source_name=source_name)
    state = {
        "source": source_name,
        "incremental_column": incremental_column,
        "last_value": str(last_value),
        "state_updated_at": datetime.utcnow().isoformat(),
    }
    payload = json.dumps(state).encode("utf-8")
    buf = io.BytesIO(payload)
    minio_client.put_object(bucket, path, buf, length=len(payload), content_type="application/json")
    log.info("State saved | source=%s | last_value=%s", source_name, last_value)


# ---------------------------------------------------------------------------
# 1. EXTRACT
# ---------------------------------------------------------------------------

def extract(
    conn_id: str,
    query: str,
    incremental_column: str | None = None,
    last_value: Any | None = None,
) -> list[dict]:
    """
    Extrai dados do PostgreSQL.

    Quando incremental_column e last_value são fornecidos, aplica filtro
    `WHERE {col} > %s` via psycopg2.sql (seguro contra SQL injection).
    Caso contrário, executa a query original (full load).
    """
    import psycopg2

    mode = "incremental" if (incremental_column and last_value is not None) else "full"
    log.info("Extracting | conn_id=%s | mode=%s", conn_id, mode)

    conn_obj = BaseHook.get_connection(conn_id)

    with psycopg2.connect(
        host=conn_obj.host,
        port=conn_obj.port or 5432,
        dbname=conn_obj.schema,
        user=conn_obj.login,
        password=conn_obj.password,
    ) as pg_conn:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            if mode == "incremental":
                # Coluna via pg_sql.Identifier (evita SQL injection no nome da coluna)
                # Valor via parâmetro %s (evita SQL injection no valor)
                stmt = pg_sql.SQL(
                    "SELECT * FROM ({base}) AS _src WHERE {col} > %s"
                ).format(
                    base=pg_sql.SQL(query),
                    col=pg_sql.Identifier(incremental_column),
                )
                log.info("Incremental filter | col=%s | last_value=%s", incremental_column, last_value)
                cur.execute(stmt, (last_value,))
            else:
                cur.execute(query)

            records = [dict(row) for row in cur.fetchall()]

    log.info("Extracted %d records | mode=%s", len(records), mode)
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

            if expected_type == "integer" and not isinstance(val, (int, float)):
                # pandas promove colunas int+null para float64 (ex: 1 → 1.0)
                # float é aceito aqui; strings/objetos continuam sendo erro
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
    incremental_column: str | None = None,
) -> dict[str, Any]:
    """
    Executa o pipeline completo: Extract → RAW → Normalize → Validate → Bronze → Quarantine.

    Parameters
    ----------
    incremental_column : str | None
        Nome da coluna de watermark (ex: "updated_at", "created_at").
        Quando None, executa full load.
        Quando definido, lê o último estado do MinIO e filtra apenas registros novos.
        Após Bronze gravado com sucesso, persiste o novo watermark.
    """
    execution_date = execution_date or datetime.utcnow()
    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.utcnow()
    mode = "incremental" if incremental_column else "full"

    log.info("Pipeline start | source=%s | batch_id=%s | mode=%s", source_name, batch_id, mode)

    minio = _build_minio_client(minio_conn_id)
    _ensure_bucket(minio, minio_bucket)

    # --- Carregar estado incremental (se aplicável) -----------------------
    last_value = None
    if incremental_column:
        state = load_state(source_name, minio, minio_bucket)
        if state:
            last_value = state.get("last_value")
        else:
            log.info("No prior state — running full load as baseline for source=%s", source_name)

    # --- Extract -----------------------------------------------------------
    records = extract(conn_id, query, incremental_column, last_value)

    if not records:
        log.info("No new records found | source=%s | mode=%s", source_name, mode)
        return {
            "batch_id": batch_id,
            "mode": mode,
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "raw_path": "",
            "bronze_path": "",
            "quarantine_path": "",
        }

    # --- RAW ---------------------------------------------------------------
    raw_path = save_raw(records, source_name, batch_id, minio, minio_bucket, execution_date)

    # --- Normalize ---------------------------------------------------------
    df = normalize(records)

    # --- Validate ----------------------------------------------------------
    schema = load_schema(schema_path)
    valid_df, invalid_df = validate(df, schema)

    # --- Bronze ------------------------------------------------------------
    bronze_path = save_bronze(
        valid_df, source_name, batch_id, raw_path, ingestion_timestamp,
        minio, minio_bucket, execution_date,
    )

    # --- Atualizar watermark (apenas após Bronze gravado com sucesso) ------
    if incremental_column and bronze_path and not valid_df.empty:
        if incremental_column in valid_df.columns:
            new_watermark = valid_df[incremental_column].max()
            save_state(source_name, incremental_column, new_watermark, minio, minio_bucket)
        else:
            log.warning(
                "incremental_column '%s' not found in data — state not updated", incremental_column
            )

    # --- Quarantine (nunca quebra o pipeline) ------------------------------
    quarantine_path = ""
    try:
        quarantine_path = save_quarantine(
            invalid_df, source_name, batch_id, raw_path, ingestion_timestamp,
            minio, minio_bucket, execution_date,
        )
    except Exception as e:
        log.error("Quarantine failed (pipeline continues) | error=%s", e)

    result = {
        "batch_id": batch_id,
        "mode": mode,
        "total": len(records),
        "valid": len(valid_df),
        "invalid": len(invalid_df),
        "raw_path": raw_path,
        "bronze_path": bronze_path,
        "quarantine_path": quarantine_path,
    }
    log.info("Pipeline complete | %s", result)
    return result