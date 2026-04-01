"""
Testes para geração de paths e colunas de controle.

Verifica que RAW, Bronze e Quarantine seguem os padrões do PRD:
  RAW:        raw/source={source}/YYYY/MM/DD/batch_{uuid}.jsonl.gz
  Bronze:     bronze/source={source}/year=YYYY/month=MM/day=DD/batch_{uuid}.parquet
  Quarantine: bronze/quarantine/source={source}/year=YYYY/month=MM/day=DD/batch_{uuid}.parquet
"""
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion.postgres_pipeline import (
    normalize,
    save_bronze,
    save_quarantine,
    save_raw,
)

SOURCE = "orders"
BUCKET = "lakehouse"


@pytest.fixture
def batch_id():
    return str(uuid.uuid4())


@pytest.fixture
def ingestion_ts():
    return datetime(2024, 6, 15, 10, 0, 0)


@pytest.fixture
def valid_df():
    return normalize([{"order_id": 1, "customer_id": 1, "created_at": "2024-01-01"}])


@pytest.fixture
def invalid_df():
    df = normalize([{"order_id": None, "customer_id": 1, "created_at": "2024-01-01"}])
    df = df.copy()
    df["validation_error"] = "missing_required:order_id"
    return df


class TestRawPath:

    def test_path_follows_prd_pattern(self, mock_minio, batch_id, execution_date):
        records = [{"order_id": 1}]
        path = save_raw(records, SOURCE, batch_id, mock_minio, BUCKET, execution_date)
        assert path == f"raw/source={SOURCE}/2024/06/15/batch_{batch_id}.jsonl.gz"

    def test_path_uses_execution_date_not_today(self, mock_minio, batch_id):
        past_date = datetime(2023, 1, 5)
        records = [{"order_id": 1}]
        path = save_raw(records, SOURCE, batch_id, mock_minio, BUCKET, past_date)
        assert "2023/01/05" in path

    def test_put_object_called_once(self, mock_minio, batch_id, execution_date):
        records = [{"order_id": 1}]
        save_raw(records, SOURCE, batch_id, mock_minio, BUCKET, execution_date)
        mock_minio.put_object.assert_called_once()

    def test_put_object_uses_correct_bucket(self, mock_minio, batch_id, execution_date):
        records = [{"order_id": 1}]
        save_raw(records, SOURCE, batch_id, mock_minio, BUCKET, execution_date)
        call_kwargs = mock_minio.put_object.call_args
        assert call_kwargs.kwargs.get("bucket_name") == BUCKET or call_kwargs.args[0] == BUCKET

    def test_raw_content_is_gzip(self, mock_minio, batch_id, execution_date):
        """Verifica que o conteúdo enviado é gzip válido."""
        import gzip
        import io
        records = [{"order_id": 1, "status": "ok"}]
        save_raw(records, SOURCE, batch_id, mock_minio, BUCKET, execution_date)
        data_arg = mock_minio.put_object.call_args.kwargs.get("data") or mock_minio.put_object.call_args.args[2]
        data_arg.seek(0)
        with gzip.GzipFile(fileobj=data_arg) as gz:
            content = gz.read().decode("utf-8")
        assert '"order_id"' in content

    def test_returns_path_string(self, mock_minio, batch_id, execution_date):
        records = [{"order_id": 1}]
        result = save_raw(records, SOURCE, batch_id, mock_minio, BUCKET, execution_date)
        assert isinstance(result, str)
        assert result.startswith("raw/")


class TestBronzePath:

    def test_path_follows_prd_pattern(self, mock_minio, valid_df, batch_id, ingestion_ts, execution_date):
        raw_path = f"raw/source={SOURCE}/2024/06/15/batch_{batch_id}.jsonl.gz"
        path = save_bronze(valid_df, SOURCE, batch_id, raw_path, ingestion_ts, mock_minio, BUCKET, execution_date)
        assert path == f"bronze/source={SOURCE}/year=2024/month=06/day=15/batch_{batch_id}.parquet"

    def test_control_columns_added(self, mock_minio, valid_df, batch_id, ingestion_ts, execution_date):
        """PRD obriga: ingestion_timestamp, source, batch_id, raw_path."""
        raw_path = "raw/source=orders/2024/06/15/batch_x.jsonl.gz"

        # Captura o DataFrame enviado ao MinIO via pyarrow
        import pyarrow.parquet as pq
        import io

        captured = {}

        def capture_put(bucket_name, object_name, data, **kwargs):
            data.seek(0)
            captured["table"] = pq.read_table(data)

        mock_minio.put_object.side_effect = capture_put
        save_bronze(valid_df, SOURCE, batch_id, raw_path, ingestion_ts, mock_minio, BUCKET, execution_date)

        cols = captured["table"].schema.names
        assert "ingestion_timestamp" in cols
        assert "source" in cols
        assert "batch_id" in cols
        assert "raw_path" in cols

    def test_empty_df_returns_empty_string(self, mock_minio, batch_id, ingestion_ts, execution_date):
        empty_df = pd.DataFrame()
        result = save_bronze(empty_df, SOURCE, batch_id, "raw/x", ingestion_ts, mock_minio, BUCKET, execution_date)
        assert result == ""
        mock_minio.put_object.assert_not_called()

    def test_source_column_value(self, mock_minio, valid_df, batch_id, ingestion_ts, execution_date):
        import pyarrow.parquet as pq

        def capture_put(bucket_name, object_name, data, **kwargs):
            data.seek(0)
            capture_put.table = pq.read_table(data)

        mock_minio.put_object.side_effect = capture_put
        save_bronze(valid_df, SOURCE, batch_id, "raw/x", ingestion_ts, mock_minio, BUCKET, execution_date)
        sources = capture_put.table.column("source").to_pylist()
        assert all(s == SOURCE for s in sources)


class TestQuarantinePath:

    def test_path_follows_prd_pattern(self, mock_minio, invalid_df, batch_id, ingestion_ts, execution_date):
        raw_path = "raw/source=orders/2024/06/15/batch_x.jsonl.gz"
        path = save_quarantine(invalid_df, SOURCE, batch_id, raw_path, ingestion_ts, mock_minio, BUCKET, execution_date)
        assert path == f"bronze/quarantine/source={SOURCE}/year=2024/month=06/day=15/batch_{batch_id}.parquet"

    def test_empty_df_returns_empty_string(self, mock_minio, batch_id, ingestion_ts, execution_date):
        empty_df = pd.DataFrame()
        result = save_quarantine(empty_df, SOURCE, batch_id, "raw/x", ingestion_ts, mock_minio, BUCKET, execution_date)
        assert result == ""
        mock_minio.put_object.assert_not_called()

    def test_validation_error_column_preserved(self, mock_minio, invalid_df, batch_id, ingestion_ts, execution_date):
        import pyarrow.parquet as pq

        def capture_put(bucket_name, object_name, data, **kwargs):
            data.seek(0)
            capture_put.table = pq.read_table(data)

        mock_minio.put_object.side_effect = capture_put
        save_quarantine(invalid_df, SOURCE, batch_id, "raw/x", ingestion_ts, mock_minio, BUCKET, execution_date)
        assert "validation_error" in capture_put.table.schema.names
