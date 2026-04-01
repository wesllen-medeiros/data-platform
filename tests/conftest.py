"""
conftest.py — fixtures e mocks globais.

As dependências externas (airflow, minio, psycopg2) são substituídas por
MagicMock antes de qualquer importação do pipeline, permitindo testar as
funções puras sem precisar de nenhum serviço rodando.
"""
import sys
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Stub de módulos externos — deve ocorrer ANTES de qualquer import do pipeline
# ---------------------------------------------------------------------------

_airflow_mock = MagicMock()
sys.modules.setdefault("airflow", _airflow_mock)
sys.modules.setdefault("airflow.hooks", _airflow_mock.hooks)
sys.modules.setdefault("airflow.hooks.base", _airflow_mock.hooks.base)

_minio_mock = MagicMock()
sys.modules.setdefault("minio", _minio_mock)
sys.modules.setdefault("minio.error", _minio_mock.error)

_psycopg2_mock = MagicMock()
sys.modules.setdefault("psycopg2", _psycopg2_mock)
sys.modules.setdefault("psycopg2.sql", _psycopg2_mock.sql)
sys.modules.setdefault("psycopg2.extras", _psycopg2_mock.extras)

# ---------------------------------------------------------------------------
# Fixtures compartilhadas
# ---------------------------------------------------------------------------

import pytest
import pandas as pd
from datetime import datetime


@pytest.fixture
def sample_records():
    """10 registros válidos de orders."""
    return [
        {
            "order_id": i,
            "customer_id": 100 + i,
            "status": "confirmed",
            "total_amount": float(i * 10),
            "created_at": "2024-01-01T00:00:00",
            "updated_at": "2024-01-02T00:00:00",
        }
        for i in range(1, 11)
    ]


@pytest.fixture
def sample_schema():
    """Schema YAML em memória alinhado com schemas/postgres_orders.yml."""
    return {
        "source": "postgres",
        "entity": "orders",
        "version": 1,
        "on_unknown_fields": "ignore",
        "fields": [
            {"name": "order_id",    "type": "integer", "required": True},
            {"name": "customer_id", "type": "integer", "required": True},
            {"name": "created_at",  "type": "string",  "required": True},
            {"name": "total_amount","type": "float",   "required": False},
            {"name": "status",      "type": "string",  "required": False},
            {"name": "updated_at",  "type": "string",  "required": False},
        ],
    }


@pytest.fixture
def execution_date():
    return datetime(2024, 6, 15, 10, 0, 0)


@pytest.fixture
def mock_minio():
    """MinIO client com put_object e get_object mockados."""
    client = MagicMock()
    client.bucket_exists.return_value = True
    return client
