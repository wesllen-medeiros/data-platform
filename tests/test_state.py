"""
Testes para load_state() e save_state().

Verifica que o watermark incremental é lido/gravado corretamente no MinIO.
"""
import importlib
import json
import sys
from unittest.mock import MagicMock

import pytest


# S3Error precisa herdar de Exception para ser usável no `except S3Error`
class FakeS3Error(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


# Substituir o atributo S3Error no módulo mockado E no módulo já importado do pipeline
sys.modules["minio"].error.S3Error = FakeS3Error

# Re-importar o pipeline para que a referência `S3Error` dentro do módulo aponte para FakeS3Error
import ingestion.postgres_pipeline as _pipeline_module
_pipeline_module.S3Error = FakeS3Error

from ingestion.postgres_pipeline import load_state, save_state

SOURCE = "orders"
BUCKET = "lakehouse"


class TestLoadState:

    def test_returns_none_when_key_not_found(self, mock_minio):
        mock_minio.get_object.side_effect = FakeS3Error("NoSuchKey")
        result = load_state(SOURCE, mock_minio, BUCKET)
        assert result is None

    def test_returns_dict_when_state_exists(self, mock_minio):
        state = {
            "source": SOURCE,
            "incremental_column": "updated_at",
            "last_value": "2024-01-15T10:00:00",
            "state_updated_at": "2024-01-15T10:05:00",
        }
        response_mock = MagicMock()
        response_mock.read.return_value = json.dumps(state).encode("utf-8")
        mock_minio.get_object.return_value = response_mock

        result = load_state(SOURCE, mock_minio, BUCKET)
        assert result == state

    def test_raises_on_unexpected_s3_error(self, mock_minio):
        mock_minio.get_object.side_effect = FakeS3Error("AccessDenied")
        with pytest.raises(FakeS3Error):
            load_state(SOURCE, mock_minio, BUCKET)

    def test_reads_from_correct_path(self, mock_minio):
        mock_minio.get_object.side_effect = FakeS3Error("NoSuchKey")
        load_state(SOURCE, mock_minio, BUCKET)
        call_args = mock_minio.get_object.call_args
        path_arg = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("object_name")
        assert f"source={SOURCE}" in path_arg
        assert "watermark.json" in path_arg

    def test_last_value_accessible(self, mock_minio):
        state = {"last_value": "2024-06-01T00:00:00", "incremental_column": "updated_at"}
        response_mock = MagicMock()
        response_mock.read.return_value = json.dumps(state).encode("utf-8")
        mock_minio.get_object.return_value = response_mock

        result = load_state(SOURCE, mock_minio, BUCKET)
        assert result["last_value"] == "2024-06-01T00:00:00"


class TestSaveState:

    def test_put_object_called(self, mock_minio):
        save_state(SOURCE, "updated_at", "2024-06-01", mock_minio, BUCKET)
        mock_minio.put_object.assert_called_once()

    def test_saved_json_has_required_keys(self, mock_minio):
        captured = {}

        def capture(bucket, path, data, **kwargs):
            data.seek(0)
            captured["state"] = json.loads(data.read().decode("utf-8"))

        mock_minio.put_object.side_effect = capture
        save_state(SOURCE, "updated_at", "2024-06-01T10:00:00", mock_minio, BUCKET)

        state = captured["state"]
        assert "source" in state
        assert "incremental_column" in state
        assert "last_value" in state
        assert "state_updated_at" in state

    def test_last_value_serialized_as_string(self, mock_minio):
        captured = {}

        def capture(bucket, path, data, **kwargs):
            data.seek(0)
            captured["state"] = json.loads(data.read().decode("utf-8"))

        mock_minio.put_object.side_effect = capture
        save_state(SOURCE, "updated_at", "2024-06-01", mock_minio, BUCKET)
        assert isinstance(captured["state"]["last_value"], str)

    def test_saves_to_correct_bucket(self, mock_minio):
        save_state(SOURCE, "updated_at", "2024-06-01", mock_minio, BUCKET)
        call_args = mock_minio.put_object.call_args
        bucket_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("bucket_name")
        assert bucket_arg == BUCKET

    def test_path_contains_source_name(self, mock_minio):
        save_state(SOURCE, "updated_at", "2024-06-01", mock_minio, BUCKET)
        call_args = mock_minio.put_object.call_args
        path_arg = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("object_name")
        assert f"source={SOURCE}" in path_arg
