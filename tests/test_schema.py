"""
Testes para load_schema() e a estrutura do schema YAML real do projeto.

Verifica que:
  - load_schema() lê e parseia arquivos YAML corretamente
  - O schema real (schemas/postgres_orders.yml) segue o formato do PRD
"""
import os
import tempfile

import pytest
import yaml

from ingestion.postgres_pipeline import load_schema

REAL_SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schemas", "postgres_orders.yml"
)


class TestLoadSchema:

    def test_loads_valid_yaml(self, tmp_path):
        schema_file = tmp_path / "test.yml"
        schema_file.write_text(
            "source: test\nfields:\n  - name: id\n    type: integer\n    required: true\n"
        )
        result = load_schema(str(schema_file))
        assert result["source"] == "test"
        assert len(result["fields"]) == 1

    def test_returns_dict(self, tmp_path):
        schema_file = tmp_path / "schema.yml"
        schema_file.write_text("source: x\nfields: []\n")
        result = load_schema(str(schema_file))
        assert isinstance(result, dict)

    def test_raises_on_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_schema(str(tmp_path / "nonexistent.yml"))

    def test_fields_is_list(self, tmp_path):
        schema_file = tmp_path / "schema.yml"
        schema_file.write_text("source: x\nfields:\n  - name: col\n    type: string\n    required: false\n")
        result = load_schema(str(schema_file))
        assert isinstance(result["fields"], list)


class TestRealSchemaFormat:
    """Valida que schemas/postgres_orders.yml segue o PRD v2."""

    @pytest.fixture
    def real_schema(self):
        return load_schema(REAL_SCHEMA_PATH)

    def test_file_exists(self):
        assert os.path.exists(REAL_SCHEMA_PATH), "schemas/postgres_orders.yml não encontrado"

    def test_has_source_field(self, real_schema):
        assert "source" in real_schema

    def test_has_version(self, real_schema):
        assert "version" in real_schema
        assert isinstance(real_schema["version"], int)

    def test_has_fields_key(self, real_schema):
        assert "fields" in real_schema
        assert isinstance(real_schema["fields"], list)
        assert len(real_schema["fields"]) > 0

    def test_has_on_unknown_fields_policy(self, real_schema):
        assert "on_unknown_fields" in real_schema
        assert real_schema["on_unknown_fields"] in ("ignore", "quarantine", "include")

    def test_each_field_has_name(self, real_schema):
        for field in real_schema["fields"]:
            assert "name" in field, f"Campo sem 'name': {field}"

    def test_each_field_has_type(self, real_schema):
        for field in real_schema["fields"]:
            assert "type" in field, f"Campo '{field.get('name')}' sem 'type'"

    def test_each_field_has_required(self, real_schema):
        for field in real_schema["fields"]:
            assert "required" in field, f"Campo '{field.get('name')}' sem 'required'"

    def test_required_is_boolean(self, real_schema):
        for field in real_schema["fields"]:
            assert isinstance(field["required"], bool), (
                f"Campo '{field['name']}' tem required={field['required']!r}, esperado bool"
            )

    def test_type_values_are_valid(self, real_schema):
        valid_types = {"integer", "string", "float", "boolean"}
        for field in real_schema["fields"]:
            assert field["type"] in valid_types, (
                f"Campo '{field['name']}' tem type='{field['type']}' inválido"
            )

    def test_order_id_is_required(self, real_schema):
        order_id = next((f for f in real_schema["fields"] if f["name"] == "order_id"), None)
        assert order_id is not None, "Campo order_id ausente"
        assert order_id["required"] is True

    def test_customer_id_is_required(self, real_schema):
        customer_id = next((f for f in real_schema["fields"] if f["name"] == "customer_id"), None)
        assert customer_id is not None, "Campo customer_id ausente"
        assert customer_id["required"] is True

    def test_created_at_is_required(self, real_schema):
        created_at = next((f for f in real_schema["fields"] if f["name"] == "created_at"), None)
        assert created_at is not None, "Campo created_at ausente"
        assert created_at["required"] is True

    def test_incremental_column_updated_at_declared(self, real_schema):
        """updated_at é usada pelo pipeline incremental — deve estar no schema."""
        names = [f["name"] for f in real_schema["fields"]]
        assert "updated_at" in names, "Campo updated_at ausente — necessário para ingestão incremental"
