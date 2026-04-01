"""
Testes para a função validate().

validate() é pura: recebe DataFrame + schema dict,
retorna (valid_df, invalid_df). É a função com mais lógica de negócio.
"""
import pandas as pd
import pytest

from ingestion.postgres_pipeline import normalize, validate


class TestValidateAllValid:

    def test_all_valid_records_pass(self, sample_records, sample_schema):
        df = normalize(sample_records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(valid_df) == len(sample_records)
        assert invalid_df.empty

    def test_valid_df_has_no_validation_error_column(self, sample_records, sample_schema):
        df = normalize(sample_records)
        valid_df, _ = validate(df, sample_schema)
        assert "validation_error" not in valid_df.columns


class TestValidateMissingRequired:

    def test_missing_order_id_goes_to_quarantine(self, sample_schema):
        records = [{"customer_id": 1, "created_at": "2024-01-01", "order_id": None}]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(invalid_df) == 1
        assert valid_df.empty

    def test_missing_customer_id_goes_to_quarantine(self, sample_schema):
        records = [{"order_id": 1, "created_at": "2024-01-01", "customer_id": None}]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(invalid_df) == 1

    def test_missing_created_at_goes_to_quarantine(self, sample_schema):
        records = [{"order_id": 1, "customer_id": 1, "created_at": None}]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(invalid_df) == 1

    def test_validation_error_message_contains_field_name(self, sample_schema):
        records = [{"order_id": None, "customer_id": 1, "created_at": "2024-01-01"}]
        df = normalize(records)
        _, invalid_df = validate(df, sample_schema)
        assert "order_id" in invalid_df.iloc[0]["validation_error"]

    def test_multiple_missing_fields_reported_together(self, sample_schema):
        records = [{"order_id": None, "customer_id": None, "created_at": "2024-01-01"}]
        df = normalize(records)
        _, invalid_df = validate(df, sample_schema)
        error_msg = invalid_df.iloc[0]["validation_error"]
        assert "order_id" in error_msg
        assert "customer_id" in error_msg


class TestValidateTypeErrors:

    def test_string_in_integer_field_goes_to_quarantine(self, sample_schema):
        records = [{"order_id": "abc", "customer_id": 1, "created_at": "2024-01-01"}]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(invalid_df) == 1
        assert "type_error:order_id" in invalid_df.iloc[0]["validation_error"]

    def test_integer_in_string_field_goes_to_quarantine(self, sample_schema):
        records = [{"order_id": 1, "customer_id": 1, "created_at": 20240101}]
        df = normalize(records)
        _, invalid_df = validate(df, sample_schema)
        assert len(invalid_df) == 1
        assert "type_error:created_at" in invalid_df.iloc[0]["validation_error"]

    def test_string_in_float_field_goes_to_quarantine(self, sample_schema):
        records = [{"order_id": 1, "customer_id": 1, "created_at": "2024-01-01", "total_amount": "R$50"}]
        df = normalize(records)
        _, invalid_df = validate(df, sample_schema)
        assert len(invalid_df) == 1


class TestValidateOptionalFields:

    def test_null_optional_field_is_valid(self, sample_schema):
        records = [{"order_id": 1, "customer_id": 1, "created_at": "2024-01-01", "total_amount": None}]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(valid_df) == 1
        assert invalid_df.empty

    def test_optional_field_absent_is_valid(self, sample_schema):
        records = [{"order_id": 1, "customer_id": 1, "created_at": "2024-01-01"}]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(valid_df) == 1


class TestValidateMixedBatch:

    def test_splits_valid_and_invalid_correctly(self, sample_schema):
        records = [
            {"order_id": 1, "customer_id": 1, "created_at": "2024-01-01"},   # válido
            {"order_id": None, "customer_id": 1, "created_at": "2024-01-01"}, # inválido
            {"order_id": 2, "customer_id": 2, "created_at": "2024-01-01"},   # válido
            {"order_id": 3, "customer_id": None, "created_at": "2024-01-01"},# inválido
        ]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(valid_df) == 2
        assert len(invalid_df) == 2

    def test_invalid_df_has_validation_error_column(self, sample_schema):
        records = [{"order_id": None, "customer_id": 1, "created_at": "2024-01-01"}]
        df = normalize(records)
        _, invalid_df = validate(df, sample_schema)
        assert "validation_error" in invalid_df.columns

    def test_valid_records_not_contaminated_by_invalid(self, sample_schema):
        records = [
            {"order_id": 1, "customer_id": 1, "created_at": "2024-01-01"},
            {"order_id": None, "customer_id": 1, "created_at": "2024-01-01"},
        ]
        df = normalize(records)
        valid_df, _ = validate(df, sample_schema)
        assert valid_df.iloc[0]["order_id"] == 1


class TestValidateEdgeCases:

    def test_empty_dataframe_returns_two_empty_dfs(self, sample_schema):
        df = pd.DataFrame(columns=["order_id", "customer_id", "created_at"])
        valid_df, invalid_df = validate(df, sample_schema)
        assert valid_df.empty
        assert invalid_df.empty

    def test_empty_schema_all_records_valid(self, sample_records):
        empty_schema = {"fields": []}
        df = normalize(sample_records)
        valid_df, invalid_df = validate(df, empty_schema)
        assert len(valid_df) == len(sample_records)
        assert invalid_df.empty

    def test_unknown_columns_pass_through(self, sample_schema):
        """Campos não declarados no schema não causam erro (on_unknown_fields: ignore)."""
        records = [{"order_id": 1, "customer_id": 1, "created_at": "2024-01-01", "extra_col": "xyz"}]
        df = normalize(records)
        valid_df, invalid_df = validate(df, sample_schema)
        assert len(valid_df) == 1
        assert "extra_col" in valid_df.columns
