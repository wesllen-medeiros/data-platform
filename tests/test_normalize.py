"""
Testes para a função normalize().

normalize() é pura: recebe list[dict], retorna DataFrame.
Não acessa nenhum serviço externo.
"""
import pandas as pd
import pytest

from ingestion.postgres_pipeline import normalize


class TestNormalize:

    def test_returns_dataframe(self, sample_records):
        result = normalize(sample_records)
        assert isinstance(result, pd.DataFrame)

    def test_row_count_matches_input(self, sample_records):
        result = normalize(sample_records)
        assert len(result) == len(sample_records)

    def test_column_names_preserved(self, sample_records):
        result = normalize(sample_records)
        expected_cols = set(sample_records[0].keys())
        assert expected_cols.issubset(set(result.columns))

    def test_empty_list_returns_empty_dataframe(self):
        result = normalize([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_values_preserved(self, sample_records):
        result = normalize(sample_records)
        assert result.iloc[0]["order_id"] == sample_records[0]["order_id"]
        assert result.iloc[0]["customer_id"] == sample_records[0]["customer_id"]

    def test_does_not_mutate_input(self, sample_records):
        original = [r.copy() for r in sample_records]
        normalize(sample_records)
        assert sample_records == original

    def test_single_record(self):
        records = [{"order_id": 42, "status": "pending"}]
        result = normalize(records)
        assert len(result) == 1
        assert result.iloc[0]["order_id"] == 42
