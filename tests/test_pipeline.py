"""
test_pipeline.py
----------------
Unit tests for the Cloud ETL Pipeline.
Run with: pytest tests/test_pipeline.py -v
"""

import pytest
import pandas as pd
import numpy as np
import sqlite3
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.extractor.extractor         import generate_transactions, validate_schema
from src.transformer.transformer     import (
    remove_duplicates, remove_invalid_amounts,
    impute_missing_values, standardize_formats,
    filter_completed_transactions, enrich_transactions
)


# ── Fixtures ───────────────────────────────────────────────────────

@pytest.fixture
def raw_df():
    return generate_transactions(n=1000)


@pytest.fixture
def clean_df():
    return pd.DataFrame({
        "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004"],
        "date":           ["2024-01-15", "2024-02-20", "2024-03-10", "2024-06-05"],
        "merchant":       ["goldman sachs", "jpmorgan", None, "hsbc"],
        "category":       ["Wire Transfer", None, "Card Payment", "ACH Payment"],
        "amount":         [15000.0, 500.0, -100.0, 75000.0],
        "currency":       ["usd", "eur", "gbp", "usd"],
        "status":         ["completed", "completed", "failed", "completed"],
        "account_id":     ["ACC1234", "ACC5678", "ACC9012", "ACC3456"],
        "fee_amount":     [15.0, 0.5, 0.1, 75.0],
    })


# ── Extraction Tests ───────────────────────────────────────────────

class TestExtraction:

    def test_generates_correct_base_count(self, raw_df):
        """Should generate more than base n due to injected duplicates."""
        assert len(raw_df) > 1000

    def test_required_columns_exist(self, raw_df):
        for col in ["transaction_id", "date", "amount", "merchant", "category", "status"]:
            assert col in raw_df.columns

    def test_schema_validation_passes(self, raw_df):
        assert validate_schema(raw_df) is True

    def test_has_duplicates(self, raw_df):
        assert raw_df.duplicated(subset=["transaction_id"]).sum() > 0

    def test_has_missing_values(self, raw_df):
        assert raw_df["merchant"].isnull().sum() > 0


# ── Transformation Tests ───────────────────────────────────────────

class TestTransformation:

    def test_remove_duplicates(self, raw_df):
        deduped = remove_duplicates(raw_df)
        assert deduped.duplicated(subset=["transaction_id"]).sum() == 0

    def test_remove_invalid_amounts(self, clean_df):
        result = remove_invalid_amounts(clean_df)
        assert (result["amount"] > 0).all()
        assert len(result) == 3  # One negative removed

    def test_impute_missing_merchant(self, clean_df):
        result = impute_missing_values(clean_df)
        assert result["merchant"].isnull().sum() == 0
        assert "Unknown Merchant" in result["merchant"].values

    def test_impute_missing_category(self, clean_df):
        result = impute_missing_values(clean_df)
        assert result["category"].isnull().sum() == 0
        assert "Uncategorized" in result["category"].values

    def test_standardize_currency_uppercase(self, clean_df):
        result = standardize_formats(clean_df)
        assert result["currency"].str.isupper().all()

    def test_standardize_status_lowercase(self, clean_df):
        result = standardize_formats(clean_df)
        assert result["status"].str.islower().all()

    def test_filter_keeps_only_completed(self, clean_df):
        std    = standardize_formats(clean_df)
        result = filter_completed_transactions(std)
        assert (result["status"] == "completed").all()

    def test_enrich_creates_net_amount(self, clean_df):
        std    = standardize_formats(clean_df)
        result = enrich_transactions(std)
        assert "net_amount" in result.columns
        assert (result["net_amount"] == result["amount"] - result["fee_amount"]).all()

    def test_enrich_large_transaction_flag(self, clean_df):
        std    = standardize_formats(clean_df)
        result = enrich_transactions(std)
        # TXN004 has amount 75000 which is > 50000
        large  = result[result["transaction_id"] == "TXN004"]
        assert large["is_large_transaction"].values[0] == 1

    def test_enrich_small_transaction_not_flagged(self, clean_df):
        std    = standardize_formats(clean_df)
        result = enrich_transactions(std)
        small  = result[result["transaction_id"] == "TXN002"]
        assert small["is_large_transaction"].values[0] == 0

    def test_enrich_extracts_quarter(self, clean_df):
        std    = standardize_formats(clean_df)
        result = enrich_transactions(std)
        assert "transaction_quarter" in result.columns
        assert result["transaction_quarter"].between(1, 4).all()
