"""
transformer.py
--------------
Cleans, standardizes, and enriches raw transaction data.
Handles deduplication, missing values, validation, and
business rule enforcement.
"""

import os
import logging
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

LARGE_TRANSACTION_THRESHOLD = 50000

CATEGORY_DEFAULTS = {
    "Wire Transfer":    "Wire Transfer",
    "ACH":              "ACH Payment",
    "Card":             "Card Payment",
    "International":    "International",
}


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate transactions by transaction_id."""
    before = len(df)
    df     = df.drop_duplicates(subset=["transaction_id"])
    logger.info(f"Duplicates removed | dropped={before - len(df):,} | remaining={len(df):,}")
    return df


def remove_invalid_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """Remove transactions with zero or negative amounts."""
    before = len(df)
    df     = df[df["amount"] > 0]
    logger.info(f"Invalid amounts removed | dropped={before - len(df):,}")
    return df


def impute_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing merchant and category values with sensible defaults."""
    df["merchant"] = df["merchant"].fillna("Unknown Merchant")
    df["category"] = df["category"].fillna("Uncategorized")
    logger.info("Missing values imputed")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize date formats, currency codes, and string columns."""
    df["date"]     = pd.to_datetime(df["date"])
    df["merchant"] = df["merchant"].str.strip().str.title()
    df["category"] = df["category"].str.strip().str.title()
    df["currency"] = df["currency"].str.upper().str.strip()
    df["status"]   = df["status"].str.lower().str.strip()
    logger.info("Formats standardized")
    return df


def filter_completed_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only completed transactions for reporting."""
    before = len(df)
    df     = df[df["status"] == "completed"]
    logger.info(f"Non-completed filtered | dropped={before - len(df):,} | remaining={len(df):,}")
    return df


def enrich_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add business enrichment columns.

    Adds:
        net_amount      : amount after fees
        is_large        : flag for transactions above threshold
        transaction_year: year extracted from date
        transaction_month: month extracted from date
        transaction_quarter: fiscal quarter
    """
    df["net_amount"]          = (df["amount"] - df["fee_amount"]).round(2)
    df["is_large_transaction"]= (df["amount"] >= LARGE_TRANSACTION_THRESHOLD).astype(int)
    df["transaction_year"]    = df["date"].dt.year
    df["transaction_month"]   = df["date"].dt.month
    df["transaction_quarter"] = df["date"].dt.quarter
    df["processed_at"]        = pd.Timestamp.utcnow()

    large_count = df["is_large_transaction"].sum()
    logger.info(f"Enrichment complete | large_transactions={large_count:,}")
    return df


def run_transformation(input_path: str = "data/raw/transactions.csv") -> pd.DataFrame:
    """
    Run full transformation pipeline.

    Steps: load -> deduplicate -> remove invalid -> impute ->
           standardize -> filter -> enrich -> save

    Args:
        input_path: Path to raw CSV

    Returns:
        Cleaned and enriched DataFrame
    """
    os.makedirs("data/staging", exist_ok=True)

    df = pd.read_csv(input_path)
    logger.info(f"Loaded raw data | rows={len(df):,}")

    df = remove_duplicates(df)
    df = remove_invalid_amounts(df)
    df = impute_missing_values(df)
    df = standardize_formats(df)
    df = filter_completed_transactions(df)
    df = enrich_transactions(df)

    output_path = "data/staging/cleaned_transactions.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Transformation complete | final_rows={len(df):,} | saved to {output_path}")
    return df


if __name__ == "__main__":
    run_transformation()
