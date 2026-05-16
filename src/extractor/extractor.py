"""
extractor.py
------------
Generates synthetic financial transaction data and validates
the ingested dataset before passing it downstream.

In AWS mode, this module would read from S3 instead of
generating data locally.
"""

import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

np.random.seed(42)

CATEGORIES  = ["Wire Transfer", "ACH Payment", "Card Payment",
                "International", "Internal Transfer", "Check Payment"]
CURRENCIES  = ["USD", "EUR", "GBP", "CAD", "JPY"]
MERCHANTS   = [
    "Goldman Sachs", "JPMorgan", "Bank of America", "Wells Fargo",
    "Citibank", "Morgan Stanley", "HSBC", "Barclays", "Deutsche Bank",
    "BNP Paribas", "UBS", "Credit Suisse", "Fidelity", "Vanguard", "BlackRock"
]
STATUSES    = ["completed", "completed", "completed", "pending", "failed"]


def generate_transactions(n: int = 50000) -> pd.DataFrame:
    """
    Generate synthetic financial transaction records.

    Includes intentional data quality issues:
        - ~3% duplicate transaction IDs
        - ~4% missing merchant names
        - ~2% missing categories
        - ~1% invalid amounts (negative)

    Args:
        n: Number of base records to generate

    Returns:
        Raw transactions DataFrame
    """
    start_date = datetime(2024, 1, 1)

    amounts = np.concatenate([
        np.random.exponential(500,  int(n * 0.80)),   # Most transactions: small
        np.random.exponential(5000, int(n * 0.15)),   # Mid-tier
        np.random.exponential(75000, int(n * 0.05)),  # Large transactions
    ])
    np.random.shuffle(amounts)
    amounts = amounts[:n].round(2)

    dates = [
        (start_date + timedelta(days=int(np.random.randint(0, 365)))).strftime("%Y-%m-%d")
        for _ in range(n)
    ]

    df = pd.DataFrame({
        "transaction_id":  [f"TXN{str(i).zfill(8)}" for i in range(n)],
        "date":            dates,
        "merchant":        np.random.choice(MERCHANTS, n),
        "category":        np.random.choice(CATEGORIES, n),
        "amount":          amounts,
        "currency":        np.random.choice(CURRENCIES, n, p=[0.70, 0.12, 0.08, 0.06, 0.04]),
        "status":          np.random.choice(STATUSES, n),
        "account_id":      [f"ACC{str(np.random.randint(1000, 9999))}" for _ in range(n)],
        "fee_amount":      (amounts * np.random.uniform(0.001, 0.005, n)).round(2),
    })

    # Inject duplicates (~3%)
    dup_idx = np.random.choice(df.index, size=int(n * 0.03), replace=False)
    df      = pd.concat([df, df.loc[dup_idx]], ignore_index=True)

    # Inject missing values
    df.loc[np.random.choice(df.index, int(n * 0.04), replace=False), "merchant"]  = np.nan
    df.loc[np.random.choice(df.index, int(n * 0.02), replace=False), "category"]  = np.nan

    # Inject invalid amounts (~1%)
    df.loc[np.random.choice(df.index, int(n * 0.01), replace=False), "amount"] = -abs(amounts[:int(n * 0.01)])

    return df


def validate_schema(df: pd.DataFrame) -> bool:
    """
    Validate that required columns exist and types are correct.

    Args:
        df: Raw DataFrame to validate

    Returns:
        True if schema is valid, raises ValueError if not
    """
    required_cols = ["transaction_id", "date", "merchant", "category", "amount", "currency", "status"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Schema validation failed — missing columns: {missing}")
    logger.info("Schema validation passed")
    return True


def run_extraction(n: int = 50000) -> pd.DataFrame:
    """
    Generate, validate, and save raw transaction data.

    In production (AWS mode), this function would instead:
        s3_client.get_object(Bucket=S3_BUCKET, Key=S3_RAW_PREFIX + filename)

    Args:
        n: Number of records to generate

    Returns:
        Raw transactions DataFrame
    """
    os.makedirs("data/raw", exist_ok=True)

    logger.info(f"Generating {n:,} synthetic transaction records...")
    df = generate_transactions(n)

    validate_schema(df)

    output_path = "data/raw/transactions.csv"
    df.to_csv(output_path, index=False)

    logger.info(f"Extraction complete | records={len(df):,} | saved to {output_path}")
    return df


if __name__ == "__main__":
    run_extraction()
