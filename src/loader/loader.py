"""
loader.py
---------
Loads cleaned transaction data into the local SQLite warehouse.

In production (AWS mode), this module loads into Amazon Redshift.
See src/aws/redshift_loader.py for the production implementation.
"""

import os
import sqlite3
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

WAREHOUSE_DB  = "data/warehouse/warehouse.db"
TABLE_NAME    = "fact_transactions"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      TEXT PRIMARY KEY,
    date                TEXT,
    merchant            TEXT,
    category            TEXT,
    amount              REAL,
    currency            TEXT,
    status              TEXT,
    account_id          TEXT,
    fee_amount          REAL,
    net_amount          REAL,
    is_large_transaction INTEGER,
    transaction_year    INTEGER,
    transaction_month   INTEGER,
    transaction_quarter INTEGER,
    processed_at        TEXT
);
"""

UPSERT_SQL = """
INSERT OR REPLACE INTO fact_transactions
VALUES (
    :transaction_id, :date, :merchant, :category, :amount,
    :currency, :status, :account_id, :fee_amount, :net_amount,
    :is_large_transaction, :transaction_year, :transaction_month,
    :transaction_quarter, :processed_at
);
"""


def get_connection() -> sqlite3.Connection:
    """Create and return a SQLite database connection."""
    os.makedirs("data/warehouse", exist_ok=True)
    return sqlite3.connect(WAREHOUSE_DB)


def create_table_if_not_exists(conn: sqlite3.Connection):
    """Create the fact_transactions table if it does not exist."""
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    logger.info("Warehouse table verified/created")


def load_to_warehouse(df: pd.DataFrame) -> int:
    """
    Load cleaned transactions into SQLite warehouse using upsert logic.

    Uses INSERT OR REPLACE to handle reruns without creating duplicates.
    In production (Redshift), this would use a MERGE statement or
    a stage-and-swap pattern.

    Args:
        df: Cleaned transactions DataFrame

    Returns:
        Number of records loaded
    """
    conn = get_connection()

    create_table_if_not_exists(conn)

    # Select only columns that match the table schema
    load_cols = [
        "transaction_id", "date", "merchant", "category", "amount",
        "currency", "status", "account_id", "fee_amount", "net_amount",
        "is_large_transaction", "transaction_year", "transaction_month",
        "transaction_quarter", "processed_at"
    ]
    df_load = df[[c for c in load_cols if c in df.columns]].copy()
    df_load["date"]         = df_load["date"].astype(str)
    df_load["processed_at"] = df_load["processed_at"].astype(str)

    records = df_load.to_dict(orient="records")
    conn.executemany(UPSERT_SQL, records)
    conn.commit()

    count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    conn.close()

    logger.info(f"Load complete | records_in_warehouse={count:,}")
    return count


def run_loading(input_path: str = "data/staging/cleaned_transactions.csv") -> int:
    """
    Load cleaned staging data into the local SQLite warehouse.

    In production, this calls redshift_loader.py instead.

    Args:
        input_path: Path to cleaned staging CSV

    Returns:
        Total record count in warehouse after load
    """
    df    = pd.read_csv(input_path)
    count = load_to_warehouse(df)
    return count


if __name__ == "__main__":
    run_loading()
