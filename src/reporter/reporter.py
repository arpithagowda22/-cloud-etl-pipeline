"""
reporter.py
-----------
Queries the warehouse and generates three business summary reports:
    1. Daily Transaction Summary by category
    2. Merchant Performance Report
    3. Risk Flagging Report (large transactions)

Reports are saved as CSV files to the reports/ directory.
"""

import os
import sqlite3
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

WAREHOUSE_DB = "data/warehouse/warehouse.db"
REPORTS_PATH = "reports/"


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(WAREHOUSE_DB)


def generate_transaction_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Generate daily transaction summary grouped by category.

    Metrics: total volume, transaction count, average amount,
             total fees, net revenue.
    """
    query = """
        SELECT
            category,
            COUNT(*)                        AS transaction_count,
            ROUND(SUM(amount), 2)           AS total_volume,
            ROUND(AVG(amount), 2)           AS avg_transaction,
            ROUND(SUM(fee_amount), 2)       AS total_fees,
            ROUND(SUM(net_amount), 2)       AS net_volume,
            ROUND(SUM(amount) * 100.0 /
                (SELECT SUM(amount) FROM fact_transactions), 2)
                                            AS volume_pct
        FROM fact_transactions
        GROUP BY category
        ORDER BY total_volume DESC;
    """
    df = pd.read_sql_query(query, conn)
    logger.info(f"Transaction summary generated | categories={len(df)}")
    return df


def generate_merchant_report(conn: sqlite3.Connection, top_n: int = 10) -> pd.DataFrame:
    """
    Generate merchant performance report showing top merchants by revenue.

    Args:
        conn:  SQLite connection
        top_n: Number of top merchants to return
    """
    query = f"""
        SELECT
            merchant,
            COUNT(*)                  AS transaction_count,
            ROUND(SUM(amount), 2)     AS total_volume,
            ROUND(AVG(amount), 2)     AS avg_transaction,
            ROUND(SUM(fee_amount), 2) AS total_fees
        FROM fact_transactions
        WHERE merchant != 'Unknown Merchant'
        GROUP BY merchant
        ORDER BY total_volume DESC
        LIMIT {top_n};
    """
    df = pd.read_sql_query(query, conn)
    logger.info(f"Merchant report generated | top_merchants={len(df)}")
    return df


def generate_risk_report(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Generate risk flagging report for large transactions.

    Flags transactions above the configured threshold for
    compliance and risk review.
    """
    query = """
        SELECT
            transaction_id,
            date,
            merchant,
            category,
            ROUND(amount, 2)     AS amount,
            currency,
            account_id
        FROM fact_transactions
        WHERE is_large_transaction = 1
        ORDER BY amount DESC;
    """
    df = pd.read_sql_query(query, conn)
    logger.info(f"Risk report generated | flagged_transactions={len(df):,}")
    return df


def print_summary(summary_df: pd.DataFrame, merchant_df: pd.DataFrame,
                  risk_df: pd.DataFrame, total_records: int):
    """Print a formatted pipeline summary to console."""
    total_volume = summary_df["total_volume"].sum()
    total_txns   = summary_df["transaction_count"].sum()
    avg_txn      = total_volume / total_txns if total_txns else 0

    print("\n" + "=" * 60)
    print("BUSINESS REPORT SUMMARY")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"\n  Total transactions loaded : {total_records:,}")
    print(f"  Total volume              : ${total_volume:,.2f}")
    print(f"  Average transaction       : ${avg_txn:,.2f}")
    print(f"  Flagged (large)           : {len(risk_df):,}")

    print("\n  Volume by Category:")
    for _, row in summary_df.iterrows():
        print(f"    {row['category']:<25} ${row['total_volume']:>15,.2f}  ({row['volume_pct']}%)")

    print(f"\n  Top Merchant: {merchant_df.iloc[0]['merchant']} — ${merchant_df.iloc[0]['total_volume']:,.2f}")
    print("\n" + "=" * 60)


def run_reporting() -> dict:
    """
    Run all reports and save to reports/ directory.

    Returns:
        Dictionary of report DataFrames
    """
    os.makedirs(REPORTS_PATH, exist_ok=True)
    conn = get_connection()

    total_records = pd.read_sql_query(
        "SELECT COUNT(*) AS cnt FROM fact_transactions", conn
    ).iloc[0]["cnt"]

    summary_df  = generate_transaction_summary(conn)
    merchant_df = generate_merchant_report(conn)
    risk_df     = generate_risk_report(conn)

    # Save reports
    run_date = datetime.now().strftime("%Y-%m-%d")
    summary_df.to_csv( f"{REPORTS_PATH}transaction_summary_{run_date}.csv",  index=False)
    merchant_df.to_csv(f"{REPORTS_PATH}merchant_performance_{run_date}.csv", index=False)
    risk_df.to_csv(    f"{REPORTS_PATH}risk_flagging_{run_date}.csv",         index=False)

    print_summary(summary_df, merchant_df, risk_df, total_records)

    conn.close()
    logger.info(f"All reports saved to {REPORTS_PATH}")

    return {
        "summary":  summary_df,
        "merchant": merchant_df,
        "risk":     risk_df,
    }


if __name__ == "__main__":
    run_reporting()
