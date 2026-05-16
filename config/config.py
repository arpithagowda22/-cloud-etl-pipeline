"""
config.py
---------
Central configuration for the Cloud ETL Pipeline.
Switch between LOCAL and AWS mode by changing PIPELINE_MODE.
"""

import os

# ── Pipeline Mode ──────────────────────────────────────────────────
# Options: "local" or "aws"
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "local")

# ── Data Paths (Local Mode) ────────────────────────────────────────
RAW_DATA_PATH     = "data/raw/transactions.csv"
STAGING_PATH      = "data/staging/cleaned_transactions.csv"
WAREHOUSE_DB_PATH = "data/warehouse/warehouse.db"
REPORTS_PATH      = "reports/"

# ── Transaction Settings ───────────────────────────────────────────
LARGE_TRANSACTION_THRESHOLD = 50000   # Flag transactions above this amount
N_RECORDS                   = 50000   # Number of synthetic records to generate

TRANSACTION_CATEGORIES = [
    "Wire Transfer",
    "ACH Payment",
    "Card Payment",
    "International",
    "Internal Transfer",
    "Check Payment",
]

CURRENCY_CODES = ["USD", "EUR", "GBP", "CAD", "JPY"]

# ── AWS Configuration (Production Mode) ───────────────────────────
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET         = os.getenv("AWS_S3_BUCKET", "your-bucket-name")
S3_RAW_PREFIX         = "raw/transactions/"
S3_PROCESSED_PREFIX   = "processed/transactions/"

REDSHIFT_HOST         = os.getenv("REDSHIFT_HOST", "your-cluster.redshift.amazonaws.com")
REDSHIFT_PORT         = int(os.getenv("REDSHIFT_PORT", 5439))
REDSHIFT_DB           = os.getenv("REDSHIFT_DB", "finance_dw")
REDSHIFT_USER         = os.getenv("REDSHIFT_USER", "your_user")
REDSHIFT_PASSWORD     = os.getenv("REDSHIFT_PASSWORD", "your_password")
REDSHIFT_TABLE        = "fact_transactions"
