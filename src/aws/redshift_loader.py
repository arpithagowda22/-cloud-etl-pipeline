"""
redshift_loader.py
------------------
Production loader that copies processed Parquet data from S3
into Amazon Redshift using the COPY command.

This is the AWS production equivalent of src/loader/loader.py.

NOTE: Requires AWS credentials and a running Redshift cluster.
      For local execution, use run_pipeline.py instead.
"""

import logging
import boto3
import psycopg2
import os
from config.config import (
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB,
    REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_TABLE,
    AWS_S3_BUCKET, AWS_REGION, S3_PROCESSED_PREFIX
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id        VARCHAR(20)    ENCODE lzo,
    date                  DATE           ENCODE az64,
    merchant              VARCHAR(100)   ENCODE lzo,
    category              VARCHAR(50)    ENCODE lzo,
    amount                DECIMAL(18,2)  ENCODE az64,
    currency              VARCHAR(5)     ENCODE lzo,
    status                VARCHAR(20)    ENCODE lzo,
    account_id            VARCHAR(20)    ENCODE lzo,
    fee_amount            DECIMAL(18,2)  ENCODE az64,
    net_amount            DECIMAL(18,2)  ENCODE az64,
    is_large_transaction  SMALLINT       ENCODE az64,
    transaction_year      SMALLINT       ENCODE az64,
    transaction_month     SMALLINT       ENCODE az64,
    transaction_quarter   SMALLINT       ENCODE az64,
    processed_at          TIMESTAMP      ENCODE az64
)
DISTSTYLE KEY
DISTKEY (category)
SORTKEY (date, category);
-- DISTKEY on category optimizes GROUP BY category queries (most common in reporting)
-- SORTKEY on date enables efficient date range filtering
"""


def get_redshift_connection():
    """Return a psycopg2 connection to Redshift."""
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )


def get_iam_role_arn() -> str:
    """Retrieve the Glue/Redshift IAM role ARN from AWS."""
    iam    = boto3.client("iam", region_name=AWS_REGION)
    roles  = iam.list_roles()["Roles"]
    for role in roles:
        if "redshift" in role["RoleName"].lower() or "glue" in role["RoleName"].lower():
            return role["Arn"]
    raise ValueError("No Redshift/Glue IAM role found. Create one in AWS IAM.")


def create_staging_table(cursor, run_date: str):
    """
    Create a temporary staging table for the current run.

    Using a staging table and MERGE pattern avoids locking
    the production table during load — critical for large datasets.
    """
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS stg_transactions_{run_date.replace('-', '')}
        (LIKE {REDSHIFT_TABLE});
    """)
    logger.info("Staging table created")


def copy_from_s3(cursor, run_date: str, iam_role_arn: str):
    """
    Use Redshift COPY command to load Parquet files from S3.

    COPY is the most efficient way to load data into Redshift —
    it loads in parallel across all compute nodes.
    """
    s3_path = f"s3://{AWS_S3_BUCKET}/{S3_PROCESSED_PREFIX}{run_date}/"
    stg_table = f"stg_transactions_{run_date.replace('-', '')}"

    copy_sql = f"""
        COPY {stg_table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS PARQUET;
    """
    cursor.execute(copy_sql)
    logger.info(f"COPY from S3 complete | source={s3_path}")


def upsert_to_production(cursor, run_date: str):
    """
    Merge staging data into production table.

    Delete existing records for the run date, then insert new ones.
    This pattern ensures idempotent loads — safe to rerun.
    """
    stg_table = f"stg_transactions_{run_date.replace('-', '')}"

    # Delete existing records for this date (idempotent rerun safety)
    cursor.execute(f"""
        DELETE FROM {REDSHIFT_TABLE}
        WHERE date = '{run_date}';
    """)

    # Insert from staging
    cursor.execute(f"""
        INSERT INTO {REDSHIFT_TABLE}
        SELECT * FROM {stg_table};
    """)

    # Drop staging table
    cursor.execute(f"DROP TABLE IF EXISTS {stg_table};")
    logger.info("Upsert to production table complete")


def run_redshift_load(run_date: str) -> int:
    """
    Full Redshift load: create table -> stage -> COPY -> upsert.

    Args:
        run_date: Pipeline run date (YYYY-MM-DD)

    Returns:
        Total record count in Redshift after load
    """
    conn   = get_redshift_connection()
    cursor = conn.cursor()

    try:
        iam_role_arn = get_iam_role_arn()

        cursor.execute(CREATE_TABLE_SQL)
        create_staging_table(cursor, run_date)
        copy_from_s3(cursor, run_date, iam_role_arn)
        upsert_to_production(cursor, run_date)

        cursor.execute(f"SELECT COUNT(*) FROM {REDSHIFT_TABLE};")
        total = cursor.fetchone()[0]

        conn.commit()
        logger.info(f"Redshift load complete | total_records={total:,}")
        return total

    except Exception as e:
        conn.rollback()
        logger.error(f"Redshift load failed: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    from datetime import datetime
    run_date = datetime.today().strftime("%Y-%m-%d")
    run_redshift_load(run_date)
