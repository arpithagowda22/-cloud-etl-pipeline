"""
glue_job.py
-----------
Production AWS Glue ETL script for the financial transaction pipeline.

This script runs as a serverless AWS Glue job, reading raw transaction
data from S3, transforming it using PySpark, and writing clean
Parquet files back to S3 for loading into Redshift.

Deployment:
    1. Upload this script to S3: s3://your-bucket/scripts/glue_job.py
    2. Create a Glue job pointing to this script
    3. Set the IAM role with S3 and Glue permissions
    4. Trigger via Airflow, EventBridge, or manually

NOTE: This script requires an AWS Glue environment to run.
      For local execution, use run_pipeline.py instead.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# ── Glue Job Initialization ────────────────────────────────────────
args        = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "RUN_DATE"])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
RUN_DATE  = args["RUN_DATE"]   # Format: YYYY-MM-DD

RAW_PATH       = f"s3://{S3_BUCKET}/raw/transactions/{RUN_DATE}/"
PROCESSED_PATH = f"s3://{S3_BUCKET}/processed/transactions/{RUN_DATE}/"

LARGE_TRANSACTION_THRESHOLD = 50000


def read_raw_data():
    """Read raw CSV transaction data from S3."""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)
    print(f"Raw records loaded: {df.count():,}")
    return df


def transform(df):
    """
    Apply full transformation pipeline using PySpark.

    Steps:
        1. Remove duplicates by transaction_id
        2. Remove invalid amounts (<= 0)
        3. Impute missing merchant and category
        4. Standardize string columns
        5. Filter to completed transactions only
        6. Enrich with derived columns
    """
    # Deduplicate
    df = df.dropDuplicates(["transaction_id"])

    # Remove invalid amounts
    df = df.filter(F.col("amount") > 0)

    # Impute missing values
    df = df.fillna({"merchant": "Unknown Merchant", "category": "Uncategorized"})

    # Standardize strings
    df = df.withColumn("merchant", F.initcap(F.trim(F.col("merchant"))))
    df = df.withColumn("category", F.initcap(F.trim(F.col("category"))))
    df = df.withColumn("currency", F.upper(F.trim(F.col("currency"))))
    df = df.withColumn("status",   F.lower(F.trim(F.col("status"))))

    # Filter completed transactions
    df = df.filter(F.col("status") == "completed")

    # Enrich
    df = df.withColumn("net_amount",           (F.col("amount") - F.col("fee_amount")).cast(DoubleType()))
    df = df.withColumn("is_large_transaction", (F.col("amount") >= LARGE_TRANSACTION_THRESHOLD).cast(IntegerType()))
    df = df.withColumn("transaction_year",     F.year(F.col("date").cast("date")))
    df = df.withColumn("transaction_month",    F.month(F.col("date").cast("date")))
    df = df.withColumn("transaction_quarter",  F.quarter(F.col("date").cast("date")))
    df = df.withColumn("processed_at",         F.current_timestamp())

    print(f"Records after transformation: {df.count():,}")
    return df


def write_to_s3(df):
    """
    Write transformed data to S3 as Parquet, partitioned by year/month.

    Parquet format enables efficient columnar queries in Redshift Spectrum
    and reduces storage costs compared to CSV.
    """
    df.write \
      .mode("overwrite") \
      .partitionBy("transaction_year", "transaction_month") \
      .parquet(PROCESSED_PATH)

    print(f"Data written to: {PROCESSED_PATH}")


def main():
    df = read_raw_data()
    df = transform(df)
    write_to_s3(df)
    job.commit()
    print("Glue job completed successfully.")


main()
