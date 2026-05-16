"""
run_pipeline.py
---------------
Single entry point to run the full Cloud ETL Pipeline in local mode.

Usage:
    python run_pipeline.py

Stages:
    1. Extract  — Generate synthetic financial transaction data
    2. Transform — Clean, standardize, and enrich transactions
    3. Load     — Load into local SQLite warehouse
    4. Report   — Generate business summary reports
"""

import sys
import time
from src.extractor.extractor     import run_extraction
from src.transformer.transformer import run_transformation
from src.loader.loader           import run_loading
from src.reporter.reporter       import run_reporting


def run_pipeline():
    print("\n" + "=" * 60)
    print("CLOUD ETL PIPELINE — Financial Transaction Reporting")
    print("Running in: LOCAL MODE (SQLite warehouse)")
    print("=" * 60 + "\n")

    start = time.time()

    # Stage 1 — Extract
    print("Stage 1/4 — Extracting transaction data...")
    raw_df = run_extraction()
    print(f"  Raw records     : {len(raw_df):,}\n")

    # Stage 2 — Transform
    print("Stage 2/4 — Transforming and cleaning...")
    clean_df = run_transformation()
    print(f"  Clean records   : {len(clean_df):,}\n")

    # Stage 3 — Load
    print("Stage 3/4 — Loading to warehouse...")
    total_in_warehouse = run_loading()
    print(f"  Records in warehouse : {total_in_warehouse:,}\n")

    # Stage 4 — Report
    print("Stage 4/4 — Generating business reports...")
    reports = run_reporting()

    elapsed = round(time.time() - start, 2)

    print(f"\nPipeline complete in {elapsed}s")
    print(f"Warehouse        : data/warehouse/warehouse.db")
    print(f"Reports          : reports/")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_pipeline()
