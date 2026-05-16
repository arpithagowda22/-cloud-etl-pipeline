# Cloud ETL Pipeline for Financial Transaction Reporting

A hybrid cloud ETL pipeline that ingests, cleans, transforms, and aggregates financial transaction data for executive-level business reporting. The pipeline runs fully locally in demo mode and includes production-ready AWS Glue and Redshift code showing how it would operate at scale in a cloud environment.

---

## Background

Financial reporting teams need clean, aggregated transaction data delivered on a daily schedule. Raw transaction data arrives from multiple source systems with inconsistent formats, duplicates, and missing values. This pipeline handles the full journey from raw ingestion through to formatted business reports — the same pattern used in production financial data platforms.

This project demonstrates two modes:

- **Local mode** — runs entirely on your machine using SQLite as the warehouse. No accounts or configuration needed.
- **AWS mode** — production code using AWS Glue for transformation and Amazon Redshift as the data warehouse, showing how the pipeline scales in a real cloud environment.

---

## Architecture

```
Local Mode:
Raw CSV --> Extractor --> Staging --> Transformer --> SQLite Warehouse --> Reporter --> Reports

AWS Mode:
S3 (Raw) --> AWS Glue Job --> S3 (Processed) --> Redshift --> Reports
```

---

## Tech Stack

Local / Demo: Python 3.8+, Pandas, SQLite, NumPy

AWS / Production: AWS S3, AWS Glue, Amazon Redshift, AWS QuickSight, Boto3

---

## Project Structure

```
cloud_etl_pipeline/
├── src/
│   ├── extractor/
│   │   └── extractor.py            # Ingests and validates raw transaction data
│   ├── transformer/
│   │   └── transformer.py          # Cleans, standardizes, and enriches data
│   ├── loader/
│   │   └── loader.py               # Loads into SQLite (local) or Redshift (AWS)
│   ├── reporter/
│   │   └── reporter.py             # Generates business summary reports
│   └── aws/
│       ├── glue_job.py             # Production AWS Glue ETL script
│       └── redshift_loader.py      # Production Redshift loader
├── config/
│   └── config.py                   # Pipeline configuration
├── data/
│   ├── raw/                        # Raw transaction CSV files
│   ├── staging/                    # Intermediate cleaned data
│   └── warehouse/                  # Local SQLite warehouse
├── reports/                        # Generated business reports
├── tests/
│   └── test_pipeline.py
├── run_pipeline.py                 # Single entry point
├── requirements.txt
└── README.md
```

---

## Quickstart (Local Mode)

### 1. Clone the repo

```bash
git clone https://github.com/arpithagowda22/cloud-etl-pipeline.git
cd cloud-etl-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the pipeline

```bash
python run_pipeline.py
```

No API keys, no AWS account, no configuration needed.

---

## AWS Production Setup

To run in production mode with AWS:

| Requirement | Details |
|---|---|
| AWS Account | aws.amazon.com |
| S3 Bucket | Stores raw and processed data |
| AWS Glue | Serverless ETL — pay per job run |
| Amazon Redshift | Data warehouse |
| IAM Role | Glue execution role with S3 and Redshift access |

See src/aws/glue_job.py for the production Glue script and src/aws/redshift_loader.py for the Redshift loader.

---

## Pipeline Stages

### Stage 1 - Extract
Ingests raw transaction CSV files, validates schema, checks required columns, and logs ingestion statistics. In AWS mode, reads directly from S3.

### Stage 2 - Transform
- Removes duplicate transactions by transaction ID
- Standardizes date formats and currency codes
- Handles missing merchant names and categories
- Classifies transactions into business categories
- Flags large transactions above configurable threshold
- Computes net amount after fees

### Stage 3 - Load
Loads cleaned data into SQLite (local) or Redshift (production) using upsert logic to prevent duplicate loads.

### Stage 4 - Report
Generates three business reports saved to reports/:
- Daily Transaction Summary — total volume, count, average by category
- Merchant Performance Report — top merchants by revenue
- Risk Flagging Report — large and suspicious transactions

---

## Sample Output

```
Pipeline complete.

Records ingested        : 50,000
After deduplication     : 49,103
After validation        : 48,891

Total volume            : $24,731,450.00
Total transactions      : 48,891
Average transaction     : $505.83
Flagged (>$50,000)      : 312

Reports saved to: reports/
```

---

## AWS vs Local Comparison

| Feature | Local Mode | AWS Mode |
|---|---|---|
| Storage | CSV / SQLite | S3 + Redshift |
| ETL Engine | Pandas | AWS Glue (PySpark) |
| Scale | ~1M rows | Billions of rows |
| Cost | Free | Pay per use |
| Scheduling | Manual | Airflow / EventBridge |
| Monitoring | Console logs | CloudWatch |

---

## Running Tests

```bash
pytest tests/test_pipeline.py -v
```

---

## Author

Arpitha Raghu - Data Engineer
LinkedIn: https://www.linkedin.com/in/arpitha2205/
GitHub: https://github.com/arpithagowda22
Email: arpithagowda2205@gmail.com
