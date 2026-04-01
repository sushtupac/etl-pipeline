# ETL Data Pipeline — AWS S3 + Python

A production-pattern ETL pipeline that ingests raw CSV sales data,
transforms and cleans it, stores results in a partitioned S3 data lake,
and generates a business summary report.

## Pipeline Flow
```
S3 (raw CSV) → Python ETL → S3 (partitioned JSON) → S3 (report)
```

## What It Does

### Extract
- Reads raw CSV from S3 (`sales-raw-data` bucket)
- Handles encoding and parsing automatically

### Transform
- Normalises customer names to title case
- Standardises status values to lowercase
- Calculates total order value per record
- Parses and validates all date fields
- Enriches records with year/month/day fields for partitioning

### Load
- Writes clean records as newline-delimited JSON
- Partitions data by date: `year=YYYY/month=MM/day=DD/`
- Mirrors the Hive partitioning scheme used by AWS Glue and Athena

### Report
- Total orders and revenue
- Revenue breakdown by product
- Order count by status
- Top customer by spend

## Sample Output
```
EXTRACT — reading raw CSV from S3...
  Read 15 records from s3://sales-raw-data/incoming/sales_2024_01.csv

TRANSFORM — cleaning and enriching data...
  Cleaned 15 records, skipped 0

LOAD — writing processed data to S3...
  Uploaded 2 records to s3://sales-processed-data/sales/year=2024/month=01/day=15/data.json
  Created 8 partitions

REPORT — generating pipeline summary...
  Total orders:    15
  Total revenue:   $7,599.61
  Avg order value: $506.64

  Revenue by product:
    Laptop       $4,999.95
    Monitor      $1,399.96
    Keyboard     $719.91
```

## AWS Services Used

| Service | Purpose |
|---------|---------|
| S3 | Raw data ingestion, processed data lake, report storage |
| IAM | Least-privilege access between pipeline and buckets |

## S3 Bucket Structure
```
sales-raw-data/
└── incoming/
    └── sales_2024_01.csv

sales-processed-data/
└── sales/
    └── year=2024/
        └── month=01/
            └── day=15/
                └── data.json

sales-reports/
└── pipeline_report_20260401_074852.json
```

## Local Development
```bash
# Start LocalStack
$env:LOCALSTACK_ACKNOWLEDGE_ACCOUNT_REQUIREMENT=1; localstack start

# Create buckets and upload raw data
python setup_buckets.py

# Run the pipeline
python etl.py
```

## Project Structure
```
├── raw_data.csv        # Sample raw sales data (messy, real-world format)
├── setup_buckets.py    # Creates S3 buckets and uploads raw data
├── etl.py              # Main pipeline: extract, transform, load, report
└── README.md
```

## What I Learned

- How to read and write files between Python and S3 using boto3
- Why data lakes use Hive-style date partitioning (year/month/day)
- How partitioning makes downstream queries faster and cheaper in Athena
- The ETL pattern: always separate extract, transform, and load concerns
- How AWS Glue automates this exact pipeline at enterprise scale