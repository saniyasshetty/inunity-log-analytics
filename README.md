# InUnity Log Analytics

An automated log analytics pipeline that extracts application logs from AWS S3, validates and cleans records, analyzes request patterns, generates a PDF report, and uploads the report back to S3 using Apache Airflow.

## Project Overview

Application logs contain useful information about API usage, request patterns, and application traffic. This project automates the processing of raw JSON Lines (NDJSON) logs and produces a report that helps understand application activity.

## Key Features

- Extract raw log records from AWS S3.
- Validate required fields and timestamp formats.
- Remove duplicate records.
- Hash IP addresses to reduce exposure of raw IP values in processed data.
- Analyze HTTP methods, frequently accessed paths, and hourly request traffic.
- Generate a PDF analytics report.
- Upload the generated PDF to AWS S3.
- Orchestrate the pipeline using Apache Airflow.

## Architecture

```text
Raw Application Logs
        |
        v
AWS S3 - raw/
        |
        v
Apache Airflow DAG
        |
        v
Extract Logs
        |
        v
Transform and Validate
        |
        v
Analyze Request Patterns
        |
        v
Generate PDF Report
        |
        v
Upload Report to AWS S3
        |
        v
S3 - reports/
```

## Technology Stack

- **Language:** Python
- **Workflow orchestration:** Apache Airflow
- **Containerization:** Docker Compose
- **Cloud storage:** Amazon S3
- **AWS integration:** Airflow Amazon provider / S3Hook
- **PDF generation:** ReportLab
- **Analytics:** Python collections and datetime modules

## Project Structure

```text
inunity-log-analytics/
├── airflow-docker/
│   ├── dags/
│   │   └── inunity_log_analytics_dag.py
│   ├── logs/
│   ├── plugins/
│   ├── config/
│   └── docker-compose.yaml
├── pyspark/
│   ├── extract.py
│   ├── transform.py
│   └── analytics.py
├── README.md
└── .gitignore
```

## Pipeline Workflow

### 1. Extraction
Reads raw NDJSON log files from the `raw/` prefix in the configured S3 bucket.

### 2. Transformation
Checks required fields, validates timestamps, removes duplicate records, and hashes available IP address values.

### 3. Analytics
Calculates:
- Total requests
- Request counts by HTTP method
- Top 10 request paths
- Hourly request traffic

### 4. PDF Report Generation
Creates a PDF containing the analytics results.

### 5. S3 Report Upload
Uploads the PDF to the `reports/` prefix in the configured S3 bucket.

## Prerequisites

- Python
- Docker Desktop
- An AWS account and S3 bucket
- AWS credentials with appropriate S3 permissions
- Apache Airflow Amazon provider
- ReportLab

## Configuration

Configure AWS access through the Airflow connection named `aws_default`.

The DAG uses the configured connection to access S3. Do not hardcode AWS access keys or secrets in the source code.

Configure the bucket and prefixes for your own environment before running the project.

## Running the Project

1. Configure Docker Desktop and the Airflow environment.
2. Configure the `aws_default` connection in Airflow.
3. Start the Airflow services from the `airflow-docker` directory:

   ```bash
   docker compose up -d
   ```

4. Open the Airflow interface at `http://localhost:8081`.
5. Find and trigger the `inunity_log_analytics` DAG.
6. Monitor the task status until the pipeline completes.
7. Open the configured S3 bucket and navigate to `reports/` to retrieve the generated PDF.

## Output

The expected report location is:

```text
s3://<your-bucket-name>/reports/inunity_log_analytics_report.pdf
```

The report contains request totals, HTTP method counts, top paths, and hourly traffic.

## Security Notes

- Never commit AWS credentials, passwords, private keys, or populated `.env` files.
- Grant only the S3 permissions required by the pipeline.
- Hashing IP addresses reduces direct exposure but does not guarantee complete anonymization.
- Review logs and generated outputs before publishing them.

## Current Status

The five Airflow tasks have completed successfully in the development environment:

1. `extract_logs`
2. `transform_logs`
3. `analytics`
4. `generate_pdf`
5. `upload_pdf_to_s3`

The generated PDF should be verified in S3 before publishing a release or claiming the final output has been reviewed.
