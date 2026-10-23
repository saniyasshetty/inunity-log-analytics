
from datetime import datetime
import os
import sys

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/airflow/pyspark")

from extract import extract_logs
from transform import transform_logs
from analytics import (
    analyze_logs,
    generate_pdf_report,
    upload_pdf_to_s3,
)

DATA_DIR = "/opt/airflow/data"
RAW_PARQUET_PATH = os.path.join(DATA_DIR, "raw_logs")
CLEAN_PARQUET_PATH = os.path.join(DATA_DIR, "cleaned_logs")
PDF_PATH = "/opt/airflow/reports/inunity_log_analytics_report.pdf"


def create_spark_session(app_name):
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .getOrCreate()
    )


def run_extract():
    spark = create_spark_session("InUnityExtract")

    try:
        os.makedirs(DATA_DIR, exist_ok=True)

        df = extract_logs(spark)

        df.write.mode("overwrite").parquet(RAW_PARQUET_PATH)

        print(f"Raw Parquet data saved to {RAW_PARQUET_PATH}")
        return RAW_PARQUET_PATH
    finally:
        spark.stop()


def run_transform(ti):
    raw_path = ti.xcom_pull(task_ids="extract_logs")
    if not raw_path:
        raise ValueError("Raw Parquet path was not received.")

    spark = create_spark_session("InUnityTransform")

    try:
        df = spark.read.parquet(raw_path)
        cleaned_df = transform_logs(df)

        cleaned_df.write.mode("overwrite").parquet(
            CLEAN_PARQUET_PATH
        )

        print(f"Cleaned Parquet data saved to {CLEAN_PARQUET_PATH}")
        return CLEAN_PARQUET_PATH
    finally:
        spark.stop()


def run_analytics(ti):
    cleaned_path = ti.xcom_pull(task_ids="transform_logs")
    if not cleaned_path:
        raise ValueError("Cleaned Parquet path was not received.")

    spark = create_spark_session("InUnityAnalytics")

    try:
        df = spark.read.parquet(cleaned_path)

        # The existing analytics module expects Python dictionaries.
        # Convert rows only at this reporting stage.
        records = [
            row.asDict(recursive=True)
            for row in df.toLocalIterator()
        ]

        results = analyze_logs(records)
        return results
    finally:
        spark.stop()


def run_generate_pdf(ti):
    results = ti.xcom_pull(task_ids="analytics")
    if not results:
        raise ValueError("Analytics results were not received.")

    generate_pdf_report(results, PDF_PATH)
    return PDF_PATH


def run_upload_pdf(ti):
    pdf_path = ti.xcom_pull(task_ids="generate_pdf")

    if not pdf_path or not os.path.exists(pdf_path):
        raise FileNotFoundError(
            f"PDF report not found: {pdf_path}"
        )

    upload_pdf_to_s3(pdf_path)


with DAG(
    dag_id="inunity_log_analytics",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["inunity", "data-engineering", "pyspark"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_logs",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_logs",
        python_callable=run_transform,
    )

    analytics_task = PythonOperator(
        task_id="analytics",
        python_callable=run_analytics,
    )

    generate_pdf_task = PythonOperator(
        task_id="generate_pdf",
        python_callable=run_generate_pdf,
    )

    upload_pdf_task = PythonOperator(
        task_id="upload_pdf_to_s3",
        python_callable=run_upload_pdf,
    )

    (
        extract_task
        >> transform_task
        >> analytics_task
        >> generate_pdf_task
        >> upload_pdf_task
    )
