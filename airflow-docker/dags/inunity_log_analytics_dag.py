
from datetime import datetime
import sys
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/pyspark")

from extract import extract_logs
from transform import transform_logs
from analytics import (
    analyze_logs,
    generate_pdf_report,
    upload_pdf_to_s3,
)

PDF_PATH = "/opt/airflow/reports/inunity_log_analytics_report.pdf"


def run_extract():
    return extract_logs()


def run_transform(ti):
    records = ti.xcom_pull(task_ids="extract_logs")
    return transform_logs(records)


def run_analytics(ti):
    records = ti.xcom_pull(task_ids="transform_logs")
    results = analyze_logs(records)
    return results


def run_generate_pdf(ti):
    results = ti.xcom_pull(task_ids="analytics")
    generate_pdf_report(results, PDF_PATH)
    return PDF_PATH


def run_upload_pdf(ti):
    pdf_path = ti.xcom_pull(task_ids="generate_pdf")
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF report not found: {pdf_path}")

    upload_pdf_to_s3(pdf_path)


with DAG(
    dag_id="inunity_log_analytics",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["inunity", "data-engineering"],
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
