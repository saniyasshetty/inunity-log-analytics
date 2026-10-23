
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark.sql import SparkSession

BUCKET_NAME = "inunity-log-analytics-dev"
S3_PREFIX = "raw/"
AWS_REGION = "ap-south-1"


def extract_logs(spark: SparkSession):
    """
    Read JSON log records from S3 into a PySpark DataFrame.
    Uses Airflow's AWS connection to retrieve the records.
    """

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3 = s3_hook.get_conn()

    paginator = s3.get_paginator("list_objects_v2")
    records = []

    for page in paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/"):
                continue

            print(f"Reading: {key}")

            file_obj = s3.get_object(
                Bucket=BUCKET_NAME,
                Key=key
            )

            for line in file_obj["Body"].iter_lines():
                if not line:
                    continue

                try:
                    record = json.loads(line.decode("utf-8"))
                    if isinstance(record, dict):
                        records.append(record)
                    else:
                        print(f"Skipping non-object JSON in {key}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    print(f"Skipping invalid JSON line in {key}")

    if not records:
        raise ValueError(
            f"No valid log records found in s3://{BUCKET_NAME}/{S3_PREFIX}"
        )

    # Create a Spark DataFrame from the extracted records
    df = spark.createDataFrame(records)

    print(f"Total records extracted: {df.count()}")
    df.printSchema()

    return df


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("InUnityLogExtraction")
        .getOrCreate()
    )

    logs_df = extract_logs(spark)
    logs_df.show(5, truncate=False)

    spark.stop()
