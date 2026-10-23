
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET_NAME = "inunity-log-analytics-dev"
S3_PREFIX = "raw/"
AWS_REGION = "ap-south-1"


def extract_logs():
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3 = s3_hook.get_conn()

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX
    )

    if "Contents" not in response:
        raise FileNotFoundError(
            f"No files found in s3://{BUCKET_NAME}/{S3_PREFIX}"
        )

    records = []

    for obj in response["Contents"]:
        key = obj["Key"]

        if key.endswith("/"):
            continue

        print(f"Reading: {key}")

        file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)

        for line in file_obj["Body"].iter_lines():
            if not line:
                continue

            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line in {key}")

    if not records:
        raise ValueError("No valid log records were found.")

    print(f"Total records extracted: {len(records)}")
    return records


if __name__ == "__main__":
    logs = extract_logs()
    print("First record:", logs[0])
