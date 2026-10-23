
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_logs(df: DataFrame) -> DataFrame:
    """Validate logs, remove duplicates, and hash client addresses."""

    initial_count = df.count()

    # 1. Keep records with all required fields.
    required_fields = ["timestamp", "request_id", "method", "path"]

    for field in required_fields:
        df = df.filter(
            F.col(field).isNotNull()
            & (F.trim(F.col(field).cast("string")) != "")
        )

    # 2. Parse timestamps safely.
    # Convert UTC suffix Z into an explicit UTC offset.
    timestamp_text = F.regexp_replace(
        F.col("timestamp").cast("string"),
        "Z$",
        "+00:00"
    )

    timestamp_format = F.lit(
        "yyyy-MM-dd'T'HH:mm:ss.SSSXX"
    )

    df = df.withColumn(
        "_parsed_timestamp",
        F.try_to_timestamp(timestamp_text, timestamp_format)
    )

    # Remove records with invalid timestamps.
    df = df.filter(F.col("_parsed_timestamp").isNotNull())

    # 3. Remove exact duplicate records.
    df = df.dropDuplicates()

    # 4. Hash client addresses for privacy.
    if "remote_addr" in df.columns:
        df = df.withColumn(
            "remote_addr",
            F.when(
                F.col("remote_addr").isNotNull()
                & (F.trim(F.col("remote_addr").cast("string")) != ""),
                F.sha2(F.col("remote_addr").cast("string"), 256)
            ).otherwise(F.col("remote_addr"))
        )

    df = df.drop("_parsed_timestamp")

    print(f"Records received: {initial_count}")
    print("Transformation applied using PySpark DataFrame operations.")

    return df


if __name__ == "__main__":
    print("PySpark transformation module is ready.")
