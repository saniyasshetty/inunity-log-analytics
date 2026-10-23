
import hashlib
from datetime import datetime


def transform_logs(records):
    cleaned_records = []
    seen_records = set()

    for record in records:
        # Check required fields
        required_fields = [
            "timestamp",
            "request_id",
            "method",
            "path"
        ]

        if any(not record.get(field) for field in required_fields):
            continue

        # Validate timestamp
        try:
            datetime.fromisoformat(
                record["timestamp"].replace("Z", "+00:00")
            )
        except (ValueError, TypeError):
            continue

        # Remove duplicate records
        record_key = str(sorted(record.items()))

        if record_key in seen_records:
            continue

        seen_records.add(record_key)

        # Protect IP address by hashing it
        cleaned_record = record.copy()
        ip_address = cleaned_record.get("remote_addr")

        if ip_address:
            cleaned_record["remote_addr"] = hashlib.sha256(
                str(ip_address).encode()
            ).hexdigest()

        cleaned_records.append(cleaned_record)

    print(f"Records received: {len(records)}")
    print(f"Records after cleaning: {len(cleaned_records)}")

    return cleaned_records


if __name__ == "__main__":
    print("Transformation module is ready.")
