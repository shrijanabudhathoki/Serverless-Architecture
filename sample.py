import boto3
import csv
import io
import random
from datetime import datetime, timedelta
import uuid
import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "health-data-bucket-shrijana")  # replace or export env var
RAW_PREFIX  = "raw/"
REGION      = os.getenv("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)

# Generate synthetic health data for N users over T minutes
def generate_health_data(num_users=5, num_records=50):
    data = []
    start_time = datetime.utcnow()

    for u in range(1, num_users + 1):
        user_id = f"user_{u}"
        for i in range(num_records):
            t = start_time + timedelta(minutes=i)
            record = {
                "event_time": t.isoformat() + "Z",
                "user_id": user_id,
                "heart_rate": random.randint(55, 180),
                "spo2": random.randint(90, 100),
                "steps": random.randint(0, 5000),
                "temp_c": round(random.uniform(35.5, 38.5), 1),
                "systolic_bp": random.randint(100, 140),
                "diastolic_bp": random.randint(60, 95),
            }

            # Inject some anomalies (invalid data)
            if random.random() < 0.1:
                record["heart_rate"] = 300  # out-of-range
            if random.random() < 0.05:
                record["spo2"] = 40  # too low

            data.append(record)
    return data

def to_csv(data):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "event_time","user_id","heart_rate","spo2",
        "steps","temp_c","systolic_bp","diastolic_bp"
    ])
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    return buf.getvalue()

def upload_to_s3(data_csv):
    filename = f"health_{uuid.uuid4().hex}.csv"
    key = RAW_PREFIX + filename

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=data_csv.encode("utf-8"),
        ContentType="text/csv"
    )
    print(f"Uploaded {filename} to s3://{BUCKET_NAME}/{key}")
    return key

if __name__ == "__main__":
    dataset = generate_health_data(num_users=3, num_records=20)
    csv_data = to_csv(dataset)
    upload_to_s3(csv_data)
