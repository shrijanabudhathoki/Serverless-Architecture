import boto3
import csv
import io
import json
import os
from datetime import datetime

# -------- AWS Clients --------
s3 = boto3.client("s3")

# -------- Environment Variables --------
BUCKET_NAME      = os.environ.get("BUCKET_NAME")
RAW_PREFIX       = os.environ.get("RAW_PREFIX", "raw/")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/")
REJECTED_PREFIX  = os.environ.get("REJECTED_PREFIX", "rejected/")
MARKERS_PREFIX   = os.environ.get("MARKERS_PREFIX", "markers/")

# -------- Validation Config --------
RANGES = {
    "heart_rate": (50, 180),
    "spo2": (90, 100),
    "steps": (0, 50000),
    "temp_c": (35.0, 40.0),
    "systolic_bp": (90, 180),
    "diastolic_bp": (60, 120),
}

REQUIRED_FIELDS = [
    "event_time", "user_id", "heart_rate", "spo2", "steps",
    "temp_c", "systolic_bp", "diastolic_bp"
]

# -------- Logging Utility --------
def log(level, message, **kwargs):
    payload = {"ts": datetime.utcnow().isoformat() + "Z", "level": level, "message": message}
    payload.update(kwargs)
    print(json.dumps(payload))

# -------- Helper Functions --------
def marker_key(bucket, key, version_id):
    safe_key = key.replace("/", "__")
    return f"{MARKERS_PREFIX}{bucket}__{safe_key}__{version_id}.done"

def object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

def parse_float(value):
    try:
        return float(value)
    except:
        return None

def parse_int(value):
    try:
        return int(float(value))
    except:
        return None

def validate_row(row):
    for f in REQUIRED_FIELDS:
        if not row.get(f):
            return False, f"missing_{f}"

    hr = parse_int(row["heart_rate"])
    sp = parse_int(row["spo2"])
    st = parse_int(row["steps"])
    temp = parse_float(row["temp_c"])
    sbp = parse_int(row["systolic_bp"])
    dbp = parse_int(row["diastolic_bp"])

    if hr is None or not RANGES["heart_rate"][0] <= hr <= RANGES["heart_rate"][1]:
        return False, "invalid_heart_rate"
    if sp is None or not RANGES["spo2"][0] <= sp <= RANGES["spo2"][1]:
        return False, "invalid_spo2"
    if st is None or not RANGES["steps"][0] <= st <= RANGES["steps"][1]:
        return False, "invalid_steps"
    if temp is None or not RANGES["temp_c"][0] <= temp <= RANGES["temp_c"][1]:
        return False, "invalid_temp_c"
    if sbp is None or not RANGES["systolic_bp"][0] <= sbp <= RANGES["systolic_bp"][1]:
        return False, "invalid_systolic_bp"
    if dbp is None or not RANGES["diastolic_bp"][0] <= dbp <= RANGES["diastolic_bp"][1]:
        return False, "invalid_diastolic_bp"
    if dbp > sbp:
        return False, "dbp_gt_sbp"

    return True, "ok"

def csv_to_string(rows, fieldnames):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")

# -------- Lambda Handler --------
def lambda_handler(event, context):
    """
    Handles S3 events and optional manual/EventBridge events.
    """
    # Determine if event is S3 notification or manual format
    if "Records" in event:
        records = event["Records"]
    elif "bucket" in event and "key" in event:
        # manual/eventbridge style test
        records = [{"s3": {"bucket": {"name": event["bucket"]}, 
                           "object": {"key": event["key"], "versionId": event.get("versionId", "null")}}}]
    else:
        log("ERROR", "invalid_event_format", event=event)
        return {"status": "failed", "reason": "invalid_event_format"}

    for rec in records:
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
        version_id = rec["s3"]["object"].get("versionId", "null")

        corr_id = f"{bucket}/{key}@{version_id}"
        log("INFO", "ingestion_started", correlation_id=corr_id)

        # Idempotency marker
        mkey = marker_key(bucket, key, version_id)
        if object_exists(BUCKET_NAME, mkey):
            log("INFO", "already_processed", correlation_id=corr_id, marker=mkey)
            continue

        if not key.startswith(RAW_PREFIX):
            log("INFO", "skip_non_raw_prefix", correlation_id=corr_id)
            continue

        try:
            # Read CSV from S3
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read().decode("utf-8").splitlines()
            reader = csv.DictReader(content)
            fieldnames = reader.fieldnames

            valid_rows, reject_rows = [], []
            for row in reader:
                valid, reason = validate_row(row)
                if valid:
                    valid_rows.append(row)
                else:
                    r = dict(row)
                    r["reject_reason"] = reason
                    reject_rows.append(r)

            base_name = os.path.basename(key)
            processed_key = f"{PROCESSED_PREFIX}{base_name}" if valid_rows else None
            rejected_key = f"{REJECTED_PREFIX}{base_name.replace('.csv','')}_rejected.csv" if reject_rows else None
            manifest_key = f"{PROCESSED_PREFIX}{base_name.replace('.csv','')}_manifest.json"

            if valid_rows:
                s3.put_object(Bucket=BUCKET_NAME, Key=processed_key,
                              Body=csv_to_string(valid_rows, fieldnames))
            if reject_rows:
                rej_fields = fieldnames + ["reject_reason"]
                s3.put_object(Bucket=BUCKET_NAME, Key=rejected_key,
                              Body=csv_to_string(reject_rows, rej_fields))

            manifest = {
                "correlation_id": corr_id,
                "source_bucket": bucket,
                "source_key": key,
                "source_version": version_id,
                "processed_key": processed_key,
                "rejected_key": rejected_key,
                "counts": {
                    "input": len(valid_rows) + len(reject_rows),
                    "valid": len(valid_rows),
                    "rejected": len(reject_rows)
                },
                "timestamp_utc": datetime.utcnow().isoformat() + "Z",
                "schema_fields": REQUIRED_FIELDS
            }
            s3.put_object(Bucket=BUCKET_NAME, Key=manifest_key,
                          Body=json.dumps(manifest, indent=2).encode("utf-8"),
                          ContentType="application/json")

            # Write marker for idempotency
            s3.put_object(Bucket=BUCKET_NAME, Key=mkey, Body=b"")

            log("INFO", "ingestion_completed", correlation_id=corr_id,
                processed_key=processed_key, rejected_key=rejected_key, manifest_key=manifest_key)

        except Exception as e:
            log("ERROR", "ingestion_failed", correlation_id=corr_id, error=str(e))
            continue

    return {"status": "ok"}
