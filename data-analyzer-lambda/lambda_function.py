import boto3
import csv
import io
import json
import os
from datetime import datetime

# -------- CONFIG --------
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")  # AWS Bedrock runtime client

BUCKET_NAME      = os.environ.get("BUCKET_NAME")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/")
ANALYSIS_PREFIX  = os.environ.get("ANALYSIS_PREFIX", "analyzed/")
MARKERS_PREFIX   = os.environ.get("MARKERS_PREFIX", "markers/")
DDB_TABLE        = os.environ.get("DDB_TABLE", "health_analysis")

# -------- Logging Utility --------
def log(level, message, **kwargs):
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message
    }
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

def csv_to_dicts(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8").splitlines()
    reader = csv.DictReader(content)
    return list(reader), reader.fieldnames

def analyze_with_llm(data_rows):
    rows_text = json.dumps(data_rows[:20])

    prompt = (
        "You are a health data analysis assistant.\n"
        "Analyze the following patient health records, detect anomalies, "
        "and summarize trends.\n\n"
        f"{rows_text}\n\n"
        "Return JSON with keys: anomalies, summary, and recommendations."
    )

    response = bedrock.invoke_model(
        modelId=os.environ.get("BEDROCK_MODEL_ID"),
        contentType="application/json",
        accept="application/json",
        body=json.dumps({   
            "messages": [
                {"role": "user", "content": [{"text": prompt}]}
            ],
            "inferenceConfig": {
                "maxTokens": 500,
                "temperature": 0.5
            }
        })
    )

    raw = response["body"].read()
    payload = json.loads(raw)

    output_text = payload["output"]["message"]["content"][0]["text"]

    try:
        result = json.loads(output_text)
    except json.JSONDecodeError:
        result = {"summary": output_text, "anomalies": [], "recommendations": []}

    return result



def save_to_dynamodb(item):
    table = dynamodb.Table(DDB_TABLE)
    table.put_item(Item=item)

# -------- Lambda Handler --------
def lambda_handler(event, context):
    if "Records" in event:
        records = event["Records"]
    else:
        records = [{"s3": {"bucket": {"name": event["bucket"]}, "object": {"key": event["key"], "versionId": event.get("versionId", "null")}}}]

    for rec in records:
        bucket = rec["s3"]["bucket"]["name"]
        key    = rec["s3"]["object"]["key"]
        version_id = rec["s3"]["object"].get("versionId", "null")

        corr_id = f"{bucket}/{key}@{version_id}"
        log("INFO", "analysis_started", correlation_id=corr_id)

        # Idempotency: skip if marker exists
        mkey = marker_key(bucket, key, version_id)
        if object_exists(BUCKET_NAME, mkey):
            log("INFO", "already_analyzed", correlation_id=corr_id, marker=mkey)
            continue

        # Only process processed/ prefix
        if not key.startswith(PROCESSED_PREFIX):
            log("INFO", "skip_non_processed_prefix", correlation_id=corr_id)
            continue

        try:
            # Read processed CSV
            rows, fieldnames = csv_to_dicts(bucket, key)

            # Analyze with LLM
            llm_result = analyze_with_llm(rows)

            # Upload analysis results to S3
            analysis_key = f"{ANALYSIS_PREFIX}{os.path.basename(key).replace('.csv','')}_analysis.json"
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=analysis_key,
                Body=json.dumps(llm_result, indent=2).encode("utf-8"),
                ContentType="application/json"
            )

            # Store facts/aggregates in DynamoDB
            save_to_dynamodb({
                "correlation_id": corr_id,
                "source_bucket": bucket,
                "source_key": key,
                "analysis_s3_key": analysis_key,
                "timestamp_utc": datetime.utcnow().isoformat() + "Z",
                "analysis": llm_result
            })

            # Write idempotency marker
            s3.put_object(Bucket=BUCKET_NAME, Key=mkey, Body=b"")

            log("INFO", "analysis_completed",
                correlation_id=corr_id,
                analysis_key=analysis_key,
                rows_analyzed=len(rows),
                dynamodb_table=DDB_TABLE)

        except Exception as e:
            log("ERROR", "analysis_failed", correlation_id=corr_id, error=str(e))

    return {"status": "ok"}
