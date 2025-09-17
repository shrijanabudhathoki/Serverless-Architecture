import boto3
import csv
import json
import os
import time
import uuid
from datetime import datetime
from boto3.dynamodb.types import TypeSerializer
from decimal import Decimal

# -------- CONFIG --------
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")
eventbridge = boto3.client("events")
serializer = TypeSerializer()

BUCKET_NAME      = os.environ.get("BUCKET_NAME")
ANALYSIS_PREFIX  = os.environ.get("ANALYSIS_PREFIX", "analyzed/")
MARKERS_PREFIX   = os.environ.get("MARKERS_PREFIX", "markers/")
DDB_TABLE        = os.environ.get("DDB_TABLE", "health_analysis")
EVENT_BUS_NAME   = os.environ.get("EVENT_BUS_NAME")

# -------- Utilities --------
def log(level, message, **kwargs):
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message
    }
    payload.update(kwargs)
    def decimal_default(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError
    print(json.dumps(payload, default=decimal_default))

def marker_key_from_hash(file_hash):
    return f"{MARKERS_PREFIX}{file_hash}.analysis.done"

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

def detect_anomalies(rows):
    anomalies = []
    for row in rows:
        anomaly_reasons = []
        hr = int(row["heart_rate"])
        spo2 = int(row["spo2"])
        temp = float(row["temp_c"])
        sys = int(row["systolic_bp"])
        dia = int(row["diastolic_bp"])

        if hr < 60 or hr > 160:
            anomaly_reasons.append("Abnormal heart rate")
        if spo2 < 92:
            anomaly_reasons.append("Low SpO2")
        if temp > 38.0:
            anomaly_reasons.append("High temperature")
        if sys > 140 or dia > 90:
            anomaly_reasons.append("High blood pressure")

        if anomaly_reasons:
            anomalies.append({
                "event_time": row["event_time"],
                "user_id": row["user_id"],
                "heart_rate": hr,
                "spo2": spo2,
                "steps": int(row["steps"]),
                "temp_c": temp,
                "systolic_bp": sys,
                "diastolic_bp": dia,
                "anomaly": ", ".join(anomaly_reasons)
            })
    return anomalies

def calculate_statistics(rows):
    if not rows:
        return {}
    heart_rates = [int(r["heart_rate"]) for r in rows]
    spo2_values = [int(r["spo2"]) for r in rows]
    temps = [float(r["temp_c"]) for r in rows]
    sys_bp = [int(r["systolic_bp"]) for r in rows]
    dia_bp = [int(r["diastolic_bp"]) for r in rows]
    steps = [int(r["steps"]) for r in rows]
    return {
        "avg_heart_rate": round(sum(heart_rates)/len(heart_rates), 1),
        "avg_spo2": round(sum(spo2_values)/len(spo2_values), 1),
        "avg_temp": round(sum(temps)/len(temps), 1),
        "avg_systolic": round(sum(sys_bp)/len(sys_bp), 1),
        "avg_diastolic": round(sum(dia_bp)/len(dia_bp), 1),
        "avg_steps": round(sum(steps)/len(steps), 0),
        "max_heart_rate": max(heart_rates),
        "min_heart_rate": min(heart_rates),
        "max_temp": max(temps),
        "min_spo2": min(spo2_values),
    }

def analyze_with_llm(rows, anomalies):
    stats = calculate_statistics(rows)
    sample_text = json.dumps(rows[:20])
    anomaly_summary = f"{len(anomalies)} anomalies detected in {len(rows)} records."
    stats_summary = f"Statistics: Avg HR {stats.get('avg_heart_rate',0)} bpm, Avg SpO2 {stats.get('avg_spo2',0)}%, Avg Temp {stats.get('avg_temp',0)}°C"

    prompt = (
        "You are a health data analysis assistant.\n\n"
        f"Dataset: {len(rows)} records\n"
        f"- {anomaly_summary}\n"
        f"- {stats_summary}\n\n"
        f"Sample:\n{sample_text}\n\n"
        "Provide JSON with: insights[], recommendations[], summary"
    )

    response = bedrock.invoke_model(
        modelId=os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0"),
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"maxTokens": 800, "temperature": 0.3}
        })
    )
    raw = response["body"].read()
    payload = json.loads(raw)
    output_text = payload["output"]["message"]["content"][0]["text"]
    if output_text.startswith("```json"):
        output_text = output_text.replace("```json", "").replace("```", "").strip()
    return json.loads(output_text)

def save_to_dynamodb(item):
    table = dynamodb.Table(DDB_TABLE)
    table.put_item(Item=item)

def send_event(event_type, detail, corr_id):
    eventbridge.put_events(
        Entries=[{
            "Source": "health.data.analyzer",
            "DetailType": event_type,
            "Detail": json.dumps(detail),
            "EventBusName": EVENT_BUS_NAME
        }]
    )
    log("INFO", "event_sent", correlation_id=corr_id, event_type=event_type)

# -------- Lambda Handler --------
def lambda_handler(event, context):
    """
    Expects EventBridge event from ingestor:
    {
      "correlation_id": "...",
      "bucket": "...",
      "processed_key": "...",
      "manifest_key": "...",
      "file_hash": "...",
      "counts": {...},
      "status": "success"
    }
    """
    if not event.get("processed_key") or not event.get("file_hash"):
        log("ERROR", "invalid_event_format", event=event)
        return {"status": "failed", "reason": "invalid_event"}

    corr_id = event["correlation_id"]
    processed_key = event["processed_key"]
    file_hash = event["file_hash"]

    # Idempotency by hash
    mkey = marker_key_from_hash(file_hash)
    if object_exists(BUCKET_NAME, mkey):
        log("INFO", "already_analyzed", correlation_id=corr_id, marker=mkey)
        return {"status": "skipped", "reason": "already_analyzed"}

    try:
        # Load CSV
        rows, _ = csv_to_dicts(BUCKET_NAME, processed_key)
        anomalies = detect_anomalies(rows)
        llm_result = analyze_with_llm(rows, anomalies)

        # Prepare analysis
        analysis = {
            "correlation_id": corr_id,
            "analysis_id": f"analysis_{int(time.time())}_{uuid.uuid4().hex[:8]}",
            "analysis_timestamp": datetime.utcnow().isoformat() + "Z",
            "source_file": processed_key,
            "records_analyzed": len(rows),
            "anomalies": anomalies,
            "insights": llm_result.get("insights", []),
            "recommendations": llm_result.get("recommendations", []),
            "summary": llm_result.get("summary", "Analysis completed."),
            "ttl": int(time.time()) + 7*24*3600
        }

        # Save to DynamoDB
        save_to_dynamodb(analysis)

        # Save to S3
        analysis_key = f"{ANALYSIS_PREFIX}{os.path.basename(processed_key).replace('.csv','')}_analysis.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=analysis_key,
            Body=json.dumps(analysis, indent=2).encode("utf-8"),
            ContentType="application/json"
        )

        # Write marker
        s3.put_object(Bucket=BUCKET_NAME, Key=mkey, Body=b"")

        # Send event
        send_event("Data Analysis Complete", {
            "correlation_id": corr_id,
            "bucket": BUCKET_NAME,
            "analysis_key": analysis_key,
            "row_count": len(rows),
            "anomaly_count": len(anomalies),
            "status": "success",
            "summary": analysis["summary"]
        }, corr_id)

        log("INFO", "analysis_completed", correlation_id=corr_id,
            rows_analyzed=len(rows), anomalies=len(anomalies))
        return {"status": "success", "analysis_key": analysis_key}

    except Exception as e:
        log("ERROR", "analysis_failed", correlation_id=corr_id, error=str(e))
        send_event("Data Analysis Failed", {
            "correlation_id": corr_id,
            "bucket": BUCKET_NAME,
            "source_key": processed_key,
            "status": "failed",
            "error": str(e)
        }, corr_id)
        return {"status": "failed", "error": str(e)}
