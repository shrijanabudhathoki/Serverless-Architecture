# Data analyzer lambda function

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
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/")
ANALYSIS_PREFIX  = os.environ.get("ANALYSIS_PREFIX", "analyzed/")
MARKERS_PREFIX   = os.environ.get("MARKERS_PREFIX", "markers/")
DDB_TABLE        = os.environ.get("DDB_TABLE", "health_analysis")
EVENT_BUS_NAME   = os.environ.get("EVENT_BUS_NAME")\

def convert_floats(obj):
    if isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))  # Safe conversion
    else:
        return obj


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


def analyze_with_llm(rows, anomalies):
    # Send only a sample + aggregated stats to Bedrock
    sample_text = json.dumps(rows[:20])
    anomaly_summary = f"{len(anomalies)} anomalies detected in {len(rows)} records."

    prompt = (
        "You are a health data analysis assistant. "
        "You receive health metrics for multiple users over time, including heart rate, SpO2, temperature, blood pressure, and steps. "
        "Your task is to analyze this dataset and provide a structured report.\n\n"

        "1. Identify overall trends and patterns in the dataset (e.g., average heart rate, typical activity levels, common anomalies).\n"
        "2. Summarize any significant anomalies and what they might indicate about health risks.\n"
        "3. Provide actionable recommendations for users or healthcare providers based on the data.\n"
        "4. Generate an executive summary suitable for reporting to non-technical stakeholders.\n"
        "5. Return the analysis in JSON format with the following keys:\n"
        "   - insights: List of key observations from the dataset.\n"
        "   - recommendations: List of suggested actions or interventions.\n"
        "   - summary: Concise executive summary of the findings.\n\n"

        f"Sample health records (first 20 rows):\n{sample_text}\n\n"
        f"Anomaly summary:\n{anomaly_summary}\n\n"
        "Provide your output in valid JSON format only."
    )

    response = bedrock.invoke_model(
        modelId=os.environ.get("BEDROCK_MODEL_ID"),
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "messages": [
                {"role": "user", "content": [{"text": prompt}]}
            ],
            "inferenceConfig": {"maxTokens": 500, "temperature": 0.5}
        })
    )
    raw = response["body"].read()
    payload = json.loads(raw)
    output_text = payload["output"]["message"]["content"][0]["text"]

    try:
        result = json.loads(output_text)
    except json.JSONDecodeError:
        result = {"summary": output_text}

    return {
        "insights": result.get("insights", ["No major trends observed"]),
        "recommendations": result.get("recommendations", ["No recommendations"]),
        "summary": result.get("summary", "Analysis completed.")
    }


def save_to_dynamodb(item):
    table = dynamodb.Table(DDB_TABLE)
    table.put_item(Item=item)

def send_event_to_eventbridge(event_type, detail, correlation_id):
    try:
        response = eventbridge.put_events(
            Entries=[
                {
                    'Source': 'health.data.analyzer',
                    'DetailType': event_type,
                    'Detail': json.dumps(detail),
                    'EventBusName': EVENT_BUS_NAME
                }
            ]
        )
        log("INFO", "event_sent_to_eventbridge", 
            correlation_id=correlation_id, 
            event_type=event_type,
            event_id=response['Entries'][0].get('EventId'))
    except Exception as e:
        log("ERROR", "failed_to_send_event", correlation_id=correlation_id, error=str(e))

# -------- Serialize DynamoDB item --------
def serialize_ddb_item(anomalies, llm_result, correlation_id, key, rows):
    summary_text = llm_result.get("summary", "Analysis completed.")

    # Fallback: parse insights/recommendations from summary if missing
    insights_list = llm_result.get("insights")
    if not insights_list or insights_list == ["No major trends observed"]:
        insights_list = extract_insights_from_summary(summary_text)

    recommendations_list = llm_result.get("recommendations")
    if not recommendations_list or recommendations_list == ["No recommendations"]:
        recommendations_list = extract_recommendations_from_summary(summary_text)

    item = {
        "correlation_id": correlation_id,
        "analysis_id": f"analysis_{int(time.time())}_{uuid.uuid4().hex[:8]}",
        "analysis_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_file": key,
        "processed_file": key.replace("raw/", "processed/"),
        "records_analyzed": len(rows),
        "anomalies": anomalies,
        "insights": llm_result.get("insights", []),
        "recommendations": llm_result.get("recommendations", []),
        "summary": llm_result.get("summary", ""),
        "notification_sent": False,
        "notification_timestamp": None,
        "ttl": int(time.time()) + 7*24*3600
    }
    return convert_floats(item)   # ✅ ensures no floats go to DynamoDB


# -------- Lambda Handler --------
def lambda_handler(event, context):
    # Determine records and correlation_id
    if "source" in event and event.get("source") == "eventbridge":
        records = [{"s3": {"bucket": {"name": event["bucket"]}, 
                           "object": {"key": event["key"], "versionId": event.get("versionId", "null")}}}]
        correlation_id = event.get("correlation_id", "unknown")
    elif "Records" in event:
        records = event["Records"]
        correlation_id = None
    elif "bucket" in event and "key" in event:
        records = [{"s3": {"bucket": {"name": event["bucket"]}, 
                           "object": {"key": event["key"], "versionId": event.get("versionId", "null")}}}]
        correlation_id = event.get("correlation_id", "unknown")
    else:
        log("ERROR", "invalid_event_format", event=event)
        return {"status": "failed", "reason": "invalid_event_format"}

    results = []

    for rec in records:
        bucket = rec["s3"]["bucket"]["name"]
        key    = rec["s3"]["object"]["key"]
        version_id = rec["s3"]["object"].get("versionId", "null")
        corr_id = correlation_id if correlation_id else f"{bucket}/{key}@{version_id}"

        log("INFO", "analysis_started", correlation_id=corr_id)

        # Idempotency check
        mkey = marker_key(bucket, key, version_id)
        if object_exists(BUCKET_NAME, mkey):
            log("INFO", "already_analyzed", correlation_id=corr_id, marker=mkey)
            results.append({"correlation_id": corr_id, "status": "skipped", "reason": "already_analyzed"})
            continue

        # Skip non-processed prefix
        if not key.startswith(PROCESSED_PREFIX):
            log("INFO", "skip_non_processed_prefix", correlation_id=corr_id)
            results.append({"correlation_id": corr_id, "status": "skipped", "reason": "non_processed_prefix"})
            continue

        try:
            rows, _ = csv_to_dicts(bucket, key)
            if not rows:
                log("INFO", "no_data_to_analyze", correlation_id=corr_id)
                results.append({"correlation_id": corr_id, "status": "skipped", "reason": "no_data"})
                continue

            # 1. Detect anomalies per row
            anomalies = detect_anomalies(rows)

            # 2. Get insights/summary from Bedrock
            llm_result = analyze_with_llm(rows, anomalies)

            # 3. Save results to DynamoDB
            ddb_item = serialize_ddb_item(anomalies, llm_result, corr_id, key, rows)
            save_to_dynamodb(ddb_item)

            # 4. Upload full analysis (anomalies + summary) to S3
            analysis_output = {
                "anomalies": anomalies,
                "insights": llm_result["insights"],
                "recommendations": llm_result["recommendations"],
                "summary": llm_result["summary"]
            }
            analysis_key = f"{ANALYSIS_PREFIX}{os.path.basename(key).replace('.csv','')}_analysis.json"
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=analysis_key,
                Body=json.dumps(analysis_output, indent=2).encode("utf-8"),
                ContentType="application/json"
            )


            # Write idempotency marker
            s3.put_object(Bucket=BUCKET_NAME, Key=mkey, Body=b"")

            log("INFO", "analysis_completed",
                correlation_id=corr_id,
                analysis_key=analysis_key,
                rows_analyzed=len(rows),
                dynamodb_table=DDB_TABLE)

            # Send EventBridge event
            event_detail = {
                "correlation_id": corr_id,
                "bucket": BUCKET_NAME,
                "source_key": key,
                "analysis_key": analysis_key,
                "row_count": len(rows),
                "status": "success",
                "summary": llm_result.get("summary", "Analysis completed")
            }
            send_event_to_eventbridge("Data Analysis Complete", event_detail, corr_id)

            results.append({
                "correlation_id": corr_id,
                "status": "success",
                "analysis_key": analysis_key,
                "rows_analyzed": len(rows)
            })

        except Exception as e:
            log("ERROR", "analysis_failed", correlation_id=corr_id, error=str(e))
            send_event_to_eventbridge("Data Analysis Failed", {
                "correlation_id": corr_id,
                "bucket": bucket,
                "source_key": key,
                "status": "failed",
                "error": str(e)
            }, corr_id)
            results.append({"correlation_id": corr_id, "status": "failed", "error": str(e)})

    return {"status": "ok", "results": results}
