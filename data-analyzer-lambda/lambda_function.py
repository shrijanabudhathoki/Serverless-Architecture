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

#    CONFIG   
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
EVENT_BUS_NAME   = os.environ.get("EVENT_BUS_NAME")

def convert_floats(obj):
    if isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))  # Safe conversion
    else:
        return obj

#    Logging Utility   
def log(level, message, **kwargs):
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message
    }
    payload.update(kwargs)
    
    # Convert Decimal objects to regular numbers for JSON serialization
    def decimal_default(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError
    
    try:
        print(json.dumps(payload, default=decimal_default))
    except TypeError:
        # Fallback: convert payload to string if JSON serialization still fails
        print(f"{payload['ts']} [{payload['level']}] {payload['message']} {kwargs}")

#    Helper Functions   
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

def calculate_statistics(rows):
    """Calculate basic statistics from the health data"""
    if not rows:
        return {}
    
    heart_rates = [int(row["heart_rate"]) for row in rows]
    spo2_values = [int(row["spo2"]) for row in rows]
    temps = [float(row["temp_c"]) for row in rows]
    sys_bp = [int(row["systolic_bp"]) for row in rows]
    dia_bp = [int(row["diastolic_bp"]) for row in rows]
    steps = [int(row["steps"]) for row in rows]
    
    return {
        "avg_heart_rate": round(sum(heart_rates) / len(heart_rates), 1),
        "avg_spo2": round(sum(spo2_values) / len(spo2_values), 1),
        "avg_temp": round(sum(temps) / len(temps), 1),
        "avg_systolic": round(sum(sys_bp) / len(sys_bp), 1),
        "avg_diastolic": round(sum(dia_bp) / len(dia_bp), 1),
        "avg_steps": round(sum(steps) / len(steps), 0),
        "max_heart_rate": max(heart_rates),
        "min_heart_rate": min(heart_rates),
        "max_temp": max(temps),
        "min_spo2": min(spo2_values)
    }

def analyze_with_llm(rows, anomalies):
    # Calculate statistics
    stats = calculate_statistics(rows)
    
    sample_text = json.dumps(rows[:20])
    anomaly_summary = f"{len(anomalies)} anomalies detected in {len(rows)} records."
    stats_summary = f"Statistics: Avg HR {stats.get('avg_heart_rate', 0)} bpm, Avg SpO2 {stats.get('avg_spo2', 0)}%, Avg Temp {stats.get('avg_temp', 0)}°C"

    prompt = (
        "You are a health data analysis assistant. "
        "Analyze the health metrics and provide insights about trends, patterns, and health risks.\n\n"

        f"Dataset Overview:\n"
        f"- Total records: {len(rows)}\n"
        f"- {anomaly_summary}\n"
        f"- {stats_summary}\n\n"

        f"Sample health records:\n{sample_text}\n\n"

        "Provide analysis in JSON format with these keys:\n"
        "- insights: Array of specific health observations (e.g., 'Average heart rate of 123 bpm indicates elevated cardiovascular activity')\n"
        "- recommendations: Array of actionable health advice (e.g., 'Consult cardiologist for persistent high heart rate readings')\n"
        "- summary: Executive summary of key findings and health status\n\n"
        
        "Focus on:\n"
        "1. Cardiovascular health patterns\n"
        "2. Respiratory health (SpO2 trends)\n"
        "3. Temperature anomalies and fever patterns\n"
        "4. Blood pressure health risks\n"
        "5. Activity level assessment\n\n"
        
        "Return only valid JSON."
    )

    response = bedrock.invoke_model(
        modelId=os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0"),
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "messages": [
                {"role": "user", "content": [{"text": prompt}]}
            ],
            "inferenceConfig": {"maxTokens": 800, "temperature": 0.3}
        })
    )
    raw = response["body"].read()
    payload = json.loads(raw)
    output_text = payload["output"]["message"]["content"][0]["text"]

    # Clean up the output text (remove markdown formatting if present)
    if output_text.startswith('```json'):
        output_text = output_text.replace('```json', '').replace('```', '').strip()
    
    COST_PER_1K_TOKENS = 0.00006	  # USD
    
    usage = payload.get("usage", {}) 
    prompt_tokens = usage.get("promptTokens", 0)
    completion_tokens = usage.get("completionTokens", 0)
    total_tokens = usage.get("totalTokens", 0)
    estimated_cost = (total_tokens / 1000) * COST_PER_1K_TOKENS

    log("INFO", "bedrock_usage", 
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        estimated_cost_usd=estimated_cost
    )

    result = json.loads(output_text)
    log("INFO", "llm_analysis_success", insights_count=len(result.get("insights", [])), recommendations_count=len(result.get("recommendations", [])))
        
    return {
        "insights": result.get("insights", []),
        "recommendations": result.get("recommendations", []),
        "summary": result.get("summary", "Analysis completed.")
    }

def save_to_dynamodb(item):
    table = dynamodb.Table(DDB_TABLE)
    try:
        table.put_item(Item=item)
        log("INFO", "dynamodb_save_success", correlation_id=item.get("correlation_id"))
    except Exception as e:
        log("ERROR", "dynamodb_save_failed", error=str(e), correlation_id=item.get("correlation_id"))
        raise

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

#    Serialize DynamoDB item   
def serialize_ddb_item(anomalies, llm_result, correlation_id, key, rows):
    """Create DynamoDB item with proper data types"""
    
    # Ensure we have valid insights and recommendations
    insights = llm_result.get("insights", [])
    recommendations = llm_result.get("recommendations", [])
    summary = llm_result.get("summary", "Analysis completed.")
    
    log("INFO", "serializing_item", 
        correlation_id=correlation_id,
        insights_count=len(insights),
        recommendations_count=len(recommendations),
        summary_length=len(summary))

    item = {
        "correlation_id": correlation_id,
        "analysis_id": f"analysis_{int(time.time())}_{uuid.uuid4().hex[:8]}",
        "analysis_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_file": key,
        "processed_file": key.replace("raw/", "processed/"),
        "records_analyzed": len(rows),
        "anomalies": anomalies,
        "insights": insights,  # Direct assignment - no fallback logic here
        "recommendations": recommendations,  # Direct assignment - no fallback logic here
        "summary": summary,
        "notification_sent": False,
        "notification_timestamp": None,
        "ttl": int(time.time()) + 7*24*3600
    }
    return convert_floats(item)

#    Lambda Handler   
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
            
            log("INFO", "llm_analysis_completed", 
                correlation_id=corr_id,
                insights=len(llm_result.get("insights", [])),
                recommendations=len(llm_result.get("recommendations", [])))

            # 3. Save results to DynamoDB
            ddb_item = serialize_ddb_item(anomalies, llm_result, corr_id, key, rows)
            save_to_dynamodb(ddb_item)

            # 4. Upload full analysis (anomalies + summary) to S3
            analysis_output = {
                "correlation_id": corr_id,
                "analysis_timestamp": datetime.utcnow().isoformat() + "Z",
                "records_analyzed": len(rows),
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
                anomalies_detected=len(anomalies),
                dynamodb_table=DDB_TABLE)

            # Send EventBridge event
            event_detail = {
                "correlation_id": corr_id,
                "bucket": BUCKET_NAME,
                "source_key": key,
                "analysis_key": analysis_key,
                "row_count": len(rows),
                "anomaly_count": len(anomalies),
                "status": "success",
                "summary": llm_result.get("summary", "Analysis completed")
            }
            send_event_to_eventbridge("Data Analysis Complete", event_detail, corr_id)

            results.append({
                "correlation_id": corr_id,
                "status": "success",
                "analysis_key": analysis_key,
                "rows_analyzed": len(rows),
                "anomalies_detected": len(anomalies)
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