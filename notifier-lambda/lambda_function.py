import boto3
import os
import json
from datetime import datetime
from decimal import Decimal

# -------- CONFIG --------
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
bedrock = boto3.client("bedrock-runtime")

DDB_TABLE = os.environ.get("DDB_TABLE", "health_analysis")
SES_SENDER = os.environ.get("SES_SENDER")
SES_RECIPIENTS = os.environ.get("SES_RECIPIENTS", "").split(",")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID")
BEDROCK_MAX_TOKENS = int(os.environ.get("BEDROCK_MAX_TOKENS", 500))

# -------- Logging --------
def log(level, message, **kwargs):
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message
    }
    payload.update(kwargs)
    print(json.dumps(payload))

# -------- Bedrock Executive Summary --------
def generate_summary(anomalies, insights, recommendations):
    prompt = f"""
You are a health data assistant. 
Summarize the following analysis for an executive audience. Focus on key anomalies, trends, and recommendations.

Anomalies: {json.dumps(anomalies)}
Insights: {json.dumps(insights)}
Recommendations: {json.dumps(recommendations)}

Return a concise JSON object with a single key 'summary' and a textual summary.
"""
    try:
        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            body=json.dumps({
                "messages": [{"role": "user", "content": [{"text": prompt}]}],
                "inferenceConfig": {"maxTokens": BEDROCK_MAX_TOKENS, "temperature": 0.5}
            })
        )
        raw = response["body"].read()
        payload = json.loads(raw)
        output_text = payload["output"]["message"]["content"][0]["text"]
        try:
            summary = json.loads(output_text).get("summary", output_text)
        except Exception:
            summary = output_text
        return summary
    except Exception as e:
        log("ERROR", "bedrock_summary_failed", error=str(e))
        return "Executive summary could not be generated."

# -------- DynamoDB Retrieval --------
def fetch_recent_analysis(correlation_id=None, limit=10):
    table = dynamodb.Table(DDB_TABLE)
    scan_kwargs = {"Limit": limit}
    if correlation_id:
        scan_kwargs["FilterExpression"] = "correlation_id = :cid"
        scan_kwargs["ExpressionAttributeValues"] = {":cid": correlation_id}
    response = table.scan(**scan_kwargs)
    return response.get("Items", [])

# -------- SES Email --------
def send_email(subject, body_text):
    if not SES_SENDER or not SES_RECIPIENTS:
        log("ERROR", "SES_not_configured")
        return

    try:
        ses.send_email(
            Source=SES_SENDER,
            Destination={"ToAddresses": SES_RECIPIENTS},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body_text}}
            }
        )
        log("INFO", "email_sent", subject=subject)
    except Exception as e:
        log("ERROR", "email_failed", error=str(e))

# -------- Lambda Handler --------
def lambda_handler(event, context):
    correlation_id = event.get("correlation_id")  # optional filter
    items = fetch_recent_analysis(correlation_id=correlation_id, limit=5)

    if not items:
        log("INFO", "no_analysis_found", correlation_id=correlation_id)
        return {"status": "no_data"}

    # Aggregate row counts and anomalies
    total_rows = sum(item.get("records_analyzed", 0) for item in items)
    total_anomalies = sum(len(item.get("anomalies", [])) for item in items)
    top_anomalies = {}
    for item in items:
        for anomaly in item.get("anomalies", []):
            key = anomaly.get("anomaly", "Unknown")
            top_anomalies[key] = top_anomalies.get(key, 0) + 1

    # Optional: generate executive summary via Bedrock
    anomalies_list = [a.get("anomaly") for item in items for a in item.get("anomalies", [])]
    insights_list = [i for item in items for i in item.get("insights", [])]
    recommendations_list = [r for item in items for r in item.get("recommendations", [])]

    executive_summary = generate_summary(anomalies_list, insights_list, recommendations_list)

    # Prepare email content
    body = (
        f"Health Data Analysis Summary\n\n"
        f"Total Rows Processed: {total_rows}\n"
        f"Total Anomalies Detected: {total_anomalies}\n"
        f"Top Anomalies: {json.dumps(top_anomalies, indent=2)}\n\n"
        f"Executive Summary:\n{executive_summary}\n"
    )
    subject = f"Health Data Analysis Report ({datetime.utcnow().date()})"

    # Send email
    send_email(subject, body)

    return {"status": "success", "total_rows": total_rows, "total_anomalies": total_anomalies}
