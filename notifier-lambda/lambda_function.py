import boto3
import os
import json
from datetime import datetime
from decimal import Decimal

# -------- CONFIG --------
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")

DDB_TABLE = os.environ.get("DDB_TABLE", "health_analysis")
SES_SENDER = os.environ.get("SES_SENDER")
SES_RECIPIENTS = os.environ.get("SES_RECIPIENTS", "").split(",")


# -------- Logging --------
def log(level, message, **kwargs):
    def convert(obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        else:
            return obj

    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message
    }
    payload.update(kwargs)
    safe_payload = convert(payload)
    print(json.dumps(safe_payload))


# -------- DynamoDB Retrieval --------
def fetch_recent_analysis(correlation_id=None, limit=10):
    table = dynamodb.Table(DDB_TABLE)
    scan_kwargs = {"Limit": limit}
    if correlation_id:
        scan_kwargs["FilterExpression"] = "correlation_id = :cid"
        scan_kwargs["ExpressionAttributeValues"] = {":cid": correlation_id}

    response = table.scan(**scan_kwargs)
    items = response.get("Items", [])

    log("INFO", "fetched_items",
        count=len(items),
        correlation_id=correlation_id,
        sample_keys=list(items[0].keys()) if items else [])

    return items


# -------- Extract Email Data --------
def extract_ddb_content(item):
    """Extract insights, recommendations, and summary from a DynamoDB item"""
    insights = []
    recommendations = []
    summary_text = ""

    # Insights
    if "insights" in item and isinstance(item["insights"], list):
        insights = [i for i in item["insights"] if isinstance(i, str)]

    # Recommendations
    if "recommendations" in item and isinstance(item["recommendations"], list):
        recommendations = [r for r in item["recommendations"] if isinstance(r, str)]

    # Summary
    summary_val = item.get("summary", {})
    if isinstance(summary_val, dict):
        health_status = summary_val.get("health_status", "")
        key_findings = summary_val.get("key_findings", {})

        findings_texts = []
        if isinstance(key_findings, dict):
            for v in key_findings.values():
                if isinstance(v, str):
                    findings_texts.append(v)

        if health_status:
            summary_text = health_status.strip()
        if findings_texts:
            summary_text += " Key findings: " + " ".join(findings_texts)

    elif isinstance(summary_val, str):
        summary_text = summary_val.strip()

    return insights, recommendations, summary_text


# -------- SES Email --------
def send_email(subject, body_text, body_html):
    if not SES_SENDER or not SES_RECIPIENTS:
        log("ERROR", "SES_not_configured")
        return False

    try:
        ses.send_email(
            Source=SES_SENDER,
            Destination={"ToAddresses": [r.strip() for r in SES_RECIPIENTS if r.strip()]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": body_text, "Charset": "UTF-8"},
                    "Html": {"Data": body_html, "Charset": "UTF-8"},
                },
            }
        )
        log("INFO", "email_sent", subject=subject, recipients=len(SES_RECIPIENTS))
        return True
    except Exception as e:
        log("ERROR", "email_failed", error=str(e))
        return False


# -------- Lambda Handler --------
def lambda_handler(event, context):
    correlation_id = event.get("correlation_id")
    items = fetch_recent_analysis(correlation_id=correlation_id, limit=1)

    if not items:
        log("INFO", "no_analysis_found", correlation_id=correlation_id)
        return {"status": "no_data", "message": "No analysis data found"}

    item = items[0]

    # Extract only what’s in DynamoDB
    insights, recommendations, executive_summary = extract_ddb_content(item)

    # Email content
    subject = f"Health Data Analysis Report - {datetime.utcnow().strftime('%B %d, %Y')}"

    body_text = f"""Health Data Analysis Report
Generated on: {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p UTC')}

💡 Key Health Insights
{chr(10).join(f"- {i}" for i in insights) if insights else "- None"}

📋 Recommended Actions
{chr(10).join(f"- {r}" for r in recommendations) if recommendations else "- None"}

📊 Executive Summary
{executive_summary or "No summary available"}
"""

    body_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2>💡 Key Health Insights</h2>
        <ul>{"".join(f"<li>{i}</li>" for i in insights) or "<li>None</li>"}</ul>

        <h2>📋 Recommended Actions</h2>
        <ul>{"".join(f"<li>{r}</li>" for r in recommendations) or "<li>None</li>"}</ul>

        <h2>📊 Executive Summary</h2>
        <p>{executive_summary or "No summary available"}</p>
    </body>
    </html>
    """

    # Send email
    email_sent = send_email(subject, body_text, body_html)

    if email_sent:
        try:
            table = dynamodb.Table(DDB_TABLE)
            table.update_item(
                Key={"correlation_id": item["correlation_id"]},
                UpdateExpression="SET notification_sent = :sent, notification_timestamp = :ts",
                ExpressionAttributeValues={
                    ":sent": True,
                    ":ts": datetime.utcnow().isoformat() + "Z"
                }
            )
        except Exception as e:
            log("WARN", "failed_to_update_notification_status",
                correlation_id=item.get("correlation_id"), error=str(e))

    return {
        "status": "success" if email_sent else "failed",
        "insights_count": len(insights),
        "recommendations_count": len(recommendations),
        "email_sent": email_sent,
        "report_generated": datetime.utcnow().isoformat() + "Z"
    }
