import boto3
import os
import json
from datetime import datetime

# -------- CONFIG --------
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")

DDB_TABLE = os.environ.get("DDB_TABLE", "health_analysis")
SES_SENDER = os.environ.get("SES_SENDER")
SES_RECIPIENTS = os.environ.get("SES_RECIPIENTS", "").split(",")

# -------- Logging --------
def log(level, message, **kwargs):
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message
    }
    payload.update(kwargs)
    print(json.dumps(payload))

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
def send_email(subject, body_text, body_html):
    if not SES_SENDER or not SES_RECIPIENTS:
        log("ERROR", "SES_not_configured")
        return

    try:
        ses.send_email(
            Source=SES_SENDER,
            Destination={"ToAddresses": SES_RECIPIENTS},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": body_text, "Charset": "UTF-8"},
                    "Html": {"Data": body_html, "Charset": "UTF-8"},
                },
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

    # Build anomaly frequency
    top_anomalies = {}
    for item in items:
        for anomaly in item.get("anomalies", []):
            key = anomaly.get("anomaly", "Unknown")
            top_anomalies[key] = top_anomalies.get(key, 0) + 1

    # Collect insights, recommendations, and summaries from DynamoDB
    insights_list = [i for item in items for i in item.get("insights", [])]
    recommendations_list = [r for item in items for r in item.get("recommendations", [])]
    summaries = [item.get("summary") for item in items if item.get("summary")]
    combined_summary = "\n".join(summaries) if summaries else "No summary available."

    # Email content
    subject = f"Health Data Analysis Report ({datetime.utcnow().date()})"
    anomalies_text = "\n".join([f"- {k}: {v}" for k, v in top_anomalies.items()]) or "None"

    body_text = (
        f"Health Data Analysis Summary\n\n"
        f"Total Rows Processed: {total_rows}\n"
        f"Total Anomalies Detected: {total_anomalies}\n"
        f"Top Anomalies:\n{anomalies_text}\n\n"
        f"Insights:\n" + "\n".join(f"- {i}" for i in insights_list) + "\n\n"
        f"Recommendations:\n" + "\n".join(f"- {r}" for r in recommendations_list) + "\n\n"
        f"Executive Summary:\n{combined_summary}\n"
    )

    body_html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
        h2 {{ color: #2E86C1; }}
        h3 {{ color: #C0392B; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #2980B9; color: white; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        ul {{ margin-top: 0; }}
        .section-header {{ color: #117A65; font-weight: bold; font-size: 16px; margin-top: 15px; }}
    </style>
    </head>
    <body>
    <h2>Health Data Analysis Report</h2>
    <p><b>Total Rows Processed:</b> {total_rows}</p>
    <p><b>Total Anomalies Detected:</b> {total_anomalies}</p>

    <h3>Top Anomalies</h3>
    <table>
        <tr>
        <th>Anomaly</th>
        <th>Count</th>
        </tr>
        {''.join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in top_anomalies.items())}
    </table>

    <div class="section-header">Insights</div>
    <ul>
        {''.join(f"<li>{i}</li>" for i in insights_list)}
    </ul>

    <div class="section-header">Recommendations</div>
    <ul>
        {''.join(f"<li>{r}</li>" for r in recommendations_list)}
    </ul>

    <div class="section-header">Executive Summary</div>
    <p>{combined_summary}</p>
    </body>
    </html>
    """

    send_email(subject, body_text, body_html)

    return {"status": "success", "total_rows": total_rows, "total_anomalies": total_anomalies}
