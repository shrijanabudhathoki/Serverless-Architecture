import boto3
import os
import json
from datetime import datetime
from decimal import Decimal

# -------- CONFIG --------
s3 = boto3.client("s3")
ses = boto3.client("ses")

BUCKET_NAME = os.environ.get("BUCKET_NAME")  # S3 bucket where analyses are stored
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

# -------- S3 Retrieval --------
def fetch_analysis_from_s3(analysis_key):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=analysis_key)
        content = obj["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        log("ERROR", "failed_to_fetch_analysis", analysis_key=analysis_key, error=str(e))
        raise

# -------- Helper Functions --------
def extract_insights_and_recommendations(items):
    all_insights, all_recommendations = [], []
    for item in items:
        insights = item.get("insights", [])
        if insights and insights != ["No major trends observed"]:
            all_insights.extend(insights)
        recommendations = item.get("recommendations", [])
        if recommendations and recommendations != ["No recommendations"]:
            all_recommendations.extend(recommendations)
    return list(dict.fromkeys(all_insights)), list(dict.fromkeys(all_recommendations))

def format_executive_summary(items):
    summaries = []
    for item in items:
        summary_val = item.get("summary", "")
        if isinstance(summary_val, dict):
            health_status = summary_val.get("health_status", "")
            key_findings = summary_val.get("key_findings", {})
            findings_texts = []
            if isinstance(key_findings, dict):
                for v in key_findings.values():
                    if isinstance(v, str):
                        findings_texts.append(v)
            text_summary = health_status.strip()
            if findings_texts:
                text_summary += " Key findings: " + " ".join(findings_texts)
        elif isinstance(summary_val, str):
            text_summary = summary_val.strip()
        if text_summary and text_summary != "Analysis completed.":
            summaries.append(text_summary)
    if not summaries:
        return "Health data analysis completed successfully. Regular monitoring continues."
    return "\n".join(summaries)

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
    """
    Expects EventBridge event:
    {
        "detail": {
            "analysis_key": "analyzed/file_analysis.json",
            "correlation_id": "bucket/key@version",
            ...
        }
    }
    """
    detail = event.get("detail", {})
    analysis_key = detail.get("analysis_key")
    correlation_id = detail.get("correlation_id", "unknown")

    if not analysis_key:
        log("ERROR", "missing_analysis_key", event=event)
        return {"status": "failed", "reason": "missing analysis_key"}

    log("INFO", "fetching_analysis", analysis_key=analysis_key, correlation_id=correlation_id)

    # Fetch the analysis from S3
    analysis_data = fetch_analysis_from_s3(analysis_key)
    items = [analysis_data]  # wrap in list to reuse existing helpers

    # Aggregate row counts and anomalies
    total_rows = analysis_data.get("records_analyzed", 0)
    total_anomalies = len(analysis_data.get("anomalies", []))

    # Build anomaly frequency
    top_anomalies = {}
    for anomaly in analysis_data.get("anomalies", []):
        key = anomaly.get("anomaly", "Unknown")
        top_anomalies[key] = top_anomalies.get(key, 0) + 1
    sorted_anomalies = sorted(top_anomalies.items(), key=lambda x: x[1], reverse=True)

    # Extract insights and recommendations
    insights, recommendations = extract_insights_and_recommendations(items)

    # Generate executive summary
    executive_summary = format_executive_summary(items)
    executive_summary_html = executive_summary.replace("\n", "<br>")

    # Prepare email content
    subject = f"Health Data Analysis Report - {datetime.utcnow().strftime('%B %d, %Y')}"
    anomalies_text = "\n".join([f"  • {a}: {c} occurrences" for a, c in sorted_anomalies[:10]]) or "  • No anomalies detected"
    insights_text = "\n".join([f"  • {i}" for i in insights[:10]]) or "  • Continuing to monitor health patterns"
    recommendations_text = "\n".join([f"  • {r}" for r in recommendations[:10]]) or "  • Continue regular health monitoring"

    body_text = f"""Health Data Analysis Report
Generated on: {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p UTC')}

=== OVERVIEW ===
Total Health Records Processed: {total_rows:,}
Total Anomalies Detected: {total_anomalies:,}

=== TOP HEALTH ANOMALIES ===
{anomalies_text}

=== KEY HEALTH INSIGHTS ===
{insights_text}

=== RECOMMENDATIONS ===
{recommendations_text}

=== EXECUTIVE SUMMARY ===
{executive_summary}

This report is automatically generated from your health monitoring system.
If you have concerns about any anomalies, please consult with a healthcare professional.
"""

    anomalies_table_rows = "".join([
        f"<tr><td>{a}</td><td style='text-align: center;'><strong>{c}</strong></td></tr>" for a, c in sorted_anomalies[:10]
    ]) or "<tr><td colspan='2' style='text-align: center; color: #2ECC71;'>No anomalies detected</td></tr>"

    insights_html = "".join([f"<li>{i}</li>" for i in insights[:10]]) or "<li style='color: #7F8C8D;'>Continuing to monitor health patterns</li>"
    recommendations_html = "".join([f"<li>{r}</li>" for r in recommendations[:10]]) or "<li style='color: #7F8C8D;'>Continue regular health monitoring</li>"

    body_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{ 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                line-height: 1.6; 
                color: #333; 
                max-width: 800px; 
                margin: 0 auto; 
                padding: 20px;
                background-color: #f8f9fa;
            }}
            .container {{ 
                background: white; 
                padding: 30px; 
                border-radius: 10px; 
                box-shadow: 0 2px 10px rgba(0,0,0,0.1); 
            }}
            .header {{ 
                text-align: center; 
                margin-bottom: 30px; 
                padding-bottom: 20px; 
                border-bottom: 3px solid #3498DB; 
            }}
            h1 {{ 
                color: #2C3E50; 
                margin-bottom: 10px; 
                font-size: 28px; 
            }}
            .subtitle {{ 
                color: #7F8C8D; 
                font-size: 14px; 
                margin: 0; 
            }}
            .metrics {{ 
                display: flex; 
                justify-content: space-around; 
                margin: 20px 0; 
                padding: 20px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                border-radius: 8px; 
                color: white; 
            }}
            .metric {{ 
                text-align: center; 
                margin: 0 15px
            }}
            .metric-number {{ 
                font-size: 32px; 
                font-weight: bold; 
                display: block; 
            }}
            .metric-label {{ 
                font-size: 12px; 
                opacity: 0.9; 
            }}
            h2 {{ 
                color: #2980B9; 
                margin-top: 30px; 
                margin-bottom: 15px; 
                padding-bottom: 8px; 
                border-bottom: 2px solid #ECF0F1; 
            }}
            table {{ 
                border-collapse: collapse; 
                width: 100%; 
                margin: 15px 0; 
                border-radius: 8px; 
                overflow: hidden; 
                box-shadow: 0 2px 8px rgba(0,0,0,0.1); 
            }}
            th {{ 
                background: linear-gradient(135deg, #3498DB, #2980B9); 
                color: white; 
                font-weight: 600; 
                padding: 12px; 
                text-align: left; 
            }}
            td {{ 
                padding: 12px; 
                border-bottom: 1px solid #ECF0F1; 
            }}
            tr:nth-child(even) {{ 
                background-color: #F8F9FA; 
            }}
            tr:hover {{ 
                background-color: #E3F2FD; 
            }}
            ul {{ 
                margin: 15px 0; 
                padding-left: 0; 
            }}
            li {{ 
                list-style: none; 
                padding: 8px 0; 
                padding-left: 25px; 
                position: relative; 
            }}
            li:before {{ 
                content: '✓'; 
                position: absolute; 
                left: 0; 
                color: #27AE60; 
                font-weight: bold; 
            }}
            .summary-box {{ 
                background: linear-gradient(135deg, #74b9ff, #0984e3); 
                color: white; 
                padding: 20px; 
                border-radius: 8px; 
                margin: 20px 0; 
            }}
            .summary-box h3 {{ 
                margin-top: 0; 
                color: white; 
            }}
            .footer {{ 
                margin-top: 30px; 
                padding-top: 20px; 
                border-top: 1px solid #ECF0F1; 
                text-align: center; 
                color: #7F8C8D; 
                font-size: 12px; 
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Health Data Analysis Report</h1>
                <p class="subtitle">Generated on {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p UTC')}</p>
            </div>

            <div class="metrics">
                <div class="metric">
                    <span class="metric-number">{total_rows:,}</span>
                    <span class="metric-label">Records Processed</span>
                </div>
                <div class="metric">
                    <span class="metric-number">{total_anomalies:,}</span>
                    <span class="metric-label">Anomalies Detected</span>
                </div>
            </div>

            <h2>🔍 Top Health Anomalies</h2>
            <table>
                <tr>
                    <th>Health Anomaly</th>
                    <th style="text-align: center;">Frequency</th>
                </tr>
                {anomalies_table_rows}
            </table>

            <h2>💡 Key Health Insights</h2>
            <ul>
                {insights_html}
            </ul>

            <h2>📋 Recommended Actions</h2>
            <ul>
                {recommendations_html}
            </ul>

            <div class="summary-box">
                <h3>📊 Executive Summary</h3>
                <p>{executive_summary_html}</p>
            </div>

            <div class="footer">
                <p>This report is automatically generated from your health monitoring system.<br>
                If you have concerns about any anomalies, please consult with a healthcare professional.</p>
            </div>
        </div>
    </body>
    </html>
    """

    email_sent = send_email(subject, body_text, body_html)

    return {
        "status": "success" if email_sent else "failed",
        "total_rows": total_rows,
        "total_anomalies": total_anomalies,
        "insights_count": len(insights),
        "recommendations_count": len(recommendations),
        "email_sent": email_sent,
        "report_generated": datetime.utcnow().isoformat() + "Z"
    }