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
    from decimal import Decimal

    def convert(obj):
        if isinstance(obj, Decimal):
            # Convert integer-like Decimals to int, others to float
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

# -------- Helper Functions --------
def extract_insights_and_recommendations(items):
    """Extract insights and recommendations from DynamoDB items"""
    all_insights = []
    all_recommendations = []
    
    for item in items:
        log("DEBUG", "processing_item", 
            correlation_id=item.get("correlation_id", "unknown"),
            has_insights=bool(item.get("insights")),
            has_recommendations=bool(item.get("recommendations")))
        
        # Extract insights
        insights = item.get("insights", [])
        if insights and isinstance(insights, list) and insights != ["No major trends observed"]:
            all_insights.extend(insights)
            log("DEBUG", "added_insights", count=len(insights))
        
        # Extract recommendations  
        recommendations = item.get("recommendations", [])
        if recommendations and isinstance(recommendations, list) and recommendations != ["No recommendations"]:
            all_recommendations.extend(recommendations)
            log("DEBUG", "added_recommendations", count=len(recommendations))
    
    # Remove duplicates while preserving order
    unique_insights = list(dict.fromkeys(all_insights))
    unique_recommendations = list(dict.fromkeys(all_recommendations))
    
    log("INFO", "extracted_data", 
        total_insights=len(unique_insights), 
        total_recommendations=len(unique_recommendations))
    
    return unique_insights, unique_recommendations

def format_executive_summary(items):
    """Extract only the summary text from the stored JSON in DynamoDB"""
    summaries = []
    
    for item in items:
        summary_json = item.get("summary")
        if isinstance(summary_json, dict) and "summary" in summary_json:
            summaries.append(summary_json["summary"])
        elif isinstance(summary_json, str):
            summaries.append(summary_json)
    
    if not summaries:
        return "Health data analysis completed successfully. Regular monitoring continues."
    
    # Use only the first summary if multiple
    return summaries[0]


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

def format_executive_summary(items):
    """Extract only the summary field from DynamoDB items"""
    summaries = []

    for item in items:
        summary = item.get("summary", "").strip()
        if summary and summary != "Analysis completed.":
            summaries.append(summary)

    if not summaries:
        return "Health data analysis completed successfully. Regular monitoring continues."

    # Combine all summaries into one text
    # But if too long, take only the first summary
    combined_summary = " ".join(summaries)
    if len(combined_summary) > 500:
        combined_summary = summaries[0]

    return combined_summary


# -------- Lambda Handler --------
def lambda_handler(event, context):
    correlation_id = event.get("correlation_id")  # optional filter
    items = fetch_recent_analysis(correlation_id=correlation_id, limit=5)

    if not items:
        log("INFO", "no_analysis_found", correlation_id=correlation_id)
        return {"status": "no_data", "message": "No analysis data found"}

    # Aggregate row counts and anomalies
    total_rows = sum(item.get("records_analyzed", 0) for item in items)
    total_anomalies = sum(len(item.get("anomalies", [])) for item in items)

    # Build anomaly frequency
    top_anomalies = {}
    for item in items:
        for anomaly in item.get("anomalies", []):
            key = anomaly.get("anomaly", "Unknown")
            top_anomalies[key] = top_anomalies.get(key, 0) + 1

    # Sort anomalies by frequency (most common first)
    sorted_anomalies = sorted(top_anomalies.items(), key=lambda x: x[1], reverse=True)

    # Extract insights and recommendations
    insights, recommendations = extract_insights_and_recommendations(items)
    
    # Generate executive summary
    executive_summary = format_executive_summary(items)
    executive_summary_html = executive_summary.replace("\n", "<br>")


    log("INFO", "email_data_prepared", 
        total_rows=total_rows, 
        total_anomalies=total_anomalies,
        insights_count=len(insights),
        recommendations_count=len(recommendations),
        anomaly_types=len(sorted_anomalies))

    # Email content
    subject = f"Health Data Analysis Report - {datetime.utcnow().strftime('%B %d, %Y')}"
    
    # Format content for email
    anomalies_text = "\n".join([f"  • {anomaly}: {count} occurrences" for anomaly, count in sorted_anomalies[:10]]) or "  • No anomalies detected"
    insights_text = "\n".join([f"  • {insight}" for insight in insights[:10]]) or "  • Continuing to monitor health patterns"
    recommendations_text = "\n".join([f"  • {rec}" for rec in recommendations[:10]]) or "  • Continue regular health monitoring"

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

    # HTML version with better formatting
    anomalies_table_rows = "".join([
        f"<tr><td>{anomaly}</td><td style='text-align: center;'><strong>{count}</strong></td></tr>" 
        for anomaly, count in sorted_anomalies[:10]
    ]) or "<tr><td colspan='2' style='text-align: center; color: #2ECC71;'>No anomalies detected</td></tr>"

    insights_html = "".join([f"<li>{insight}</li>" for insight in insights[:10]]) or "<li style='color: #7F8C8D;'>Continuing to monitor health patterns</li>"
    
    recommendations_html = "".join([f"<li>{rec}</li>" for rec in recommendations[:10]]) or "<li style='color: #7F8C8D;'>Continue regular health monitoring</li>"

    # Format executive summary for HTML (preserve line breaks)
    executive_summary_html = executive_summary.replace('\n', '<br>')

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

    # Send email
    email_sent = send_email(subject, body_text, body_html)
    
    if email_sent:
        # Update notification status in DynamoDB
        table = dynamodb.Table(DDB_TABLE)
        for item in items:
            try:
                table.update_item(
                    Key={"correlation_id": item["correlation_id"], "analysis_id": item["analysis_id"]},
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
        "total_rows": total_rows,
        "total_anomalies": total_anomalies,
        "insights_count": len(insights),
        "recommendations_count": len(recommendations),
        "email_sent": email_sent,
        "report_generated": datetime.utcnow().isoformat() + "Z"
    }