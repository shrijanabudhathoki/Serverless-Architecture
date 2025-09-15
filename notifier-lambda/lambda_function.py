import boto3
import os
import json
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

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
    
    if correlation_id:
        # Query specific correlation_id and get latest analysis
        response = table.query(
            KeyConditionExpression=Key('correlation_id').eq(correlation_id),
            ScanIndexForward=False,  # Sort by range key (analysis_id) descending
            Limit=limit
        )
        items = response.get("Items", [])
    else:
        # Get all recent items and sort them by timestamp
        response = table.scan()
        all_items = response.get("Items", [])
        
        # Continue scanning if there are more items
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            all_items.extend(response.get("Items", []))
        
        # Sort by analysis_timestamp (newest first) and take the limit
        items = sorted(
            all_items,
            key=lambda x: x.get('analysis_timestamp', ''),
            reverse=True
        )[:limit]
    
    log("INFO", "fetched_items", 
        count=len(items),
        correlation_id=correlation_id,
        sample_keys=list(items[0].keys()) if items else [],
        latest_timestamp=items[0].get('analysis_timestamp') if items else None)
    
    return items

# Alternative function using GSI (if you implement Option 1)
def fetch_recent_analysis_with_gsi(correlation_id=None, limit=10):
    table = dynamodb.Table(DDB_TABLE)
    
    if correlation_id:
        # Query specific correlation_id
        response = table.query(
            KeyConditionExpression=Key('correlation_id').eq(correlation_id),
            ScanIndexForward=False,
            Limit=limit
        )
        items = response.get("Items", [])
    else:
        # Use GSI to get items sorted by timestamp
        response = table.scan(
            IndexName='TimestampIndex',
            Limit=limit * 3  # Get more items since we'll sort them
        )
        all_items = response.get("Items", [])
        
        # Sort by timestamp (newest first)
        items = sorted(
            all_items,
            key=lambda x: x.get('analysis_timestamp', ''),
            reverse=True
        )[:limit]
    
    log("INFO", "fetched_items_with_gsi", 
        count=len(items),
        correlation_id=correlation_id,
        latest_timestamp=items[0].get('analysis_timestamp') if items else None)
    
    return items

# -------- Helper Functions --------
def extract_insights_and_recommendations(items):
    """Extract insights and recommendations from the most recent DynamoDB item only"""
    if not items:
        return [], []
    
    # Only use the most recent item (first item after sorting)
    most_recent_item = items[0]
    
    log("DEBUG", "processing_most_recent_item", 
        correlation_id=most_recent_item.get("correlation_id", "unknown"),
        timestamp=most_recent_item.get("analysis_timestamp", "unknown"),
        has_insights=bool(most_recent_item.get("insights")),
        has_recommendations=bool(most_recent_item.get("recommendations")))
    
    # Extract insights from most recent analysis only
    insights = most_recent_item.get("insights", [])
    if not (insights and isinstance(insights, list) and insights != ["No major trends observed"]):
        insights = []
    
    # Extract recommendations from most recent analysis only
    recommendations = most_recent_item.get("recommendations", [])
    if not (recommendations and isinstance(recommendations, list) and recommendations != ["No recommendations"]):
        recommendations = []
    
    log("INFO", "extracted_data_from_latest", 
        insights_count=len(insights), 
        recommendations_count=len(recommendations),
        analysis_timestamp=most_recent_item.get("analysis_timestamp", "unknown"))
    
    return insights, recommendations

def format_executive_summary(items):
    summaries = []

    for item in items:
        summary_val = item.get("summary", {})
        text_summary = ""

        if isinstance(summary_val, dict):
            # Extract only health_status + key_findings
            health_status = summary_val.get("health_status", "")
            key_findings = summary_val.get("key_findings", {})

            findings_texts = []
            if isinstance(key_findings, dict):
                for v in key_findings.values():
                    if isinstance(v, str):
                        findings_texts.append(v)

            if health_status:
                text_summary = health_status.strip()
            if findings_texts:
                text_summary += " Key findings: " + " ".join(f.strip() for f in findings_texts)

        elif isinstance(summary_val, str):
            # Old format: if summary is already a plain string
            text_summary = summary_val.strip()

        if text_summary and text_summary != "Analysis completed.":
            summaries.append(text_summary)

    if not summaries:
        return "Health data analysis completed successfully. Regular monitoring continues."

    # Only use the first one (which should be the most recent)
    return "\n".join(summaries[:1])  # Just take the most recent summary

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
    correlation_id = event.get("correlation_id")  # optional filter
    items = fetch_recent_analysis(correlation_id=correlation_id, limit=5)

    if not items:
        log("INFO", "no_analysis_found", correlation_id=correlation_id)
        return {"status": "no_data", "message": "No analysis data found"}

    log("INFO", "processing_recent_items", 
        item_count=len(items),
        newest_timestamp=items[0].get('analysis_timestamp') if items else None,
        oldest_timestamp=items[-1].get('analysis_timestamp') if items else None)

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
    executive_summary_html = str(executive_summary).replace("\n", "<br>")

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
Based on analysis from: {items[0].get('analysis_timestamp', 'Unknown') if items else 'Unknown'}

=== OVERVIEW ===
Total Health Records Processed: {total_rows:,}
Total Anomalies Detected: {total_anomalies:,}
Analysis Period: {len(items)} recent analysis runs

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
            .timestamp-info {{
                background: #f8f9fa;
                padding: 10px;
                border-radius: 5px;
                margin: 10px 0;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="health-icon">🏥</div>
                <h1>Health Data Analysis Report</h1>
                <p class="subtitle">Generated on {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p UTC')}</p>
                <div class="timestamp-info">
                    <strong>Latest analysis:</strong> {items[0].get('analysis_timestamp', 'Unknown') if items else 'Unknown'}<br>
                    <strong>Analysis period:</strong> {len(items)} recent runs
                </div>
            </div>

            <div class="content">
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

                <div class="section">
                    <h2>🔍 Top Health Anomalies</h2>
                    <div class="anomaly-table">
                        <table>
                            <tr>
                                <th>Health Anomaly</th>
                                <th style="text-align: center;">Frequency</th>
                            </tr>
                            {anomalies_table_rows}
                        </table>
                    </div>
                </div>

                <div class="section">
                    <h2>💡 Key Health Insights</h2>
                    <div class="insights-list">
                        <ul>
                            {insights_html}
                        </ul>
                    </div>
                </div>

                <div class="section">
                    <h2>📋 Recommended Actions</h2>
                    <div class="recommendations-list">
                        <ul>
                            {recommendations_html}
                        </ul>
                    </div>
                </div>

                <div class="summary-box">
                    <h3>📊 Executive Summary</h3>
                    <p>{executive_summary_html}</p>
                </div>
            </div>

            <div class="footer">
                <p><strong>Important:</strong> This report is automatically generated from your health monitoring system.<br>
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
        "report_generated": datetime.utcnow().isoformat() + "Z",
        "latest_analysis_timestamp": items[0].get('analysis_timestamp') if items else None
    }