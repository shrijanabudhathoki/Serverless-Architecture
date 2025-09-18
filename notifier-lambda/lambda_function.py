# Notifier Lambda Function
import boto3
import os
import json
from datetime import datetime
from boto3.dynamodb.conditions import Key
import time
from decimal import Decimal

# CONFIG   
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
s3 = boto3.client("s3")

DDB_TABLE = os.environ.get("DDB_TABLE", "health_analysis")
SES_SENDER = os.environ.get("SES_SENDER")
SES_RECIPIENTS = os.environ.get("SES_RECIPIENTS", "").split(",")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/")

# Logging   
def log(level, message, **kwargs):

    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message
    }
    payload.update(kwargs)
    safe_payload = convert(payload)
    print(json.dumps(safe_payload))

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

# Helper Functions for Row Counts   
def fetch_manifest_data(correlation_ids):
    """Fetch processing statistics from manifest files stored in S3"""
    processing_stats = {
        "total_input": 0,
        "total_valid": 0, 
        "total_rejected": 0,
        "files_processed": 0,
        "manifests_found": []
    }
    
    if not BUCKET_NAME:
        log("WARN", "bucket_name_not_configured", 
            note="Set BUCKET_NAME environment variable to enable processing statistics")
        return processing_stats
    
    for correlation_id in correlation_ids:
        try:
            # Extract original filename from correlation_id format: bucket/key@version
            if "/" in correlation_id and "@" in correlation_id:
                # Parse correlation_id: health-data-bucket-shrijana/raw/health_4a87f4fd83b34de5912885e0e7536c6a.csv@nrJlEYaQQCfBYFinSvQPG9bX7apq9mAJ
                parts = correlation_id.split("/")
                if len(parts) >= 3:  # bucket/prefix/filename@version
                    filename_with_version = parts[-1]  # health_4a87f4fd83b34de5912885e0e7536c6a.csv@nrJlEYaQQCfBYFinSvQPG9bX7apq9mAJ
                    base_name = filename_with_version.split("@")[0]  # health_4a87f4fd83b34de5912885e0e7536c6a.csv
                    manifest_key = f"{PROCESSED_PREFIX}{base_name.replace('.csv','')}_manifest.json"
                
                try:
                    obj = s3.get_object(Bucket=BUCKET_NAME, Key=manifest_key)
                    manifest_content = obj["Body"].read().decode("utf-8")
                    manifest = json.loads(manifest_content)
                    
                    counts = manifest.get("counts", {})
                    processing_stats["total_input"] += counts.get("input", 0)
                    processing_stats["total_valid"] += counts.get("valid", 0)
                    processing_stats["total_rejected"] += counts.get("rejected", 0)
                    processing_stats["files_processed"] += 1
                    processing_stats["manifests_found"].append({
                        "correlation_id": correlation_id,
                        "manifest_key": manifest_key,
                        "counts": counts
                    })
                    
                    log("INFO", "manifest_processed", 
                        correlation_id=correlation_id,
                        input_rows=counts.get("input", 0),
                        valid_rows=counts.get("valid", 0),
                        rejected_rows=counts.get("rejected", 0))
                        
                except s3.exceptions.NoSuchKey:
                    log("WARN", "manifest_not_found", 
                        correlation_id=correlation_id,
                        manifest_key=manifest_key)
                except Exception as e:
                    log("ERROR", "manifest_fetch_failed", 
                        correlation_id=correlation_id,
                        manifest_key=manifest_key,
                        error=str(e))
            else:
                log("WARN", "invalid_correlation_id_format", correlation_id=correlation_id)
                
        except Exception as e:
            log("ERROR", "processing_correlation_id_failed", 
                correlation_id=correlation_id, 
                error=str(e))
    
    log("INFO", "processing_stats_aggregated", 
        total_input=processing_stats["total_input"],
        total_valid=processing_stats["total_valid"],
        total_rejected=processing_stats["total_rejected"],
        files_processed=processing_stats["files_processed"])
    
    return processing_stats

# DynamoDB Retrieval   
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

# Helper Functions   
def extract_insights_and_recommendations(items):
    """Extract insights and recommendations from the most recent DynamoDB item only"""
    if not items:
        return [], []
    
    # Only use the most recent item (first item after sorting)
    most_recent_item = items[0]
    
    # Extract insights from most recent analysis only
    insights = most_recent_item.get("insights", [])
    
    # Extract recommendations from most recent analysis only
    recommendations = most_recent_item.get("recommendations", [])
    
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

# SES Email   

def send_email(subject, body_text, body_html, retries=3, delay=2):
    if not SES_SENDER or not SES_RECIPIENTS:
        log("ERROR", "SES_not_configured")
        return False

    recipients = [r.strip() for r in SES_RECIPIENTS if r.strip()]

    for attempt in range(retries):
        try:
            ses.send_email(
                Source=SES_SENDER,
                Destination={"ToAddresses": recipients},
                Message={
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": body_text, "Charset": "UTF-8"},
                        "Html": {"Data": body_html, "Charset": "UTF-8"},
                    },
                }
            )
            log("INFO", "email_sent", subject=subject, recipients=len(recipients))
            return True
        except Exception as e:
            log("ERROR", "email_failed", error=str(e), attempt=attempt + 1)
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))  # exponential backoff
            else:
                return False

        

# Lambda Handler   
def lambda_handler(event, context):
    """
    Updated handler to properly handle EventBridge events from the analyzer
    """
    
    # DEBUG: Log the incoming event structure
    log("DEBUG", "notifier_event_received", 
        event_keys=list(event.keys()),
        event_source=event.get("source"),
        detail_type=event.get("detail-type"))
    
    # Handle EventBridge event format from analyzer
    correlation_id = None
    
    if "detail" in event and event.get("source") == "health.data.analyzer":
        # EventBridge event from analyzer
        correlation_id = event["detail"].get("correlation_id")
        event_type = "eventbridge"
        log("INFO", "received_eventbridge_event", 
            correlation_id=correlation_id,
            event_source=event.get("source"),
            detail_type=event.get("detail-type"))
    elif "correlation_id" in event:
        # Direct invocation or manual test
        correlation_id = event.get("correlation_id")
        event_type = "direct"
        log("INFO", "received_direct_event", correlation_id=correlation_id)
    else:
        # Fallback - no correlation_id provided
        event_type = "fallback"
        log("INFO", "received_fallback_event", 
            message="No correlation_id found, processing recent analyses")
    
    if correlation_id:
        # Process specific file only
        items = fetch_recent_analysis(correlation_id=correlation_id, limit=1)
        correlation_ids = [correlation_id]
        scope_description = "Current file only"
        
        log("INFO", "processing_specific_correlation", 
            correlation_id=correlation_id,
            items_found=len(items),
            event_type=event_type)
    else:
        # Process recent files (fallback for manual triggers)
        items = fetch_recent_analysis(correlation_id=None, limit=5)
        correlation_ids = [item.get("correlation_id") for item in items if item.get("correlation_id")]
        scope_description = f"Last {len(correlation_ids)} processed files"
        
        log("INFO", "processing_recent_analyses_fallback", 
            items_found=len(items),
            correlation_ids_count=len(correlation_ids),
            event_type=event_type)

    # Validation check
    if not items:
        log("INFO", "no_analysis_found", 
            correlation_id=correlation_id,
            event_type=event_type)
        return {"status": "no_data", "message": "No analysis data found"}

    # Log processing scope
    log("INFO", "processing_recent_items", 
        item_count=len(items),
        scope=scope_description,
        event_type=event_type,
        newest_timestamp=items[0].get('analysis_timestamp') if items else None,
        oldest_timestamp=items[-1].get('analysis_timestamp') if items else None)

    # Get correlation IDs for fetching processing statistics
    processing_stats = fetch_manifest_data(correlation_ids)

    # Aggregate row counts and anomalies from analysis results
    total_analyzed_rows = sum(item.get("records_analyzed", 0) for item in items)
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
        scope=scope_description,
        event_type=event_type,
        total_input_rows=processing_stats["total_input"],
        total_valid_rows=processing_stats["total_valid"], 
        total_rejected_rows=processing_stats["total_rejected"],
        total_analyzed_rows=total_analyzed_rows,
        total_anomalies=total_anomalies,
        insights_count=len(insights),
        recommendations_count=len(recommendations),
        anomaly_types=len(sorted_anomalies))

    # Email content - updated subject to indicate scope
    subject = f"Health Data Analysis Report - {datetime.utcnow().strftime('%B %d, %Y')}"
    
    # Calculate data quality percentage
    data_quality_pct = (processing_stats["total_valid"] / processing_stats["total_input"] * 100) if processing_stats["total_input"] > 0 else 100

    # Format content for email
    anomalies_text = "\n".join([f"  • {anomaly}: {count} occurrences" for anomaly, count in sorted_anomalies[:10]]) or "  • No anomalies detected"
    insights_text = "\n".join([f"  • {insight}" for insight in insights[:10]]) or "  • Continuing to monitor health patterns"
    recommendations_text = "\n".join([f"  • {rec}" for rec in recommendations[:10]]) or "  • Continue regular health monitoring"

    body_text = f"""Health Data Analysis Report
Generated on: {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p UTC')}
Event Type: {event_type.title()}

=== DATA PROCESSING OVERVIEW ===
Total Raw Records (this scope): {processing_stats['total_input']:,}
Valid Records: {processing_stats['total_valid']:,}
Rejected Records: {processing_stats['total_rejected']:,}

=== ANALYSIS OVERVIEW ===
Records Analyzed: {total_analyzed_rows:,}
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

    # Determine quality status color
    quality_color = "#27AE60" if data_quality_pct >= 95 else "#F39C12" if data_quality_pct >= 85 else "#E74C3C"

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
                flex-wrap: wrap;
            }}
            .metric {{ 
                text-align: center; 
                margin: 10px 15px;
                min-width: 100px;
            }}
            .metric-number {{ 
                font-size: 24px; 
                font-weight: bold; 
                display: block; 
            }}
            .metric-label {{ 
                font-size: 11px; 
                opacity: 0.9; 
            }}
            .data-quality-section {{
                background: linear-gradient(135deg, #a8e6cf 0%, #7fcdcd 100%);
                padding: 20px;
                border-radius: 8px;
                margin: 20px 0;
                color: #2c3e50;
            }}
            .quality-metric {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin: 8px 0;
            }}
            .quality-percentage {{
                font-size: 24px;
                font-weight: bold;
                color: {quality_color};
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
                <div class="data-quality-section">
                    <h3 style="margin-top: 0; color: #2c3e50;">📊 Data Processing Summary</h3>
                    <div class="quality-metric">
                        <span><strong>Raw Records:</strong> {processing_stats['total_input']:,}</span>
                    </div>
                    <div class="quality-metric">
                        <span><strong>Valid Records:</strong> {processing_stats['total_valid']:,}</span>
                    </div>
                    <div class="quality-metric">
                        <span><strong>Rejected Records:</strong> {processing_stats['total_rejected']:,}</span>
                    </div>
                </div>

                <div class="metrics">
                    <div class="metric">
                        <span class="metric-number">{total_analyzed_rows:,}</span>
                        <span class="metric-label">Records Analyzed</span>
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
                    Key={"correlation_id": item["correlation_id"], "analysis_timestamp": item["analysis_timestamp"]},
                    UpdateExpression="SET notification_sent = :sent, notification_timestamp = :ts",
                    ExpressionAttributeValues={
                        ":sent": True,
                        ":ts": datetime.utcnow().isoformat() + "Z"
                    }
                )
            except Exception as e:
                log("WARN", "failed_to_update_notification_status", 
                    correlation_id=item.get("correlation_id"), 
                    analysis_id=item.get("analysis_id"),
                    error=str(e))

    return {
        "status": "success" if email_sent else "failed",
        "event_type": event_type,
        "scope": scope_description,
        "processing_stats": processing_stats,
        "total_analyzed_rows": total_analyzed_rows,
        "total_anomalies": total_anomalies,
        "email_sent": email_sent,
        "report_generated": datetime.utcnow().isoformat() + "Z",
        "latest_analysis_timestamp": items[0].get('analysis_timestamp') if items else None
    }