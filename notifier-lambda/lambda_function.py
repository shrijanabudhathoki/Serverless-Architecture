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

# -------- Helper Functions --------
def parse_json_summary(summary_text):
    """Parse JSON summary and extract readable content"""
    try:
        # Try to parse as JSON
        summary_data = json.loads(summary_text)
        
        # Extract insights, recommendations, and summary
        insights = summary_data.get("insights", [])
        recommendations = summary_data.get("recommendations", [])
        main_summary = summary_data.get("summary", "")
        
        return insights, recommendations, main_summary
    except json.JSONDecodeError:
        # If it's not JSON, return as plain text
        return [], [], summary_text

def format_executive_summary(summaries):
    """Format multiple summaries into a coherent executive summary"""
    if not summaries:
        return "No analysis data available."
    
    all_insights = []
    all_recommendations = []
    main_summaries = []
    
    for summary in summaries:
        if not summary:
            continue
            
        insights, recommendations, main_summary = parse_json_summary(summary)
        all_insights.extend(insights)
        all_recommendations.extend(recommendations)
        if main_summary and main_summary != summary:
            main_summaries.append(main_summary)
    
    # Build formatted summary
    formatted_summary = ""
    
    if main_summaries:
        formatted_summary += "Overall Health Status:\n"
        for i, summary in enumerate(main_summaries, 1):
            formatted_summary += f"{i}. {summary}\n"
        formatted_summary += "\n"
    
    if all_insights:
        formatted_summary += "Key Health Insights:\n"
        for i, insight in enumerate(set(all_insights), 1):  # Remove duplicates
            formatted_summary += f"• {insight}\n"
        formatted_summary += "\n"
    
    if all_recommendations:
        formatted_summary += "Recommended Actions:\n"
        for i, rec in enumerate(set(all_recommendations), 1):  # Remove duplicates
            formatted_summary += f"• {rec}\n"
    
    return formatted_summary if formatted_summary else "Analysis completed successfully."

def format_insights_and_recommendations(items):
    """Extract and format insights and recommendations from DynamoDB items"""
    formatted_insights = []
    formatted_recommendations = []
    
    for item in items:
        log("DEBUG", "processing_item", item_keys=list(item.keys()))
        
        # Get insights - try different possible formats
        insights = item.get("insights", [])
        if isinstance(insights, str):
            try:
                insights = json.loads(insights)
            except:
                insights = [insights] if insights else []
        
        if insights and insights != ["No major trends observed"]:
            formatted_insights.extend(insights)
            log("DEBUG", "found_insights", count=len(insights))
        
        # Get recommendations - try different possible formats
        recommendations = item.get("recommendations", [])  
        if isinstance(recommendations, str):
            try:
                recommendations = json.loads(recommendations)
            except:
                recommendations = [recommendations] if recommendations else []
                
        if recommendations and recommendations != ["No recommendations"]:
            formatted_recommendations.extend(recommendations)
            log("DEBUG", "found_recommendations", count=len(recommendations))
        
        # Also try to parse from summary field
        summary = item.get("summary", "")
        if summary:
            log("DEBUG", "parsing_summary", summary_length=len(summary))
            insights_from_summary, recs_from_summary, _ = parse_json_summary(summary)
            if insights_from_summary:
                formatted_insights.extend(insights_from_summary)
                log("DEBUG", "extracted_insights_from_summary", count=len(insights_from_summary))
            if recs_from_summary:
                formatted_recommendations.extend(recs_from_summary)
                log("DEBUG", "extracted_recs_from_summary", count=len(recs_from_summary))
    
    # Remove duplicates while preserving order
    unique_insights = list(dict.fromkeys(formatted_insights))
    unique_recommendations = list(dict.fromkeys(formatted_recommendations))
    
    log("INFO", "insights_and_recs_processed", 
        total_insights=len(unique_insights), 
        total_recommendations=len(unique_recommendations))
    
    return unique_insights, unique_recommendations

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

    # Sort anomalies by frequency (most common first)
    sorted_anomalies = sorted(top_anomalies.items(), key=lambda x: x[1], reverse=True)

    # Extract and format insights and recommendations
    formatted_insights, formatted_recommendations = format_insights_and_recommendations(items)
    
    # Get summaries and format executive summary
    summaries = [item.get("summary") for item in items if item.get("summary")]
    executive_summary = format_executive_summary(summaries)

    # Email content
    subject = f"Health Data Analysis Report - {datetime.utcnow().strftime('%B %d, %Y')}"
    
    # Format anomalies for text version
    anomalies_text = "\n".join([f"  • {anomaly}: {count} occurrences" for anomaly, count in sorted_anomalies[:10]]) or "  • No anomalies detected"
    
    # Format insights for text version
    insights_text = "\n".join([f"  • {insight}" for insight in formatted_insights]) or "  • No significant trends identified"
    
    # Format recommendations for text version
    recommendations_text = "\n".join([f"  • {rec}" for rec in formatted_recommendations]) or "  • Continue regular health monitoring"

    body_text = f"""Health Data Analysis Report
Generated on: {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p UTC')}

=== OVERVIEW ===
Total Health Records Processed: {total_rows:,}
Total Anomalies Detected: {total_anomalies:,}

=== TOP HEALTH ANOMALIES ===
{anomalies_text}

=== KEY INSIGHTS ===
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

    insights_html = "".join([f"<li>{insight}</li>" for insight in formatted_insights]) or "<li style='color: #7F8C8D;'>No significant trends identified</li>"
    
    recommendations_html = "".join([f"<li>{rec}</li>" for rec in formatted_recommendations]) or "<li style='color: #7F8C8D;'>Continue regular health monitoring</li>"

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

    send_email(subject, body_text, body_html)

    return {
        "status": "success", 
        "total_rows": total_rows, 
        "total_anomalies": total_anomalies,
        "report_generated": datetime.utcnow().isoformat() + "Z"
    }