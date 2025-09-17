# EventBridge Custom Bus for workflow orchestration
resource "aws_cloudwatch_event_bus" "health_data_bus" {
  name = "health-data-processing-bus"

  tags = {
    Project = var.project_name
  }
}

# EventBridge Rule to trigger data analyzer when processing is complete
resource "aws_cloudwatch_event_rule" "data_processed_rule" {
  name           = "health-data-processed"
  description    = "Trigger data analysis when data processing is complete"
  event_bus_name = aws_cloudwatch_event_bus.health_data_bus.name

  event_pattern = jsonencode({
    source      = ["health.data.ingestor"]
    detail-type = ["Data Processing Complete"]
    detail = {
      status = ["success"]
    }
  })
}

# EventBridge Target to invoke data analyzer Lambda
resource "aws_cloudwatch_event_target" "analyzer_target" {
  rule           = aws_cloudwatch_event_rule.data_processed_rule.name
  event_bus_name = aws_cloudwatch_event_bus.health_data_bus.name
  target_id      = "DataAnalyzerLambdaTarget"
  arn            = aws_lambda_function.data_analyzer.arn

  input_transformer {
    input_paths = {
      bucket         = "$.detail.bucket"
      key            = "$.detail.processed_key"
      correlation_id = "$.detail.correlation_id"
    }
    input_template = <<TEMPLATE
{
  "bucket": "<bucket>",
  "key": "<key>",
  "correlation_id": "<correlation_id>",
  "source": "eventbridge"
}
TEMPLATE


  }

  depends_on = [
    aws_lambda_function.data_analyzer,
    aws_lambda_permission.allow_eventbridge_analyzer,
    aws_cloudwatch_event_rule.data_processed_rule
  ]
}

# EventBridge Rule to capture analysis completion events
resource "aws_cloudwatch_event_rule" "analysis_complete_rule" {
  name           = "health-data-analysis-complete"
  description    = "Capture when data analysis is complete"
  event_bus_name = aws_cloudwatch_event_bus.health_data_bus.name

  event_pattern = jsonencode({
    source      = ["health.data.analyzer"]
    detail-type = ["Data Analysis Complete"]
  })
}

# EventBridge Target to log analysis completion events to CloudWatch Logs
resource "aws_cloudwatch_event_rule" "analysis_complete_notifier_rule" {
  name           = "health-data-analysis-complete-notifier"
  description    = "Trigger notifier Lambda when data analysis completes"
  event_bus_name = aws_cloudwatch_event_bus.health_data_bus.name

  event_pattern = jsonencode({
    source      = ["health.data.analyzer"]
    detail-type = ["Data Analysis Complete"]
  })
}

# EventBridge Target to invoke notifier Lambda
resource "aws_cloudwatch_event_target" "notifier_target" {
  rule           = aws_cloudwatch_event_rule.analysis_complete_notifier_rule.name
  target_id      = "NotifierLambdaTarget"
  arn            = aws_lambda_function.notifier_lambda.arn
  event_bus_name = aws_cloudwatch_event_bus.health_data_bus.name
}