# CloudWatch Log Group for EventBridge
resource "aws_cloudwatch_log_group" "eventbridge_logs" {
  name              = "/aws/events/health-data-processing"
  retention_in_days = 7
}

# SNS Topic for Notification
resource "aws_sns_topic" "lambda_alerts" {
  name = "LambdaFailureAlerts"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.lambda_alerts.arn
  protocol  = "email"
  endpoint  = var.email_address
}

# Ingestor Lambda Errors
resource "aws_cloudwatch_metric_alarm" "ingestor_failure_alarm" {
  alarm_name          = "IngestorLambdaFailure"
  alarm_description   = "Triggers when the ingestor Lambda fails"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.lambda_alerts.arn]
  dimensions = {
    FunctionName = aws_lambda_function.data_ingestor.function_name
  }
}

# Analyzer Lambda Errors
resource "aws_cloudwatch_metric_alarm" "analyzer_failure_alarm" {
  alarm_name          = "AnalyzerLambdaFailure"
  alarm_description   = "Triggers when the analyzer Lambda fails"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.lambda_alerts.arn]
  dimensions = {
    FunctionName = aws_lambda_function.data_analyzer.function_name
  }
}

# Notifier Lambda Errors
resource "aws_cloudwatch_metric_alarm" "notifier_failure_alarm" {
  alarm_name          = "NotifierLambdaFailure"
  alarm_description   = "Triggers when the notifier Lambda fails"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.lambda_alerts.arn]
  dimensions = {
    FunctionName = aws_lambda_function.notifier_lambda.function_name
  }
}