# Data ingestor Lambda Function
data "archive_file" "ingestor_lambda_zip_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../data-ingestor-lambda"
  output_path = "${path.cwd}/../data-ingestor-lambda/lambda_function.zip"
}

resource "aws_lambda_function" "data_ingestor" {
  filename         = data.archive_file.ingestor_lambda_zip_archive.output_path
  function_name    = "data-ingestor-lambda"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60
  source_code_hash = data.archive_file.ingestor_lambda_zip_archive.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME      = aws_s3_bucket.input_bucket.bucket
      RAW_PREFIX       = "raw/"
      PROCESSED_PREFIX = "processed/"
      REJECTED_PREFIX  = "rejected/"
      MARKERS_PREFIX   = "markers/"
      EVENT_BUS_NAME   = aws_cloudwatch_event_bus.health_data_bus.name
    }
  }
}

# Data analyzer Lambda Function
data "archive_file" "analyzer_lambda_zip_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../data-analyzer-lambda"
  output_path = "${path.cwd}/../data-analyzer-lambda/lambda_function.zip"
}

resource "aws_lambda_function" "data_analyzer" {
  filename         = data.archive_file.analyzer_lambda_zip_archive.output_path
  function_name    = "data-analyzer-lambda"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 300
  source_code_hash = data.archive_file.analyzer_lambda_zip_archive.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME      = aws_s3_bucket.input_bucket.bucket
      PROCESSED_PREFIX = "processed/"
      ANALYSIS_PREFIX  = "analyzed/"
      MARKERS_PREFIX   = "markers/"
      DDB_TABLE        = aws_dynamodb_table.analysis_table.name
      BEDROCK_MODEL_ID = var.bedrock_model_id
      EVENT_BUS_NAME   = aws_cloudwatch_event_bus.health_data_bus.name
    }
  }
}

# Notifier Lambda Function
data "archive_file" "notifier_lambda_zip_archive" {
  type        = "zip"
  source_dir  = "${path.module}/../notifier-lambda"
  output_path = "${path.cwd}/../notifier-lambda/lambda_function.zip"
}

resource "aws_lambda_function" "notifier_lambda" {
  filename         = data.archive_file.notifier_lambda_zip_archive.output_path
  function_name    = "health-notifier-lambda"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60
  source_code_hash = data.archive_file.notifier_lambda_zip_archive.output_base64sha256

  environment {
    variables = {
      DDB_TABLE          = aws_dynamodb_table.analysis_table.name
      SES_SENDER         = var.email_sender
      SES_RECIPIENTS     = var.email_recipients
      BEDROCK_MODEL_ID   = var.bedrock_model_id
      BEDROCK_MAX_TOKENS = "500"
    }
  }

  depends_on = [aws_iam_role_policy.notifier_policy]
}

# Lambda Permissions
# Permissions for S3 to invoke Data Ingestor Lambda
resource "aws_lambda_permission" "allow_s3_ingestor" {
  statement_id  = "AllowExecutionFromS3Ingestor"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_ingestor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.input_bucket.arn
}

# Permissions for EventBridge to invoke Data Analyzer Lambda
resource "aws_lambda_permission" "allow_eventbridge_analyzer" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_analyzer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.data_processed_rule.arn
  depends_on    = [aws_cloudwatch_event_rule.data_processed_rule]
}

# Permissions for EventBridge to invoke Notifier Lambda
resource "aws_lambda_permission" "allow_eventbridge_notifier" {
  statement_id  = "AllowExecutionFromEventBridgeNotifier"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notifier_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.analysis_complete_notifier_rule.arn
}
