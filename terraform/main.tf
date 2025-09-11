provider "aws" {
  region = "us-east-1"
}

# S3 Bucket to store raw data
resource "aws_s3_bucket" "input_bucket"{
    bucket = "health-data-bucket-shrijana"

    tags = {
        Name = "Shrijana"
        Project = "Serverless Architecture"
    }
}

resource "aws_s3_bucket_versioning" "input_bucket_versioning" {
  bucket = aws_s3_bucket.input_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
  depends_on = [aws_s3_bucket.input_bucket]
}

# DynamoDB Table for analysis results
resource "aws_dynamodb_table" "analysis_table" {
  name           = "health_analysis"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "correlation_id"

  attribute {
    name = "correlation_id"
    type = "S"
  }

  tags = {
    Project = "Serverless Architecture"
  }
}

# EventBridge Custom Bus for workflow orchestration
resource "aws_cloudwatch_event_bus" "health_data_bus" {
  name = "health-data-processing-bus"
  
  tags = {
    Project = "Serverless Architecture"
  }
}

# IAM Role and Policy for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "lambda_s3_eventbridge_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name   = "lambda_s3_eventbridge_policy"
  role   = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject","s3:PutObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::health-data-bucket-shrijana",
          "arn:aws:s3:::health-data-bucket-shrijana/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem","dynamodb:UpdateItem","dynamodb:GetItem","dynamodb:Scan"]
        Resource = aws_dynamodb_table.analysis_table.arn
      },
      {
        Effect   = "Allow"
        Action   = [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream"
        ]
        Resource = "arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-lite-v1:0"
      },
      {
        Effect   = "Allow"
        Action   = [
          "events:PutEvents"
        ]
        Resource = [
          aws_cloudwatch_event_bus.health_data_bus.arn,
          "arn:aws:events:us-east-1:*:event-bus/default"
        ]
      }
    ]
  })
}

# Data ingestor Lambda Function
data "archive_file" "ingestor_lambda_zip_archive" {
  type        = "zip"
  source_dir = "${path.module}/../data-ingestor-lambda"
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
      BUCKET_NAME = aws_s3_bucket.input_bucket.bucket
      RAW_PREFIX  = "raw/"
      PROCESSED_PREFIX = "processed/"
      REJECTED_PREFIX = "rejected/"
      MARKERS_PREFIX = "markers/"
      EVENT_BUS_NAME = aws_cloudwatch_event_bus.health_data_bus.name
    }
  }
}

# Data analyzer Lambda Function
data "archive_file" "analyzer_lambda_zip_archive" {
  type        = "zip"
  source_dir = "${path.module}/../data-analyzer-lambda"
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
      BUCKET_NAME = aws_s3_bucket.input_bucket.bucket
      PROCESSED_PREFIX = "processed/"
      ANALYSIS_PREFIX = "analyzed/"
      MARKERS_PREFIX = "markers/"
      DDB_TABLE         = aws_dynamodb_table.analysis_table.name
      BEDROCK_MODEL_ID  = "amazon.nova-lite-v1:0"
      EVENT_BUS_NAME = aws_cloudwatch_event_bus.health_data_bus.name
    }
  }
}

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
  depends_on = [aws_cloudwatch_event_rule.data_processed_rule]
}

# S3 bucket notification for raw data ingestion
resource "aws_s3_bucket_notification" "bucket_notifications" {
  bucket = aws_s3_bucket.input_bucket.bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.data_ingestor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }

  depends_on = [
    aws_lambda_function.data_ingestor,
    aws_lambda_permission.allow_s3_ingestor
  ]
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
      bucket = "$.detail.bucket"
      key    = "$.detail.processed_key"
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

# CloudWatch Log Group for EventBridge (optional but useful for debugging)
resource "aws_cloudwatch_log_group" "eventbridge_logs" {
  name              = "/aws/events/health-data-processing"
  retention_in_days = 7
}

# Outputs for reference
output "s3_bucket_name" {
  value = aws_s3_bucket.input_bucket.bucket
}

output "eventbridge_bus_name" {
  value = aws_cloudwatch_event_bus.health_data_bus.name
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.analysis_table.name
}