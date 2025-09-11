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

# IAM Role and Policy for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "lambda_s3_role"
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
  name   = "lambda_s3_policy"
  role   = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject","s3:PutObject", "s3:ListBucket"]
        Resource = ["arn:aws:s3:::health-data-bucket-shrijana/*"]
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

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.input_bucket.bucket
      RAW_PREFIX  = "raw/"
      PROCESSED_PREFIX = "processed/"
      REJECTED_PREFIX = "rejected/"
      MARKER_PREFIX = "markers/"
    }
  }
}

# Permissions for S3 to invoke Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_ingestor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.input_bucket.arn
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

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.input_bucket.bucket
      PROCESSED_PREFIX = "processed/"
      ANALYSIS_PREFIX = "analyzed/"
      MARKER_PREFIX = "markers/"
      DDB_TABLE         = aws_dynamodb_table.analysis_table.name
      BEDROCK_MODEL_ID  = "amazon.nova-lite-v1:0" 
    }
  }
}

# Permissions for S3 to invoke Lambda
resource "aws_lambda_permission" "allow_s3_analyzer" {
  statement_id  = "AllowExecutionFromS3Analyzer"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_analyzer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.input_bucket.arn
}

resource "aws_s3_bucket_notification" "bucket_notifications" {
  bucket = aws_s3_bucket.input_bucket.bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.data_ingestor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.data_analyzer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "processed/"
    filter_suffix       = ".csv"
  }

  depends_on = [
    aws_lambda_function.data_ingestor,
    aws_lambda_permission.allow_s3,
    aws_lambda_function.data_analyzer,
    aws_lambda_permission.allow_s3_analyzer
  ]
}

