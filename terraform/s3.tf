# S3 Bucket to store raw data
resource "aws_s3_bucket" "input_bucket" {
  bucket = var.bucket_name

  tags = {
    Name    = "Shrijana"
    Project = var.project_name
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "s3_bucket_encryption" {
  bucket = aws_s3_bucket.input_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "s3_lifecycle" {
  bucket = aws_s3_bucket.input_bucket.id

  rule {
    id = "delete_raw_data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    expiration {
      days = 30
    }
  }

  rule {
    id = "delete_rejected_data"
    status = "Enabled"

    filter {
      prefix = "rejected/"
    }

    expiration {
      days = 30
    }

  }

  rule {
    id = "processed_data_to_glacier"
    status = "Enabled"

    filter {
      prefix = "processed/"
    }

    transition {
      days          = 30
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }

  rule {
    id = "analyzed_data_to_glacier"
    status = "Enabled"

    filter {
      prefix = "analyzed/"
    }

    transition {
      days          = 30
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }

  }
}

resource "aws_s3_bucket_versioning" "input_bucket_versioning" {
  bucket = aws_s3_bucket.input_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
  depends_on = [aws_s3_bucket.input_bucket]
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