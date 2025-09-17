# DynamoDB Table for analysis results
resource "aws_dynamodb_table" "analysis_table" {
  name         = "health_analysis"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "correlation_id"
  range_key = "analysis_timestamp"

  attribute {
    name = "correlation_id"
    type = "S"
  }

  attribute {
    name = "analysis_timestamp"
    type = "S"
  }

  tags = {
    Project = var.project_name
  }
}