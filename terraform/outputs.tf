output "s3_bucket_name" {
  value = aws_s3_bucket.input_bucket.bucket
}

output "eventbridge_bus_name" {
  value = aws_cloudwatch_event_bus.health_data_bus.name
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.analysis_table.name
}