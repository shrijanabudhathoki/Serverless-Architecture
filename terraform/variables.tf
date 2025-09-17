variable "project_name" {
  description = "Project name for resource tagging"
  type        = string
  default     = "Serverless Architecture"
}

variable "bucket_name" {
  description = "S3 bucket name for health data"
  type        = string
  default     = "health-data-bucket-shrijana"
}

variable "email_address" {
  description = "Email address for notifications"
  type        = string
  default     = "shrijanabudhathoki51@gmail.com"
}

variable "email_sender" {
  description = "Sender email address"
  type        = string
  default     = "shrijanabudhathoki51@gmail.com"
}

variable "email_recipients" {
  description = "Recipient email address"
  type        = string
  default     = "shrijanabudhathoki51@gmail.com"
}

variable "bedrock_model_id" {
  description = "Bedrock model ID for analysis"
  type        = string
  default     = "amazon.nova-lite-v1:0"
}