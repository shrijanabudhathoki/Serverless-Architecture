resource "aws_ses_email_identity" "notifier_email" {
  email = var.email_address
}