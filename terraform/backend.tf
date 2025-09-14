terraform {
  backend "s3" {
    bucket         = "health-terraform-state-s3"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    use_lockfile   = true
  }
}