terraform {
  required_version = ">=0.13"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

variable "TIMESTAMP" {
  type        = string
  description = "Timestamp of the GitHub workflow run."
}

variable "UNIQUE_ID" {
  type        = string
  description = "Unique ID of the GitHub workflow run."
}

provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      Purpose   = "AutomatedConformanceTesting"
      Timestamp = "${var.TIMESTAMP}"
    }
  }
}

# Create the first secret in AWS Secrets Manager
resource "aws_secretsmanager_secret" "conftestsecret" {
  name = "conftestsecret-${var.UNIQUE_ID}"
  description = "Secret for conformance test"
}

resource "aws_secretsmanager_secret_version" "conftestsecret_value" {
  secret_id     = aws_secretsmanager_secret.conftestsecret.id
  secret_string = "abcd" # Value for the secret
}

# Create the second secret in AWS Secrets Manager
resource "aws_secretsmanager_secret" "secondsecret" {
  name = "secondsecret-${var.UNIQUE_ID}"
  description = "Another secret for conformance test"
}

resource "aws_secretsmanager_secret_version" "secondsecret_value" {
  secret_id     = aws_secretsmanager_secret.secondsecret.id
  secret_string = "efgh" # Value for the secret
}
