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
    description = "Timestamp of the github worklow run."
}

variable "UNIQUE_ID" {
    type        = string
    description = "Unique Id of the github worklow run."
}

provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      Purpose  = "AutomatedTesting"
      Timestamp = "${var.TIMESTAMP}"
    }
  }
}

resource "aws_s3_bucket" "dapr_bucket" {
  bucket = "dapr-cert-test-${var.UNIQUE_ID}"
  force_destroy = true
  tags = {
    dapr-topic-name = "dapr-cert-test-${var.UNIQUE_ID}"
  }
}

resource "aws_s3_bucket_acl" "dapr_bucket_acl" {
  bucket = aws_s3_bucket.dapr_bucket.id
  acl    = "private"
}
