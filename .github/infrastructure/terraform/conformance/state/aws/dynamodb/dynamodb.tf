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
      Purpose   = "AutomatedConformanceTesting"
      Timestamp = "${var.TIMESTAMP}"
    }
  }
}

resource "aws_dynamodb_table" "conformance_test_basic_table" {
  name           = "conformance-test-terraform-basic-${var.UNIQUE_ID}"
  billing_mode   = "PROVISIONED"
  read_capacity  = "10"
  write_capacity = "10"
  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }
  attribute {
    name = "key"
    type = "S"
  }
  hash_key = "key"
}

resource "aws_dynamodb_table" "conformance_test_partition_key_table" {
  name           = "conformance-test-terraform-partition-key-${var.UNIQUE_ID}"
  billing_mode   = "PROVISIONED"
  read_capacity  = "10"
  write_capacity = "10"
  attribute {
    name = "pkey"
    type = "S"
  }
  hash_key = "pkey"
}
