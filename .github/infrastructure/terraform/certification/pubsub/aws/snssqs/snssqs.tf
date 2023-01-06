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

resource "aws_sns_topic" "multiTopic1" {
  name = "sqsssnscerttest-q1-${var.UNIQUE_ID}"
  tags = {
    dapr-topic-name = "sqsssnscerttest-q1-${var.UNIQUE_ID}"
  }
}

resource "aws_sqs_queue" "testQueue" {
  name = "testQueue-${var.UNIQUE_ID}"
  tags = {
    dapr-queue-name = "testQueue-${var.UNIQUE_ID}"
  }
}

resource "aws_sns_topic_subscription" "multiTopic1_testQueue" {
  topic_arn = aws_sns_topic.multiTopic1.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.testQueue.arn
}

resource "aws_sqs_queue_policy" "testQueue_policy" {
    queue_url = "${aws_sqs_queue.testQueue.id}"

    policy = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [{
    "Sid": "Allow-SNS-SendMessage",
    "Effect": "Allow",
    "Principal": {
      "Service": "sns.amazonaws.com"
    },
    "Action": "sqs:SendMessage",
    "Resource": "${aws_sqs_queue.testQueue.arn}",
    "Condition": {
      "ArnEquals": {
        "aws:SourceArn": [
          "${aws_sns_topic.multiTopic1.arn}"
        ]
      }
    }
  }]
}
POLICY
}
