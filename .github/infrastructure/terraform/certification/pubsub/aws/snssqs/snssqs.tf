terraform {
  required_version = ">=0.13"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

// ###### INPUT VARIABLES ########
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

// ###### RESOURCES ########
resource "aws_sns_topic" "existingTopic" {
  name = "sqssnscerttest-t3-${var.UNIQUE_ID}"
  tags = {
    dapr-topic-name = "sqssnscerttest-t3-${var.UNIQUE_ID}"
  }
}

resource "aws_sqs_queue" "existingQueue" {
  name = "sqssnscerttest-q3-${var.UNIQUE_ID}"
  tags = {
    dapr-queue-name = "sqssnscerttest-q3-${var.UNIQUE_ID}"
  }
}

resource "aws_sns_topic_subscription" "existingTopic_existingQueue" {
  topic_arn = aws_sns_topic.existingTopic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.existingQueue.arn
}

resource "aws_sqs_queue_policy" "existingQueue_policy" {
    queue_url = "${aws_sqs_queue.existingQueue.id}"

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
    "Resource": "${aws_sqs_queue.existingQueue.arn}",
    "Condition": {
      "ArnEquals": {
        "aws:SourceArn": [
          "${aws_sns_topic.existingTopic.arn}"
        ]
      }
    }
  }]
}
POLICY
}

// ###### OUTPUT VARIABLES ########
output "existingQueue" {
  value = aws_sqs_queue.existingQueue.name
}

output "existingTopic" {
  value = aws_sns_topic.existingTopic.name
}
