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

// ## Existing Topic ##
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

// ## Message Visibility Timeout Topic ##
resource "aws_sns_topic" "messageVisibilityTimeoutTopic" {
  name = "sqssnscerttest-tp-mvt-${var.UNIQUE_ID}"
  tags = {
    dapr-topic-name = "sqssnscerttest-tp-mvt-${var.UNIQUE_ID}"
  }
}

resource "aws_sqs_queue" "messageVisibilityTimeoutQueue" {
  name = "sqssnscerttest-q-mvt-${var.UNIQUE_ID}"
  tags = {
    dapr-queue-name = "sqssnscerttest-q-mvt-${var.UNIQUE_ID}"
  }
}

resource "aws_sns_topic_subscription" "messageVisibilityTimeoutTopic_messageVisibilityTimeoutQueue" {
  topic_arn = aws_sns_topic.messageVisibilityTimeoutTopic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.messageVisibilityTimeoutQueue.arn
}

resource "aws_sqs_queue_policy" "messageVisibilityTimeoutQueue_policy" {
    queue_url = "${aws_sqs_queue.messageVisibilityTimeoutQueue.id}"

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
    "Resource": "${aws_sqs_queue.messageVisibilityTimeoutQueue.arn}",
    "Condition": {
      "ArnEquals": {
        "aws:SourceArn": [
          "${aws_sns_topic.messageVisibilityTimeoutTopic.arn}"
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
output "existingTopic_existingQueue_subscription" {
  value = aws_sns_topic_subscription.existingTopic_existingQueue.id
}

output "messageVisibilityTimeoutQueue" {
  value = aws_sqs_queue.messageVisibilityTimeoutQueue.name
}
output "messageVisibilityTimeoutTopic" {
  value = aws_sns_topic.messageVisibilityTimeoutTopic.name
}
output "messageVisibilityTimeoutTopic_messageVisibilityTimeoutQueue_subscription" {
  value = aws_sns_topic_subscription.messageVisibilityTimeoutTopic_messageVisibilityTimeoutQueue.id
}