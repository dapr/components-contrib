variable "UUID" {
    type        = string
    description = "This is an example input variable using env variables."
}

resource "aws_sns_topic" "testTopic" {
  name = "testTopic"
  tags = {
    dapr-topic-name = "testTopic"
  }
}

resource "aws_sns_topic" "multiTopic1" {
  name = "multiTopic1"
  tags = {
    dapr-topic-name = "multiTopic1"
  }
}

resource "aws_sns_topic" "multiTopic2" {
  name = "multiTopic2"
  tags = {
    dapr-topic-name = "multiTopic2"
  }
}

resource "aws_sqs_queue" "testQueue" {
  name = "testQueue"
  tags = {
    dapr-queue-name = "testQueue"
  }
}

resource "aws_sns_topic_subscription" "multiTopic1_testQueue" {
  topic_arn = aws_sns_topic.multiTopic1.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.testQueue.arn
}

resource "aws_sns_topic_subscription" "multiTopic2_testQueue" {
  topic_arn = aws_sns_topic.multiTopic2.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.testQueue.arn
}

resource "aws_sns_topic_subscription" "testTopic_testQueue" {
  topic_arn = aws_sns_topic.testTopic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.testQueue.arn
}

resource "aws_sqs_queue_policy" "testQueue_policy" {
    queue_url = "${aws_sqs_queue.testQueue.id}"

    policy = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [
    {
      "Sid": "First",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "${aws_sqs_queue.testQueue.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_sns_topic.testTopic.arn}"
        }
      }
    }, 
    {
      "Sid": "First",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "${aws_sqs_queue.testQueue.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_sns_topic.multiTopic1.arn}"
        }
      }
    },
    {
      "Sid": "First",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "${aws_sqs_queue.testQueue.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_sns_topic.multiTopic2.arn}"
        }
      }
    }
  ]
}
POLICY
}
