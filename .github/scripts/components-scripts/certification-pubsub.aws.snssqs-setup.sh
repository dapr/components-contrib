#!/bin/sh

set -e

# Set variables for GitHub Actions
echo "AWS_REGION=us-east-1" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_1=sqssnscerttest-q1-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_2=sqssnscerttest-q2-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_3=sqssnscerttest-q3-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC_3=sqssnscerttest-t3-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_MVT=sqssnscerttest-q-mvt-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC_MVT=sqssnscerttest-tp-mvt-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_DLIN=sqssnscerttest-dlq-in-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_DLOUT=sqssnscerttest-dlq-out-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC_DLIN=sqssnscerttest-dlt-in-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_FIFO=sqssnscerttest-q-fifo-$UNIQUE_ID.fifo" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC_FIFO=sqssnscerttest-t-fifo-$UNIQUE_ID.fifo" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_FIFO_GROUP_ID=sqssnscerttest-q-fifo-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_QUEUE_NODRT=sqssnscerttest-q-nodrt-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC_NODRT=sqssnscerttest-t-nodrt-$UNIQUE_ID" >> $GITHUB_ENV

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/certification/pubsub/aws/snssqs"

# Run Terraform
terraform init
terraform validate -no-color
terraform plan -no-color -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
terraform apply -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
