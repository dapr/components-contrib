#!/bin/sh

# Set variables for GitHub Actions
echo "PUBSUB_AWS_SNSSQS_QUEUE=testQueue-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC=testTopic-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC_MULTI_1=multiTopic1-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_AWS_SNSSQS_TOPIC_MULTI_2=multiTopic2-$UNIQUE_ID" >> $GITHUB_ENV

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/conformance/pubsub/aws/snssqs"

# Run Terraform
terraform init
terraform validate -no-color
terraform plan -no-color -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
terraform apply -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
