#!/bin/sh

set -e

# Set variables for GitHub Actions
echo "GCP_PROJECT_ID=$GCP_PROJECT" >> $GITHUB_ENV
echo "PUBSUB_GCP_TOPIC=conf-testTopic-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_TOPIC_MULTI_1=conf-multiTopic1-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_TOPIC_MULTI_2=conf-multiTopic2-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_CONSUMER_ID_FIFO=conf-fifo-$UNIQUE_ID" >> $GITHUB_ENV

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/conformance/pubsub/gcp/pubsub"

# Run Terraform
terraform init
terraform validate -no-color
terraform plan -no-color -var="GCP_PROJECT_ID=$GCP_PROJECT" -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
terraform apply -auto-approve -var="GCP_PROJECT_ID=$GCP_PROJECT" -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
