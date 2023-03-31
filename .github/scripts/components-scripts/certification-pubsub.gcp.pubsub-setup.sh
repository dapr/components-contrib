#!/bin/sh

set -e

# Set variables for GitHub Actions
echo "GCP_PROJECT_ID=$GCP_PROJECT" >> $GITHUB_ENV
echo "PUBSUB_GCP_CONSUMER_ID_1=gcpps-ct-t1-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_CONSUMER_ID_2=gcpps-ct-t2-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_CONSUMER_ID_FIFO=gcpps-ct-fifo-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_PUBSUB_TOPIC_DLIN=gcpps-ct-dlin-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_PUBSUB_TOPIC_DLOUT=gcpps-ct-dlout-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_TOPIC_EXISTS=gcpps-ct-tp-exists-$UNIQUE_ID" >> $GITHUB_ENV
echo "PUBSUB_GCP_CONSUMER_ID_EXISTS=sub" >> $GITHUB_ENV

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/certification/pubsub/gcp/pubsub"

# Run Terraform
terraform init
terraform validate -no-color
terraform plan -no-color -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME" -var="GCP_PROJECT_ID=$GCP_PROJECT"
terraform apply -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME" -var="GCP_PROJECT_ID=$GCP_PROJECT"
