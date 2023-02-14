#!/bin/sh

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/certification/pubsub/aws/dynamodb"

# Run Terraform
terraform init
terraform validate -no-color
terraform plan -no-color -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
terraform apply -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"

# Set variables for GitHub Actions
echo "AWS_REGION=us-east-1" >> $GITHUB_ENV
echo "STATE_AWS_DYNAMODB_TABLE_1=certification-test-terraform-basic-$UNIQUE_ID" >> $GITHUB_ENV
echo "STATE_AWS_DYNAMODB_TABLE_2=certification-test-terraform-partition-key-$UNIQUE_ID" >> $GITHUB_ENV
