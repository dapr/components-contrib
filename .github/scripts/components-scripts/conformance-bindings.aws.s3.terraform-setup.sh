#!/bin/sh

set -e

# Set variables for GitHub Actions
echo "AWS_REGION=us-east-1" >> $GITHUB_ENV
echo "BINDINGS_AWS_S3_BUCKET=dapr-conformance-test-$UNIQUE_ID" >> $GITHUB_ENV

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/conformance/bindings/aws/s3"

# Run Terraform
terraform init
terraform validate -no-color
terraform plan -no-color -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
terraform apply -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
