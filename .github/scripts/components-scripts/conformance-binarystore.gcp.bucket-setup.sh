#!/bin/sh

set -e

cd ".github/infrastructure/terraform/conformance/binarystore/gcp/bucket"

terraform init
terraform validate -no-color
terraform plan -no-color -var="GCP_PROJECT_ID=$GCP_PROJECT" -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
terraform apply -auto-approve -var="GCP_PROJECT_ID=$GCP_PROJECT" -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"

GCP_BUCKET=$(terraform output -raw GCPBucket)
echo "GCPBucket=$GCP_BUCKET" >> "$GITHUB_ENV"
