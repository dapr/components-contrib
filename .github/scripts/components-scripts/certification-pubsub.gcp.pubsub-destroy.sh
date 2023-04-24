#!/bin/sh

set +e

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/certification/pubsub/gcp/pubsub"

# Run Terraform
terraform destroy -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME" -var="GCP_PROJECT_ID=$GCP_PROJECT"
