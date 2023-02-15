#!/bin/sh

set +e

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/certification/pubsub/aws/dynamodb"

# Run Terraform
terraform destroy -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
