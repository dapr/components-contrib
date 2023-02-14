#!/bin/sh

# Navigate to the Terraform directory
cd ".github/infrastructure/terraform/certification/pubsub/aws/snssqs"

# Run Terraform
terraform destroy -auto-approve -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
