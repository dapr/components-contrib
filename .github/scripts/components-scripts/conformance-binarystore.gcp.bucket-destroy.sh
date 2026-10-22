#!/bin/sh

set -e

cd ".github/infrastructure/terraform/conformance/binarystore/gcp/bucket"

terraform destroy -auto-approve -var="GCP_PROJECT_ID=$GCP_PROJECT" -var="UNIQUE_ID=$UNIQUE_ID" -var="TIMESTAMP=$CURRENT_TIME"
