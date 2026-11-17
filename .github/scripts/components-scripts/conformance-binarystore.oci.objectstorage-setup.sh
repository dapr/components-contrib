#!/bin/sh

set -e

: "${OCIConfigFile:?OCIConfigFile must point to a readable OCI configuration file}"
: "${OCICompartmentOCID:?OCICompartmentOCID must identify the test compartment}"
: "${UNIQUE_ID:?UNIQUE_ID must identify the workflow run}"

BUCKET_NAME="dapr-binarystore-conformance-$UNIQUE_ID"

oci --config-file "$OCIConfigFile" --profile "${OCIConfigProfile:-DEFAULT}" \
  os bucket create \
  --compartment-id "$OCICompartmentOCID" \
  --name "$BUCKET_NAME" \
  --public-access-type NoPublicAccess \
  --versioning Disabled \
  --freeform-tags "{\"Purpose\":\"conformance-testing\",\"Timestamp\":\"$CURRENT_TIME\"}"

echo "OCIBucketName=$BUCKET_NAME" >> "$GITHUB_ENV"
