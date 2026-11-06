#!/bin/sh

set -e

: "${OCIConfigFile:?OCIConfigFile must point to a readable OCI configuration file}"
: "${UNIQUE_ID:?UNIQUE_ID must identify the workflow run}"

# Only delete this job's isolated bucket, including leftover multipart uploads.
oci --config-file "$OCIConfigFile" --profile "${OCIConfigProfile:-DEFAULT}" \
  os bucket delete \
  --bucket-name "dapr-binarystore-conformance-$UNIQUE_ID" \
  --empty --force
