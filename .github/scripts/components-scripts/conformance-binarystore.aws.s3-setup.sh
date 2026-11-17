#!/bin/sh

set -e

# Keep the bucket separate from the S3 binding conformance job in the same run.
export UNIQUE_ID="binarystore-$UNIQUE_ID"

.github/scripts/components-scripts/conformance-bindings.aws.s3.terraform-setup.sh

echo "AWSS3Bucket=dapr-conformance-test-$UNIQUE_ID" >> "$GITHUB_ENV"
