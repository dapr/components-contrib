#!/bin/sh

set -e

# Set variables for GitHub Actions
echo "GCP_FIRESTORE_ENTITY_KIND=CertificationTestEntity-$UNIQUE_ID" >> $GITHUB_ENV
