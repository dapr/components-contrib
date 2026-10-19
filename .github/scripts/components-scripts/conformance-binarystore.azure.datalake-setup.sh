#!/bin/sh

set -e

: "${AzureResourceGroupName:?AzureResourceGroupName must identify the test resource group}"
: "${UNIQUE_ID:?UNIQUE_ID must identify the workflow run}"

# Storage account names must be globally unique, alphanumeric, and <= 24 characters.
ACCOUNT_HASH=$(printf '%s' "$UNIQUE_ID" | sha256sum)
ACCOUNT_NAME="dpradls$(printf '%.16s' "$ACCOUNT_HASH")"
LOCATION=$(az group show --name "$AzureResourceGroupName" --query location --output tsv)

az storage account create \
  --name "$ACCOUNT_NAME" \
  --resource-group "$AzureResourceGroupName" \
  --location "$LOCATION" \
  --kind StorageV2 \
  --sku Standard_LRS \
  --enable-hierarchical-namespace true \
  --allow-blob-public-access false \
  --min-tls-version TLS1_2 \
  --tags Purpose=conformance-testing Timestamp="$CURRENT_TIME" \
  --output none

ACCOUNT_KEY=$(az storage account keys list \
  --account-name "$ACCOUNT_NAME" \
  --resource-group "$AzureResourceGroupName" \
  --query '[0].value' --output tsv)
if [ -z "$ACCOUNT_KEY" ]; then
  echo "::error::Azure returned no access key for the ADLS test account" >&2
  exit 1
fi
echo "::add-mask::$ACCOUNT_KEY"

az storage fs create \
  --name binarystore-conf-datalake \
  --account-name "$ACCOUNT_NAME" \
  --account-key "$ACCOUNT_KEY" \
  --auth-mode key \
  --output none

{
  echo "AzureDataLakeStorageAccount=$ACCOUNT_NAME"
  echo "AzureDataLakeStorageAccessKey=$ACCOUNT_KEY"
} >> "$GITHUB_ENV"
