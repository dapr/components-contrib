#!/bin/sh

set -e

: "${AzureResourceGroupName:?AzureResourceGroupName must identify the test resource group}"
: "${UNIQUE_ID:?UNIQUE_ID must identify the workflow run}"

ACCOUNT_HASH=$(printf '%s' "$UNIQUE_ID" | sha256sum)
ACCOUNT_NAME="dpradls$(printf '%.16s' "$ACCOUNT_HASH")"

# Delete only this job's account, never the shared conformance resource group.
az storage account delete \
  --name "$ACCOUNT_NAME" \
  --resource-group "$AzureResourceGroupName" \
  --yes
