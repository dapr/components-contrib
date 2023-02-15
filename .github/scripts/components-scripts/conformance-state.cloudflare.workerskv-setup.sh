#!/bin/sh

set -e

# Remove dashes from UNIQUE_ID
Suffix=$(echo "$UNIQUE_ID" | sed -E 's/-//g')
CloudflareWorkerName="daprconfkv${Suffix}"
CloudflareKVNamespaceID=$( curl -s -X POST "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
  -H "Content-Type: application/json" \
  --data "{\"title\":\"${CloudflareWorkerName}\"}" \
    | jq -r ".result.id" )

echo "CloudflareWorkerName=${CloudflareWorkerName}" >> $GITHUB_ENV
echo "CloudflareAPIToken=${CLOUDFLARE_API_TOKEN}" >> $GITHUB_ENV
echo "CloudflareAccountID=${CLOUDFLARE_ACCOUNT_ID}" >> $GITHUB_ENV
echo "CloudflareKVNamespaceID=${CloudflareKVNamespaceID}" >> $GITHUB_ENV
