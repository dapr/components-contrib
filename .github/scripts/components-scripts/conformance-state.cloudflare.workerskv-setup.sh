#!/bin/sh

set -e

# Rebuild the Worker
(
  cd internal/component/cloudflare/worker-src;
  npm ci;
  npm run build;
)

# Check that the code of the worker is correct
git diff --exit-code ./internal/component/cloudflare/workers/code \
  || (echo "The source code of the Cloudflare Worker has changed, but the Worker has not been recompiled. Please re-compile the Worker by running 'npm ci && npm run build' in 'internal/component/cloudflare/worker-src'" && exit 1)

# Remove dashes from UNIQUE_ID
Suffix=$(echo "$UNIQUE_ID" | sed -E 's/-//g')

# Ensure the Workers KV namespace exists
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
