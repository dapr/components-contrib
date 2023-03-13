#!/bin/sh

set +e

# Delete the Worker
curl -X DELETE "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${CloudflareWorkerName}" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}"

# Delete the KV namespace
curl -X DELETE "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces/${CloudflareKVNamespaceID}" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}"