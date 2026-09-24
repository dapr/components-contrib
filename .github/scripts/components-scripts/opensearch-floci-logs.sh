#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
if [[ -z "${OPENSEARCH_FLOCI_PROJECT:-}" && -f tests/config/search/.env ]]; then
    set -a
    source tests/config/search/.env
    set +a
fi
export OPENSEARCH_FLOCI_PROJECT="${OPENSEARCH_FLOCI_PROJECT:-dapr-opensearch-floci}"
export OPENSEARCH_FLOCI_DOMAIN="${OPENSEARCH_FLOCI_DOMAIN:-dapr-search-$(printf '%s' "$OPENSEARCH_FLOCI_PROJECT" | cksum | cut -d ' ' -f 1)}"
docker compose -f .github/infrastructure/docker-compose-opensearch-floci.yml \
    -p "$OPENSEARCH_FLOCI_PROJECT" logs --no-color --tail=200
while read -r id; do
    [[ -z "$id" ]] || docker logs --tail=200 "$id"
done < <(docker ps -aq --filter "network=$OPENSEARCH_FLOCI_PROJECT" \
    --filter label=io.floci.service=opensearch \
    --filter "label=io.floci.resource-id=$OPENSEARCH_FLOCI_DOMAIN")
