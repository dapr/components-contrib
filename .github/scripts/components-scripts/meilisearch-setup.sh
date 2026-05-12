#!/bin/bash

set -e

# Start the Meilisearch container shared by search and vector tests.
# Idempotent: docker compose up -d is a no-op if the container is already running
# under the same project name.
docker compose -f .github/infrastructure/docker-compose-meilisearch.yml -p meilisearch up -d

# Export connection settings so the conformance and certification tests do not
# skip with "MEILISEARCH_HOST is required ...".
echo "MEILISEARCH_HOST=http://localhost:7700" >> $GITHUB_ENV
echo "MEILISEARCH_API_KEY=masterKey" >> $GITHUB_ENV
