<!--
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Vector conformance tests

Start Meilisearch from `.github/infrastructure/docker-compose-meilisearch.yml`, then export the environment variables from `.env.template` (or create `.env` in this directory). Vector tests require `MEILI_EXPERIMENTAL_VECTOR_STORE=true` in the Meilisearch container (the shared compose file already sets it).

```sh
docker compose -f .github/infrastructure/docker-compose-meilisearch.yml -p meilisearch up -d
MEILISEARCH_HOST=http://localhost:7700 MEILISEARCH_API_KEY=masterKey go test -tags conftests -run TestVectorConformance ./tests/conformance/...
```
