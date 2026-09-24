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

# Meilisearch Vector certification tests

These lightweight certification tests exercise the `vector.Vector` component interface directly; they do not start an embedded Dapr sidecar. The component YAML in `components/` is informational and mirrors the configuration used by the test.

## Prerequisites

The shared `.github/infrastructure/docker-compose-meilisearch.yml` already starts Meilisearch with `MEILI_EXPERIMENTAL_VECTOR_STORE=true`, which is required for vector tests.

```sh
# from the repo root
docker compose -f .github/infrastructure/docker-compose-meilisearch.yml -p meilisearch up -d
export MEILISEARCH_HOST=http://localhost:7700
export MEILISEARCH_API_KEY=masterKey
go test -tags certtests ./tests/certification/vector/meilisearch/...
```

The test is skipped when `MEILISEARCH_HOST` is unset.
