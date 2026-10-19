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

## AWS OpenSearch through Floci

Use the [shared OpenSearch/Floci setup](../search/README.md#aws-opensearch-through-floci)
to run both search and vector conformance against a real, pinned OpenSearch
domain managed by Floci. Setup verifies the k-NN plugin and actual vector/text
queries, saves `OPENSEARCH_ENDPOINT` to this directory's `.env`, and exports it
through `$GITHUB_ENV` in CI. No real AWS credentials are required.

```sh
.github/scripts/components-scripts/opensearch-floci-setup.sh
set -a; source tests/config/vector/.env; set +a
go test -tags conftests -count=1 -run TestVectorConformance -timeout 20m ./tests/conformance
(cd tests/certification && go test -tags certtests,unit -count=1 -timeout 20m ./vector/aws/opensearch)
.github/scripts/components-scripts/opensearch-floci-destroy.sh
```

OpenSearch declares cosine, dot-product, and Euclidean metrics, filters,
query-by-ID, and score thresholds. It does not declare queued acknowledgements.
The [vector certification suite](../../certification/vector/aws/opensearch/README.md)
additionally checks OpenSearch-specific scoring, persistence and bulk behavior.

## Declaring component capabilities

`tests.yml` drives the suite. Each component lists the optional capabilities it
supports under `operations` (`queued-ack`, `score-threshold`, `filter`,
`query-by-id`, and one `metric-*` entry per supported distance metric).
Capabilities that are not listed are asserted to be reported as
`UNIMPLEMENTED`/`INVALID_ARGUMENT` rather than silently ignored. The alpha
contract is dense-vector only: there are no sparse, hybrid or named-vector
opt-ins.

`config.dimensions` is passed as the typed `CreateCollectionRequest.Dimensions`
and drives the generated test vectors. The shared collection is created with
`DISTANCE_METRIC_UNSPECIFIED`, so the component's default metric applies and
must be one of the declared `metric-*` entries. Provider-specific collection
settings such as index parameters travel through
`config.createCollectionMetadata`, which is passed verbatim to
`CreateCollection`; `config.upsertMetadata` and `config.queryMetadata` do the
same for writes (`Upsert`, `Delete`) and reads (`Query`, `BatchQuery`, `Get`).

## What the suite asserts

- `CreateCollection` on an existing collection returns `ALREADY_EXISTS`; a
  metric that is not declared is `INVALID_ARGUMENT`; `GetCollection` reports
  the configured `Dimensions` and a concrete `Metric`.
- `Upsert` and `Delete` share the `IndexingOptions` matrix: option validation,
  every indexing mode, and an `IndexAck` that is always `QUEUED` or
  `COMPLETED`. Deleting IDs that do not exist succeeds.
- `Record.Metadata` is structured (`map[string]any`) and round-trips with its
  JSON types; `Payload` is opaque. `Get` returns found records in request
  order.
- With `filter`, the portable DSL is exercised against typed metadata (`make`
  string, `price` number, `inStock` bool, `notes` for `$exists`) including
  `$in`/`$nin` and `$and`/`$or`/`$not`; an unsupported operator is
  `INVALID_ARGUMENT`.
- `BatchQuery` returns one result per query in request order. A query that
  fails validation (for example neither `vector` nor `by_id`) or is rejected
  by the provider yields an `Error` result with a canonical code while the
  other queries succeed; only request-wide failures fail the RPC.
