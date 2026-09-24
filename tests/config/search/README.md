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

# Search conformance tests

Start Meilisearch from `.github/infrastructure/docker-compose-meilisearch.yml`, then export the environment variables from `.env.template` (or create `.env` in this directory).

```sh
docker compose -f .github/infrastructure/docker-compose-meilisearch.yml -p meilisearch up -d
MEILISEARCH_HOST=http://localhost:7700 MEILISEARCH_API_KEY=masterKey go test -tags conftests -run TestSearchConformance ./tests/conformance/...
```

## AWS OpenSearch through Floci

The local/CI setup uses pinned Floci **2.1.0 compatibility** and OpenSearch
**2.19.5** images, not the Floci metadata-only mock. It creates a domain through
Floci's AWS-compatible management API and discovers the published data-plane
port from that domain's labeled backend container. No AWS account, real
credentials, AWS CLI installation, or cloud access is required; the component
configurations use dummy `test` credentials and `us-east-1`.

Requirements: Docker with Compose and permission to mount the Docker socket,
Python 3, and enough memory for OpenSearch. On Linux, `vm.max_map_count` must be
at least `262144`; setup checks this but never changes the shared host setting.
The Floci container runs as root to access the Docker socket. Use a trusted,
isolated development/CI machine: Floci publishes its managed OpenSearch backend
on all host interfaces with security disabled.

From the repository root:

```sh
# Optional isolation settings. Use a distinct project AND non-overlapping
# management/backend port ranges for concurrent runs on the same Docker host.
export OPENSEARCH_FLOCI_PROJECT=dapr-opensearch-local
export OPENSEARCH_FLOCI_PORT=14567
export OPENSEARCH_FLOCI_PROXY_BASE_PORT=19400
export OPENSEARCH_FLOCI_PROXY_MAX_PORT=19499
.github/scripts/components-scripts/opensearch-floci-setup.sh
set -a; source tests/config/search/.env; set +a
go test -tags conftests -count=1 -run 'Test(Search|Vector)Conformance' -timeout 20m ./tests/conformance
(cd tests/certification && go test -tags certtests,unit -count=1 -timeout 20m ./search/aws/opensearch ./vector/aws/opensearch)
.github/scripts/components-scripts/opensearch-floci-logs.sh
.github/scripts/components-scripts/opensearch-floci-destroy.sh
```

Defaults are project `dapr-opensearch-floci`, management port `4567`, and backend
ports `9400`–`9499`. `OPENSEARCH_FLOCI_DOMAIN` optionally overrides the default
project-derived domain name; domain names must also be unique on the Docker
host. Setup is idempotent within a project and preserves unrelated entries in
`tests/config/search/.env` and `tests/config/vector/.env`. It writes connection
settings to both files and to `$GITHUB_ENV` when running in CI. Source either
file locally (the conformance suites also load their own `.env`).

Readiness requires Floci `/health`, a yellow/green OpenSearch cluster, the
`opensearch-knn` plugin, and successful real text-search and vector-query probes.
Do not point `OPENSEARCH_ENDPOINT` at Floci's management port. The destroy
wrapper deletes only the configured domain through Floci before bringing down
the owned Compose project/network; it does not stop unrelated containers.
It also removes that project's generated OpenSearch settings from the local
`.env` files while preserving unrelated settings and other projects' files.
Always destroy before recreating Floci, whose domain metadata is in memory.

The [search certification suite](../../certification/search/aws/opensearch/README.md)
and [vector certification suite](../../certification/vector/aws/opensearch/README.md)
exercise provider-specific behavior beyond the portable conformance contract.
Both run in CI without cloud credentials. Local certification tests require
exporting the generated environment explicitly and running inside the
`tests/certification` Go module, as shown above.
Floci disables data-plane security, so these tests do not certify AWS IAM
authorization; signing and credential-provider behavior are covered by unit tests.

## Declaring component capabilities

`tests.yml` drives the suite. Each component lists the optional capabilities it
supports under `operations` (`queued-ack`, `total-hits`, `continuation-token`,
`highlights`, `native-query`, `filter`, `sort`, `return-fields`,
`search-fields`). Capabilities that are not listed are asserted to be reported
as `UNIMPLEMENTED`/`INVALID_ARGUMENT` rather than silently ignored.

Provider-specific index settings travel through `config.createIndexMetadata`,
which is passed verbatim to `CreateIndex`; `config.indexMetadata` and
`config.searchMetadata` do the same for writes (`IndexDocuments`,
`DeleteDocuments`) and reads (`Search`, `GetDocuments`). `config.waitTimeout`
sets the `INDEXING_MODE_WAIT_FOR_COMPLETION` wait used when the suite needs a
final provider result before reading back.

`config.nativeQuery` overrides the provider-native query used by the suite
(the default remains Meilisearch's `q`). OpenSearch uses its Query DSL and
declares every search capability except `queued-ack`.

## What the suite asserts

- `CreateIndex` on an existing index returns `ALREADY_EXISTS`; `GetIndex` and
  `DeleteIndex` on a missing index return `NOT_FOUND`.
- `IndexDocuments` and `DeleteDocuments` share the `IndexingOptions` matrix:
  option validation, every indexing mode, and an `IndexAck` that is always
  `QUEUED` or `COMPLETED`. Deleting IDs that do not exist succeeds.
- Document `content` is always a JSON object (the runtime rejects anything
  else before the component is invoked) and `metadata` is opaque and returned
  unchanged. `GetDocuments` returns found documents in request order.
- With `filter`, the portable DSL is exercised against typed content fields
  (`category` string, `price` number, `inStock` bool, `notes` for `$exists`)
  including `$in`/`$nin` and `$and`/`$or`/`$not`; an unsupported operator is
  `INVALID_ARGUMENT`.
- With `continuation-token` (and `sort`), walking every page of a query with a
  non-unique sort visits each document exactly once, which relies on the
  component declaring the document id as the stable tie-breaker.
