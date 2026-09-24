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
