# Search building block (alpha1)

Components implementing the `Search` interface in this package back the Dapr
Search building block (`dapr/proto/runtime/v1/search.proto`). The contract is
deliberately portable across lexical / full-text search backends
(Meilisearch, Elasticsearch, OpenSearch, Solr, Typesense, ...).

Vector similarity is a separate building block – see `../vector`.

See `search.go` for the interface and request/response types.

Write acknowledgement semantics (`IndexingOptions`, `IndexAck`, `FailedItem`)
and the shared validators and error helpers live in `indexing.go`; the vector
building block reuses them.

## Contract notes

- `Document.Content` is a UTF-8 encoded JSON object. The runtime rejects any
  other content into `FailedItems` (`INVALID_ARGUMENT`) with
  `ValidateDocumentContent` before it reaches a component; components may
  keep the same check defensively. `Document.Metadata` is opaque: stored,
  returned unchanged, never filterable.
- `DeleteDocuments` is a write: it takes the same `IndexingOptions` as
  `IndexDocuments` and returns an `IndexAck`. IDs that do not exist are not an
  error and deletions never report `FailedItems`.
- `CreateIndex` returns `ALREADY_EXISTS` for an existing index and never
  reconciles settings. `GetDocuments` returns found documents in request order.
- Pagination uses an opaque continuation token bound to the query shape;
  components append the document ID as a sort tie-breaker and declare it
  sortable when the provider only sorts on declared attributes.

## Meilisearch (`meilisearch/`)

- `id` is the Meilisearch primary key, content keys are top-level attributes
  and metadata lives under the reserved `daprMetadata` attribute. `id` is
  always added to `sortableAttributes`; `filterableAttributes`,
  `sortableAttributes` and `searchableAttributes` can be set in `CreateIndex`
  metadata.
- `INDEXING_MODE_WAIT_FOR_COMPLETION` polls the task status API with
  exponential backoff by default. When the experimental `tasksStreamingRoute`
  feature is enabled (and the API key has `tasks.get`) the component shares one
  task-change stream, filtered to document addition/update and deletion tasks,
  and falls back to polling for that and every later wait if the route reports
  itself unavailable.

## AWS OpenSearch (`aws/opensearch/`)

- Uses an AWS OpenSearch Service domain and the shared AWS authentication
  mechanism. See the [configuration and provider notes](../common/component/aws/opensearch/README.md).
- Supports lexical search, OpenSearch native queries, portable content filters,
  projections, sorting, highlights and continuation-token pagination.
- Native queries use OpenSearch's data-plane query DSL and address indexed
  content using the `content.` prefix, for example
  `{"query":{"match":{"content.title":"dapr"}}}`. Portable `SearchFields`,
  `ReturnFields`, filters, sorting and highlights use the caller's original
  content paths without that prefix. Other native request options are rejected;
  pagination and result shaping remain controlled by the portable request.
- `TopK` defaults to 20 and is limited to 9999. Pagination uses a query-bound
  `search_after` token and an ID tie-breaker, not a point-in-time snapshot;
  concurrent writes can change results between pages.
- Document content and opaque metadata are stored separately; provider fields
  do not overwrite caller content. Writes complete through the bulk API with
  `refresh=wait_for`; there is no queued acknowledgement or continue-async mode.
- Run the [Floci-backed conformance suite](../tests/config/search/README.md)
  against a real OpenSearch data plane.
