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
- A Meilisearch task is atomic, so everything Meilisearch would reject per
  document is identified before the task is enqueued and reported in
  `FailedItems` (`INVALID_ARGUMENT`): content that uses `id`, `daprMetadata`,
  `daprPayload` or an attribute starting with `_` (Meilisearch's own
  namespace), which could not be read back as written, and an ID that is not
  1 to 511 bytes of ASCII letters, digits, `-` and `_`. `GetDocuments` omits
  such an ID like any other missing document.
- `IndexDocuments` requires an existing index and returns `NOT_FOUND`
  otherwise: Meilisearch would create the index on the first document
  addition, without the settings `CreateIndex` applies. `ListIndexes` reads
  every page of the index list.
- Filter field names are quoted, so a filter key is never read as filter
  syntax. A native query cannot set pagination or sorting, its `filter` is
  ANDed with the portable filter, and the document ID, its metadata and the
  ranking score are returned whatever it projects.
- `Highlights` holds the requested `HighlightFields` only, a nested field
  under the path it was requested with.
- `INDEXING_MODE_WAIT_FOR_COMPLETION` polls the task status API with
  exponential backoff by default. When the experimental `tasksStreamingRoute`
  feature is enabled (and the API key has `tasks.get`) the component shares one
  task-change stream, filtered to document addition/update and deletion tasks,
  and falls back to polling for that and every later wait if the route reports
  itself unavailable.
