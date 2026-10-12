# Vector building block (alpha1)

Components implementing the `Vector` interface in this package back the Dapr
Vector building block (`dapr/proto/runtime/v1/vector.proto`). The contract is
portable across dense-vector backends (Meilisearch, Pinecone, Qdrant, Milvus,
pgvector, ...).

Lexical / full-text search is a separate building block – see `../search`.

See `vector.go` for the interface and request/response types.

Vector writes reuse the search building block's `IndexingOptions`, `IndexAck`
and `FailedItem` types (see `../search/indexing.go`).

## Contract notes

- Records are dense only: `Record.Values` must match the collection's
  `Dimensions`. `Record.Payload` is opaque bytes returned unchanged;
  `Record.Metadata` is a structured `map[string]any` addressed by the portable
  filter DSL (`make`, `dealer.city`, `$and`/`$or`/`$not`, `$in`, ...).
- `CreateCollection` takes typed `Dimensions` (required) and `Metric`;
  `DistanceMetricUnspecified` selects the component default and a metric the
  component cannot provide is `INVALID_ARGUMENT`. An existing collection is
  `ALREADY_EXISTS`. `GetCollection` reports the effective `Dimensions` and
  `Metric`.
- `Delete` is a write: it takes `IndexingOptions` and returns an `IndexAck`;
  IDs that do not exist are not an error. `Get` returns found records in
  request order.
- `BatchQuery` evaluates every query independently and returns one
  `BatchQueryResult{Response|Error}` per query in request order. Only
  request-wide failures (missing collection, credentials, transport) fail the
  call.
- Scores are the unnormalized metric value: cosine in `[-1, 1]` and dot
  product are higher-is-better, Euclidean is lower-is-better.
  `ScoreThreshold` is inclusive on that scale.

## Meilisearch (`meilisearch/`)

- A collection is a Meilisearch index with one `userProvided` embedder named
  `default` of the requested dimensions; cosine is the only supported metric.
  Values are stored in `_vectors.default`, the payload base64-encoded under
  `daprPayload` and metadata as the `daprMetadata` object, which is declared
  filterable so filter paths translate to `daprMetadata.<path>`.
- Collections share the instance with plain indexes, such as those of the
  search component. `ListCollections` returns only the indexes that have the
  `default` embedder, and `GetCollection` and `Upsert` return
  `FAILED_PRECONDITION` for an index without it.
- `Upsert` requires an existing collection and returns `NOT_FOUND` otherwise:
  Meilisearch would create the index on the first document addition, without
  an embedder. A Meilisearch task is atomic, so a record whose `Values` do not
  match the collection's dimensions, or whose ID is not 1 to 511 bytes of
  ASCII letters, digits, `-` and `_`, is identified before the task is
  enqueued and reported in `FailedItems` (`INVALID_ARGUMENT`). `Get` omits
  such an ID like any other missing record.
- Meilisearch's `_rankingScore` for a pure-semantic query is `(1 + cos) / 2`;
  the component reports `2 * rankingScore - 1` and translates a cosine
  threshold `t` to `rankingScoreThreshold = (1 + t) / 2`. A `ScoreThreshold`
  outside `[-1, 1]` is `INVALID_ARGUMENT`.
- Wait-for-completion writes and deletes use the shared task dispatcher of the
  search component: task status polling by default, the experimental
  task-change stream when available (see `../search/README.md`).
