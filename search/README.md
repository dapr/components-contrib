# Search building block (alpha1)

Components implementing the `Search` interface in this package back the Dapr
Search building block (`dapr/proto/runtime/v1/search.proto`). The contract is
deliberately portable across lexical / full-text search backends
(Meilisearch, Elasticsearch, OpenSearch, Solr, Typesense, ...).

Vector similarity is a separate building block – see `../vector`.

See `search.go` for the interface and request/response types.
