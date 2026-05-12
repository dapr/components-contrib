# Vector building block (alpha1)

Components implementing the `Vector` interface in this package back the Dapr
Vector building block (`dapr/proto/runtime/v1/vector.proto`). The contract is
portable across dense-vector backends (Meilisearch, Pinecone, Qdrant, Milvus,
pgvector, ...).

Lexical / full-text search is a separate building block – see `../search`.

See `vector.go` for the interface and request/response types.
