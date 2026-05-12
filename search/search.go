/*
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
*/

// Package search defines the Search building-block contract used by Dapr's
// search.proto runtime API. Components implement Search to expose lexical /
// full-text search backends (Meilisearch, Elasticsearch, OpenSearch, etc.).
//
// Vector similarity is intentionally a separate building block; see the
// sibling vector package.
package search

import (
	"context"
	"io"

	"github.com/dapr/components-contrib/metadata"
)

// Search is the Search building-block component contract. Method shapes
// mirror the alpha1 RPCs in dapr/proto/runtime/v1/search.proto using
// Go-native types so the runtime can adapt between gRPC, HTTP/JSON and
// component invocations without leaking proto types into components.
type Search interface {
	metadata.ComponentWithMetadata

	// Init is called once per component instance with the resolved metadata.
	Init(ctx context.Context, meta Metadata) error

	// Index lifecycle.
	CreateIndex(ctx context.Context, req *CreateIndexRequest) (*CreateIndexResponse, error)
	DropIndex(ctx context.Context, req *DropIndexRequest) error
	DescribeIndex(ctx context.Context, req *DescribeIndexRequest) (*DescribeIndexResponse, error)
	ListIndexes(ctx context.Context, req *ListIndexesRequest) (*ListIndexesResponse, error)

	// Document operations.
	IndexDocuments(ctx context.Context, req *IndexDocumentsRequest) (*IndexDocumentsResponse, error)
	DeleteDocuments(ctx context.Context, req *DeleteDocumentsRequest) (*DeleteDocumentsResponse, error)

	// Query.
	Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error)

	io.Closer
}

// IndexAck declares the durability guarantee a caller is requesting from the
// backend for a write (Index/Delete) operation. Mirrors search.proto IndexAck.
type IndexAck int32

const (
	IndexAckUnspecified IndexAck = 0
	IndexAckQueued      IndexAck = 1
	IndexAckDurable     IndexAck = 2
)

// IndexFieldType is the portable set of field types supported when declaring
// an index schema. Mirrors search.proto IndexFieldType.
type IndexFieldType int32

const (
	IndexFieldTypeUnspecified IndexFieldType = 0
	IndexFieldTypeText        IndexFieldType = 1
	IndexFieldTypeKeyword     IndexFieldType = 2
	IndexFieldTypeInt         IndexFieldType = 3
	IndexFieldTypeDouble      IndexFieldType = 4
	IndexFieldTypeBool        IndexFieldType = 5
	IndexFieldTypeDateTime    IndexFieldType = 6
	IndexFieldTypeGeoPoint    IndexFieldType = 7
)

// IndexFieldSchema describes a single field within an index schema.
type IndexFieldSchema struct {
	Name       string
	Type       IndexFieldType
	Filterable bool
	Sortable   bool
	Searchable bool
}

// SortOrder selects the direction of a sort clause.
type SortOrder int32

const (
	SortOrderUnspecified SortOrder = 0
	SortOrderAsc         SortOrder = 1
	SortOrderDesc        SortOrder = 2
)

// SortClause expresses a single sort key.
type SortClause struct {
	Field string
	Order SortOrder
}

// Document is a single indexable document.
type Document struct {
	ID       string
	Content  map[string]any
	Metadata map[string]string
}

// Snippet is a single highlighted excerpt.
type Snippet struct {
	Text string
}

// FieldHighlight is the set of highlighted snippets for a single field.
type FieldHighlight struct {
	Snippets []Snippet
}

// Hit is a single result entry returned from a Search call.
type Hit struct {
	Document   Document
	Score      float64
	Highlights map[string]FieldHighlight
}

// OperationResult reports the outcome of a single sub-operation.
type OperationResult struct {
	ID           string
	Success      bool
	ErrorCode    string
	ErrorMessage string
}

// CreateIndexRequest is the input to Search.CreateIndex.
type CreateIndexRequest struct {
	Index    string
	Fields   []IndexFieldSchema
	Metadata map[string]string
}

// CreateIndexResponse is the output of Search.CreateIndex.
type CreateIndexResponse struct{}

// DropIndexRequest is the input to Search.DropIndex.
type DropIndexRequest struct {
	Index    string
	Metadata map[string]string
}

// DescribeIndexRequest is the input to Search.DescribeIndex.
type DescribeIndexRequest struct {
	Index    string
	Metadata map[string]string
}

// DescribeIndexResponse is the output of Search.DescribeIndex.
type DescribeIndexResponse struct {
	Index         string
	Fields        []IndexFieldSchema
	DocumentCount uint64
	Properties    map[string]string
}

// ListIndexesRequest is the input to Search.ListIndexes.
type ListIndexesRequest struct {
	Metadata map[string]string
}

// ListIndexesResponse is the output of Search.ListIndexes.
type ListIndexesResponse struct {
	Indexes []string
}

// IndexDocumentsRequest is the input to Search.IndexDocuments.
type IndexDocumentsRequest struct {
	Index          string
	Documents      []Document
	Ack            IndexAck
	IdempotencyKey string
	Metadata       map[string]string
}

// IndexDocumentsResponse is the output of Search.IndexDocuments.
type IndexDocumentsResponse struct {
	Results []OperationResult
	Ack     IndexAck
}

// DeleteDocumentsRequest is the input to Search.DeleteDocuments.
type DeleteDocumentsRequest struct {
	Index    string
	IDs      []string
	Ack      IndexAck
	Metadata map[string]string
}

// DeleteDocumentsResponse is the output of Search.DeleteDocuments.
type DeleteDocumentsResponse struct {
	Results []OperationResult
	Ack     IndexAck
}

// SearchRequest is the input to Search.Search.
type SearchRequest struct {
	Index             string
	Text              string
	Native            map[string]any
	Filter            map[string]any
	TopK              uint32
	ReturnFields      []string
	IncludeContent    bool
	SearchFields      []string
	Sort              []SortClause
	HighlightFields   []string
	ContinuationToken string
	Metadata          map[string]string
}

// SearchResponse is the output of Search.Search.
type SearchResponse struct {
	Hits              []Hit
	TotalHits         uint64
	ContinuationToken string
}
