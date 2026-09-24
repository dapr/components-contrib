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
// search.proto runtime API. Components implement Search to store, retrieve and
// query index-ready documents in lexical / structured search backends.
//
// Vector similarity is intentionally a separate building block; see the
// sibling vector package.
package search

import (
	"context"
	"io"

	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/metadata"
)

// Search is the Search building-block component contract. Method shapes
// mirror the alpha1 RPCs in dapr/proto/runtime/v1/search.proto using
// Go-native types so the runtime can adapt between gRPC, HTTP/JSON and
// component invocations without leaking proto types into components.
//
// Errors returned by components SHOULD be gRPC status errors carrying a
// canonical code (see google.golang.org/grpc/status). Other errors are
// reported to callers as INTERNAL.
//
//nolint:interfacebloat // Search API surface intentionally exposes data and index lifecycle methods.
type Search interface {
	metadata.ComponentWithMetadata

	// Init is called once per component instance with the resolved metadata.
	Init(ctx context.Context, meta Metadata) error

	// Index lifecycle.
	CreateIndex(ctx context.Context, req *CreateIndexRequest) error
	GetIndex(ctx context.Context, req *GetIndexRequest) (*GetIndexResponse, error)
	ListIndexes(ctx context.Context, req *ListIndexesRequest) (*ListIndexesResponse, error)
	DeleteIndex(ctx context.Context, req *DeleteIndexRequest) error

	// Document operations.
	IndexDocuments(ctx context.Context, req *IndexDocumentsRequest) (*IndexDocumentsResponse, error)
	GetDocuments(ctx context.Context, req *GetDocumentsRequest) (*GetDocumentsResponse, error)
	DeleteDocuments(ctx context.Context, req *DeleteDocumentsRequest) (*DeleteDocumentsResponse, error)

	// Query.
	Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error)

	io.Closer
}

// SortOrder selects the direction of a sort clause. Mirrors search.proto.
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

// Document is a single index-ready document.
type Document struct {
	ID string
	// Content is a UTF-8 encoded JSON object. Field-level operations address
	// its keys with dotted paths. The runtime rejects content that is not a
	// JSON object before it reaches the component.
	Content []byte
	// Metadata is opaque caller metadata stored with the document and
	// returned unchanged. It is not indexed or filterable.
	Metadata map[string]string
}

// Hit is a single result entry returned from a Search call.
type Hit struct {
	Document Document
	// Score is an unnormalized provider-specific relevance score. Higher
	// values indicate a more relevant match.
	Score      float64
	Highlights map[string]string
}

// TotalHitsRelation describes the accuracy of SearchResponse.TotalHits.
type TotalHitsRelation int32

const (
	TotalHitsRelationUnspecified TotalHitsRelation = 0
	TotalHitsRelationExact       TotalHitsRelation = 1
	TotalHitsRelationLowerBound  TotalHitsRelation = 2
	TotalHitsRelationEstimate    TotalHitsRelation = 3
)

// CreateIndexRequest is the input to Search.CreateIndex.
type CreateIndexRequest struct {
	Index string
	// Component-specific index settings.
	Metadata map[string]string
}

// GetIndexRequest is the input to Search.GetIndex.
type GetIndexRequest struct {
	Index    string
	Metadata map[string]string
}

// GetIndexResponse is the output of Search.GetIndex.
type GetIndexResponse struct {
	Index string
	// Approximate document count. Providers that cannot supply this value
	// efficiently may return 0.
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

// DeleteIndexRequest is the input to Search.DeleteIndex.
type DeleteIndexRequest struct {
	Index    string
	Metadata map[string]string
}

// IndexDocumentsRequest is the input to Search.IndexDocuments. It is a keyed
// upsert: every document has a non-empty ID that is unique within the request.
type IndexDocumentsRequest struct {
	Index     string
	Documents []Document
	Metadata  map[string]string
	Options   IndexingOptions
}

// IndexDocumentsResponse is the output of Search.IndexDocuments.
type IndexDocumentsResponse struct {
	// Item-specific failures known at the acknowledgement boundary.
	FailedItems []FailedItem
	// Always IndexAckQueued or IndexAckCompleted on success.
	Ack IndexAck
}

// GetDocumentsRequest is the input to Search.GetDocuments.
type GetDocumentsRequest struct {
	Index          string
	IDs            []string
	IncludeContent bool
	Metadata       map[string]string
}

// GetDocumentsResponse is the output of Search.GetDocuments. Documents are
// returned in request order; documents that are not found are omitted.
type GetDocumentsResponse struct {
	Documents []Document
}

// DeleteDocumentsRequest is the input to Search.DeleteDocuments. IDs that do
// not exist are not an error.
type DeleteDocumentsRequest struct {
	Index    string
	IDs      []string
	Metadata map[string]string
	Options  IndexingOptions
}

// DeleteDocumentsResponse is the output of Search.DeleteDocuments.
type DeleteDocumentsResponse struct {
	// Always IndexAckQueued or IndexAckCompleted on success.
	Ack IndexAck
}

// SearchRequest is the input to Search.Search. Exactly one of Text or Native
// is set.
type SearchRequest struct {
	Index             string
	Text              string
	Native            map[string]any
	Filter            map[string]any
	TopK              uint32
	ContinuationToken string
	ReturnFields      []string
	IncludeContent    bool
	SearchFields      []string
	Sort              []SortClause
	HighlightFields   []string
	Metadata          map[string]string
}

// SearchResponse is the output of Search.Search.
type SearchResponse struct {
	Hits []Hit
	// Best-effort total. Nil when the provider cannot supply one.
	TotalHits *uint64
	// Opaque token for the next page. Empty when there are no more results.
	ContinuationToken string
	// Accuracy of TotalHits. Unspecified when TotalHits is nil.
	TotalHitsRelation TotalHitsRelation
}

// FailedItem is an item-specific write failure shared by document indexing
// and vector upserts.
type FailedItem struct {
	ID string
	// Error carries a canonical, non-OK code.
	Error *status.Status
}
