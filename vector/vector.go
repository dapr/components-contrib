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

// Package vector defines the Vector building-block contract used by Dapr's
// vector.proto runtime API. Components implement Vector to store, retrieve and
// query pre-embedded dense vectors.
package vector

import (
	"context"
	"io"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
)

// Vector is the Vector building-block component contract. Method shapes
// mirror the alpha1 RPCs in dapr/proto/runtime/v1/vector.proto using
// Go-native types.
//
// Errors returned by components SHOULD be gRPC status errors carrying a
// canonical code. Other errors are reported to callers as INTERNAL.
//
//nolint:interfacebloat // Vector store API surface intentionally exposes data and collection lifecycle methods.
type Vector interface {
	metadata.ComponentWithMetadata

	// Init is called once per component instance.
	Init(ctx context.Context, meta Metadata) error

	// Collection lifecycle.
	CreateCollection(ctx context.Context, req *CreateCollectionRequest) error
	GetCollection(ctx context.Context, req *GetCollectionRequest) (*GetCollectionResponse, error)
	ListCollections(ctx context.Context, req *ListCollectionsRequest) (*ListCollectionsResponse, error)
	DeleteCollection(ctx context.Context, req *DeleteCollectionRequest) error

	// Vector data operations.
	Upsert(ctx context.Context, req *UpsertRequest) (*UpsertResponse, error)
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error)

	// Query.
	Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error)
	BatchQuery(ctx context.Context, req *BatchQueryRequest) (*BatchQueryResponse, error)

	io.Closer
}

// DistanceMetric determines how scores and score thresholds are interpreted.
// Mirrors vector.proto DistanceMetric.
type DistanceMetric int32

const (
	// DistanceMetricUnspecified selects the component's default metric on
	// CreateCollection and the collection's configured metric on Query.
	DistanceMetricUnspecified DistanceMetric = 0
	// DistanceMetricCosine is cosine similarity in [-1, 1]. Higher is better.
	DistanceMetricCosine DistanceMetric = 1
	// DistanceMetricDotProduct is unbounded dot-product similarity. Higher is
	// better.
	DistanceMetricDotProduct DistanceMetric = 2
	// DistanceMetricEuclidean is Euclidean distance in [0, +inf). Lower is
	// better.
	DistanceMetricEuclidean DistanceMetric = 3
)

// HigherIsBetter reports whether higher scores indicate closer matches for
// the metric.
func (m DistanceMetric) HigherIsBetter() bool {
	return m != DistanceMetricEuclidean
}

// Record is a single dense vector record.
type Record struct {
	ID string
	// Values is the dense vector. Its length must equal the collection's
	// dimensions.
	Values []float32
	// Payload is opaque caller data stored with the record and returned
	// unchanged. It is not filterable.
	Payload []byte
	// Metadata holds structured, filterable attributes addressed by the
	// portable filter DSL.
	Metadata map[string]any
}

// Match is a single match returned from a vector query.
type Match struct {
	Record Record
	// Score is the unnormalized value of the effective metric.
	Score float64
}

// CreateCollectionRequest creates a new vector collection.
type CreateCollectionRequest struct {
	Collection string
	// Component-specific settings such as index parameters.
	Metadata map[string]string
	// Dimensions is the required length of every stored vector.
	Dimensions uint32
	// Metric is the collection's distance metric. Unspecified selects the
	// component's documented default.
	Metric DistanceMetric
}

// GetCollectionRequest gets an existing collection.
type GetCollectionRequest struct {
	Collection string
	Metadata   map[string]string
}

// GetCollectionResponse describes an existing collection.
type GetCollectionResponse struct {
	Collection string
	// Approximate number of records. Providers that cannot supply this value
	// efficiently may return 0.
	RecordCount uint64
	Properties  map[string]string
	Dimensions  uint32
	// Metric is the effective metric and is always concrete.
	Metric DistanceMetric
}

// ListCollectionsRequest lists collections in the store.
type ListCollectionsRequest struct {
	Metadata map[string]string
}

// ListCollectionsResponse lists collections in the store.
type ListCollectionsResponse struct {
	Collections []string
}

// DeleteCollectionRequest deletes an existing collection.
type DeleteCollectionRequest struct {
	Collection string
	Metadata   map[string]string
}

// UpsertRequest is a keyed upsert of vector records. Every record has a
// non-empty ID that is unique within the request.
type UpsertRequest struct {
	Collection string
	Records    []Record
	Metadata   map[string]string
	Options    search.IndexingOptions
}

// UpsertResponse is the result of an Upsert call.
type UpsertResponse struct {
	// Item-specific failures known at the acknowledgement boundary.
	FailedItems []search.FailedItem
	// Always IndexAckQueued or IndexAckCompleted on success.
	Ack search.IndexAck
}

// GetRequest fetches vectors by id.
type GetRequest struct {
	Collection    string
	IDs           []string
	IncludeValues bool
	Metadata      map[string]string
}

// GetResponse is the result of a Get call. Records are returned in request
// order; records that are not found are omitted.
type GetResponse struct {
	Records []Record
}

// DeleteRequest deletes vectors by id. IDs that do not exist are not an
// error.
type DeleteRequest struct {
	Collection string
	IDs        []string
	Metadata   map[string]string
	Options    search.IndexingOptions
}

// DeleteResponse is the result of a Delete call.
type DeleteResponse struct {
	// Always IndexAckQueued or IndexAckCompleted on success.
	Ack search.IndexAck
}

// QueryRequest is a single vector query. Exactly one of Vector or ByID is
// set.
type QueryRequest struct {
	Collection string
	// Vector is the query vector. Only its Values are read.
	Vector         *Record
	ByID           string
	TopK           uint32
	Filter         map[string]any
	IncludeValues  bool
	IncludePayload bool
	// Metric selects how scores are interpreted. Unspecified uses the
	// collection's configured metric.
	Metric DistanceMetric
	// ScoreThreshold is an inclusive, unnormalized cutoff.
	ScoreThreshold *float64
	Metadata       map[string]string
}

// QueryResponse is the result of a single Query call.
type QueryResponse struct {
	Matches []Match
	// Effective metric used for scores. Always concrete.
	Metric DistanceMetric
}

// BatchQueryRequest issues multiple queries against the same collection.
type BatchQueryRequest struct {
	Collection string
	Queries    []QueryRequest
	Metadata   map[string]string
}

// BatchQueryResult is the outcome of one query in a batch. Exactly one of
// Response or Error is set.
type BatchQueryResult struct {
	Response *QueryResponse
	// Error is a gRPC status error carrying a canonical, non-OK code.
	Error error
}

// BatchQueryResponse holds one result per query, in request order.
type BatchQueryResponse struct {
	Results []BatchQueryResult
}
