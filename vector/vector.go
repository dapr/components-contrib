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
// vector.proto runtime API. Components implement Vector to expose dense
// vector backends (Meilisearch, Pinecone, Qdrant, Milvus, pgvector, ...).
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
//nolint:interfacebloat // Vector store API surface intentionally exposes data and collection lifecycle methods.
type Vector interface {
	metadata.ComponentWithMetadata

	// Init is called once per component instance.
	Init(ctx context.Context, meta Metadata) error

	// Collection lifecycle.
	CreateCollection(ctx context.Context, req *CreateCollectionRequest) (*CreateCollectionResponse, error)
	DropCollection(ctx context.Context, req *DropCollectionRequest) error
	DescribeCollection(ctx context.Context, req *DescribeCollectionRequest) (*DescribeCollectionResponse, error)
	ListCollections(ctx context.Context, req *ListCollectionsRequest) (*ListCollectionsResponse, error)

	// Vector data operations.
	Upsert(ctx context.Context, req *UpsertRequest) (*UpsertResponse, error)
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error)

	// Query.
	Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error)
	BatchQuery(ctx context.Context, req *BatchQueryRequest) (*BatchQueryResponse, error)

	io.Closer
}

// DistanceMetric is the portable similarity / distance function.
type DistanceMetric int32

const (
	DistanceMetricUnspecified DistanceMetric = 0
	DistanceMetricCosine      DistanceMetric = 1
	DistanceMetricDotProduct  DistanceMetric = 2
	DistanceMetricEuclidean   DistanceMetric = 3
	DistanceMetricManhattan   DistanceMetric = 4
	DistanceMetricHamming     DistanceMetric = 5
)

// Vec is a single dense vector record.
type Vec struct {
	ID        string
	Values    []float32
	Metadata  map[string]any
	Namespace string
}

// Hit is a single match returned from a vector query.
type Hit struct {
	Vector   Vec
	Distance float64
}

// CreateCollectionRequest creates a new vector collection.
type CreateCollectionRequest struct {
	Collection     string
	Dimension      uint32
	Metric         DistanceMetric
	MetadataSchema []search.IndexFieldSchema
	Metadata       map[string]string
}

// CreateCollectionResponse is the output of Vector.CreateCollection.
type CreateCollectionResponse struct{}

// DropCollectionRequest drops an existing collection.
type DropCollectionRequest struct {
	Collection string
	Metadata   map[string]string
}

// DescribeCollectionRequest describes an existing collection.
type DescribeCollectionRequest struct {
	Collection string
	Metadata   map[string]string
}

// DescribeCollectionResponse describes an existing collection.
type DescribeCollectionResponse struct {
	Collection     string
	Dimension      uint32
	Metric         DistanceMetric
	MetadataSchema []search.IndexFieldSchema
	VectorCount    uint64
	Properties     map[string]string
}

// ListCollectionsRequest lists collections in the store.
type ListCollectionsRequest struct {
	Metadata map[string]string
}

// ListCollectionsResponse lists collections in the store.
type ListCollectionsResponse struct {
	Collections []string
}

// UpsertRequest inserts or updates a batch of vectors.
type UpsertRequest struct {
	Collection     string
	Vectors        []Vec
	Ack            search.IndexAck
	IdempotencyKey string
	Metadata       map[string]string
}

// UpsertResponse is the result of an Upsert call.
type UpsertResponse struct {
	Results []search.OperationResult
	Ack     search.IndexAck
}

// GetRequest fetches vectors by id.
type GetRequest struct {
	Collection      string
	IDs             []string
	Namespace       string
	IncludeValues   bool
	IncludeMetadata bool
	Metadata        map[string]string
}

// GetResponse is the result of a Get call.
type GetResponse struct {
	Vectors []Vec
}

// DeleteSelector selects vectors to delete – exactly one of IDs or Filter must be set.
type DeleteSelector struct {
	IDs    []string
	Filter map[string]any
}

// DeleteRequest deletes vectors by id list or filter.
type DeleteRequest struct {
	Collection string
	Namespace  string
	Selector   DeleteSelector
	Ack        search.IndexAck
	Metadata   map[string]string
}

// DeleteResponse is the result of a Delete call.
type DeleteResponse struct {
	Results      []search.OperationResult
	DeletedCount uint64
	Ack          search.IndexAck
}

// QueryRequest is a single nearest-neighbour query.
type QueryRequest struct {
	Collection        string
	Namespace         string
	QueryVector       []float32 // exactly one of QueryVector / QueryByID must be set
	QueryByID         string
	Filter            map[string]any
	TopK              uint32
	ScoreThreshold    *float64
	IncludeValues     bool
	IncludeMetadata   bool
	ContinuationToken string
	Metadata          map[string]string
}

// QueryResponse is the result of a single Query call.
type QueryResponse struct {
	Hits              []Hit
	ContinuationToken string
}

// BatchQueryRequest issues N parallel queries against the same collection.
type BatchQueryRequest struct {
	Collection string
	Queries    []QueryRequest
	Metadata   map[string]string
}

// BatchQueryResult is the per-query result inside a batch.
type BatchQueryResult struct {
	QueryIndex   uint32
	Hits         []Hit
	ErrorCode    string
	ErrorMessage string
}

// BatchQueryResponse is the result of a BatchQuery call.
type BatchQueryResponse struct {
	Results []BatchQueryResult
}
