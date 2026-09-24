//go:build e2e

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

package meilisearch

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	contribmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	kitlogger "github.com/dapr/kit/logger"
)

// Run with:
// MEILISEARCH_HOST=http://localhost:7700 MEILISEARCH_API_KEY=masterKey go test -tags=e2e ./vector/meilisearch/...
//
// Wait-for-completion works against any Meilisearch version: the component
// polls the task status API, or uses the experimental `tasksStreamingRoute`
// task-change stream when it is enabled and the API key carries `tasks.get`.
func TestMeilisearchVectorE2E(t *testing.T) {
	host := os.Getenv("MEILISEARCH_HOST")
	if host == "" {
		t.Skip("MEILISEARCH_HOST is required for e2e tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	component := NewMeilisearch(kitlogger.NewLogger("test"))
	require.NoError(t, component.Init(ctx, vector.Metadata{Base: contribmetadata.Base{Properties: map[string]string{
		"host": host, "apiKey": os.Getenv("MEILISEARCH_API_KEY"),
	}}}))
	defer func() { require.NoError(t, component.Close()) }()

	collection := "dapr_books_vector_e2e"
	_ = component.DeleteCollection(ctx, &vector.DeleteCollectionRequest{Collection: collection})
	require.NoError(t, component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: collection, Dimensions: 3}))
	defer func() { _ = component.DeleteCollection(ctx, &vector.DeleteCollectionRequest{Collection: collection}) }()

	err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: collection, Dimensions: 3})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	described, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: collection})
	require.NoError(t, err)
	assert.Equal(t, uint32(3), described.Dimensions)
	assert.Equal(t, vector.DistanceMetricCosine, described.Metric)

	waitForCompletion := search.IndexingOptions{
		Mode:          search.IndexingModeWaitForCompletion,
		WaitTimeout:   10 * time.Second,
		OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
	}
	res, err := component.Upsert(ctx, &vector.UpsertRequest{
		Collection: collection,
		Records: []vector.Record{
			{ID: "1", Values: []float32{1, 0, 0}, Payload: []byte(`{"title":"Pride and Prejudice"}`), Metadata: map[string]any{"author": "pride-and-prejudice", "year": 2024}},
			{ID: "2", Values: []float32{0, 1, 0}, Metadata: map[string]any{"author": "frankenstein", "year": 2020}},
		},
		Options: waitForCompletion,
	})
	require.NoError(t, err)
	require.Equal(t, search.IndexAckCompleted, res.Ack)
	require.Empty(t, res.FailedItems)

	got, err := component.Get(ctx, &vector.GetRequest{Collection: collection, IDs: []string{"2", "missing", "1"}, IncludeValues: true})
	require.NoError(t, err)
	require.Len(t, got.Records, 2, "found records are returned in request order")
	assert.Equal(t, "2", got.Records[0].ID)
	assert.Equal(t, "1", got.Records[1].ID)
	assert.Equal(t, []byte(`{"title":"Pride and Prejudice"}`), got.Records[1].Payload)
	assert.Equal(t, map[string]any{"author": "pride-and-prejudice", "year": float64(2024)}, got.Records[1].Metadata)

	query, err := component.Query(ctx, &vector.QueryRequest{
		Collection: collection, Vector: &vector.Record{Values: []float32{1, 0, 0}}, TopK: 1,
		IncludeValues: true, IncludePayload: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, query.Matches)
	assert.Equal(t, "1", query.Matches[0].Record.ID)
	assert.Equal(t, vector.DistanceMetricCosine, query.Metric)
	assert.InDelta(t, 1.0, query.Matches[0].Score, 0.01, "an identical vector has a cosine similarity of 1")

	filtered, err := component.Query(ctx, &vector.QueryRequest{
		Collection: collection, Vector: &vector.Record{Values: []float32{1, 0, 0}}, TopK: 2,
		Filter: map[string]any{"year": map[string]any{"$lt": 2022}},
	})
	require.NoError(t, err)
	require.Len(t, filtered.Matches, 1, "metadata is filterable without knowing the storage layout")
	assert.Equal(t, "2", filtered.Matches[0].Record.ID)

	batch, err := component.BatchQuery(ctx, &vector.BatchQueryRequest{Collection: collection, Queries: []vector.QueryRequest{
		{Vector: &vector.Record{Values: []float32{0, 1, 0}}, TopK: 1},
		{Vector: &vector.Record{Values: []float32{0, 1, 0}}, ByID: "1"},
	}})
	require.NoError(t, err)
	require.Len(t, batch.Results, 2)
	require.NotNil(t, batch.Results[0].Response)
	assert.Equal(t, "2", batch.Results[0].Response.Matches[0].Record.ID)
	assert.Equal(t, codes.InvalidArgument, status.Code(batch.Results[1].Error), "an invalid query is an error result")

	deleted, err := component.Delete(ctx, &vector.DeleteRequest{Collection: collection, IDs: []string{"1", "missing"}, Options: waitForCompletion})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, deleted.Ack, "missing ids are not an error")

	got, err = component.Get(ctx, &vector.GetRequest{Collection: collection, IDs: []string{"1", "2"}})
	require.NoError(t, err)
	require.Len(t, got.Records, 1)
	assert.Equal(t, "2", got.Records[0].ID)
}
