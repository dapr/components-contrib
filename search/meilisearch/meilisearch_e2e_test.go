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
	kitlogger "github.com/dapr/kit/logger"
)

// Run with:
// MEILISEARCH_HOST=http://localhost:7700 MEILISEARCH_API_KEY=masterKey go test -tags=e2e ./search/meilisearch/...
//
// Wait-for-completion works against any Meilisearch version: the component
// polls the task status API, or uses the experimental `tasksStreamingRoute`
// task-change stream when it is enabled and the API key carries `tasks.get`.
func TestMeilisearchSearchE2E(t *testing.T) {
	host := os.Getenv("MEILISEARCH_HOST")
	if host == "" {
		t.Skip("MEILISEARCH_HOST is required for e2e tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	component := NewMeilisearch(kitlogger.NewLogger("test"))
	require.NoError(t, component.Init(ctx, search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{
		"host": host, "apiKey": os.Getenv("MEILISEARCH_API_KEY"),
	}}}))
	defer func() { require.NoError(t, component.Close()) }()

	index := "dapr_books_e2e"
	_ = component.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: index})
	require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Metadata: map[string]string{
		"searchableAttributes": "title",
		"filterableAttributes": "author",
	}}))
	defer func() { _ = component.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: index}) }()

	err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	waitForCompletion := search.IndexingOptions{
		Mode:          search.IndexingModeWaitForCompletion,
		WaitTimeout:   10 * time.Second,
		OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
	}
	res, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{
		Index: index,
		Documents: []search.Document{
			{ID: "1", Content: []byte(`{"title":"Pride and Prejudice","author":"jane-austen"}`), Metadata: map[string]string{"tenant": "acme"}},
			{ID: "2", Content: []byte(`{"title":"Frankenstein","author":"mary-shelley"}`)},
		},
		Options: waitForCompletion,
	})
	require.NoError(t, err)
	require.Equal(t, search.IndexAckCompleted, res.Ack)
	require.Empty(t, res.FailedItems)

	got, err := component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: index, IDs: []string{"2", "missing", "1"}, IncludeContent: true})
	require.NoError(t, err)
	require.Len(t, got.Documents, 2, "found documents are returned in request order")
	assert.Equal(t, "2", got.Documents[0].ID)
	assert.Equal(t, "1", got.Documents[1].ID)
	assert.JSONEq(t, `{"title":"Pride and Prejudice","author":"jane-austen"}`, string(got.Documents[1].Content))
	assert.Equal(t, map[string]string{"tenant": "acme"}, got.Documents[1].Metadata)

	found, err := component.Search(ctx, &search.SearchRequest{
		Index: index, Text: "Pride and Prejudice", IncludeContent: true, Filter: map[string]any{"author": "jane-austen"}, TopK: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, found.Hits)
	assert.Equal(t, "1", found.Hits[0].Document.ID)
	assert.Positive(t, found.Hits[0].Score)

	deleted, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: index, IDs: []string{"1", "missing"}, Options: waitForCompletion})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, deleted.Ack, "missing ids are not an error")

	got, err = component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: index, IDs: []string{"1", "2"}})
	require.NoError(t, err)
	require.Len(t, got.Documents, 1)
	assert.Equal(t, "2", got.Documents[0].ID)
}
