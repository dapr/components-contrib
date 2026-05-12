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

	contribmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	kitlogger "github.com/dapr/kit/logger"
	"github.com/stretchr/testify/require"
)

// Run with:
// MEILISEARCH_HOST=http://localhost:7700 MEILISEARCH_API_KEY=masterKey go test -tags=e2e ./search/meilisearch/...
func TestMeilisearchSearchE2E(t *testing.T) {
	host := os.Getenv("MEILISEARCH_HOST")
	if host == "" {
		t.Skip("MEILISEARCH_HOST is required for e2e tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	component := NewMeilisearch(kitlogger.NewLogger("test"))
	require.NoError(t, component.Init(ctx, search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": host, "apiKey": os.Getenv("MEILISEARCH_API_KEY")}}}))
	index := "dapr_cars_e2e"
	_ = component.DropIndex(ctx, &search.DropIndexRequest{Index: index})
	_, err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Fields: []search.IndexFieldSchema{{Name: "title", Searchable: true}, {Name: "make", Filterable: true}}})
	require.NoError(t, err)
	_, err = component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: index, Ack: search.IndexAckDurable, Documents: []search.Document{{ID: "1", Content: map[string]any{"title": "Hyundai Sonata", "make": "hyundai"}}}})
	require.NoError(t, err)
	res, err := component.Search(ctx, &search.SearchRequest{Index: index, Text: "Hyundai", IncludeContent: true, Filter: map[string]any{"make": "hyundai"}, TopK: 1})
	require.NoError(t, err)
	require.NotEmpty(t, res.Hits)
	require.Equal(t, "1", res.Hits[0].Document.ID)
	_ = component.DropIndex(ctx, &search.DropIndexRequest{Index: index})
}
