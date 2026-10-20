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

package inmemory

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
)

func TestSortWithMissingFieldsPagesEveryDocumentOnce(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := NewInMemorySearch(logger.NewLogger("test"))
	require.NoError(t, s.Init(ctx, search.Metadata{}))
	require.NoError(t, s.CreateIndex(ctx, &search.CreateIndexRequest{Index: "idx"}))

	const total = 30
	docs := make([]search.Document, total)
	for i := range docs {
		content := map[string]any{"title": fmt.Sprintf("doc %02d", i)}
		if i%3 != 0 {
			content["price"] = float64(i % 4)
		}
		b, err := json.Marshal(content)
		require.NoError(t, err)
		docs[i] = search.Document{ID: fmt.Sprintf("doc-%02d", i), Content: b}
	}
	_, err := s.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: "idx", Documents: docs})
	require.NoError(t, err)

	seen := map[string]int{}
	token := ""
	for range total + 1 {
		resp, err := s.Search(ctx, &search.SearchRequest{
			Index: "idx", TopK: 5, ContinuationToken: token,
			Sort: []search.SortClause{{Field: "price", Order: search.SortOrderAsc}},
		})
		require.NoError(t, err)
		for _, hit := range resp.Hits {
			seen[hit.Document.ID]++
		}
		if resp.ContinuationToken == "" {
			break
		}
		token = resp.ContinuationToken
	}
	assert.Len(t, seen, total)
	for id, n := range seen {
		assert.Equal(t, 1, n, "document %s was paged %d times", id, n)
	}
}

func TestQueuedWriteIntoReplacedIndex(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := NewInMemorySearch(logger.NewLogger("test"))
	require.NoError(t, s.Init(ctx, search.Metadata{Base: metadata.Base{Properties: map[string]string{"indexingDelay": "100ms"}}}))
	require.NoError(t, s.CreateIndex(ctx, &search.CreateIndexRequest{Index: "idx"}))

	resp, err := s.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: "idx", Documents: []search.Document{{ID: "d", Content: []byte(`{"title":"queued"}`)}}})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckQueued, resp.Ack)

	require.NoError(t, s.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: "idx"}))
	require.NoError(t, s.CreateIndex(ctx, &search.CreateIndexRequest{Index: "idx"}))
	time.Sleep(300 * time.Millisecond)

	// The queued document belonged to the deleted index, not its replacement.
	got, err := s.GetIndex(ctx, &search.GetIndexRequest{Index: "idx"})
	require.NoError(t, err)
	assert.Zero(t, got.DocumentCount)
}

func TestReturnFieldsWithOverlappingPaths(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := NewInMemorySearch(logger.NewLogger("test"))
	require.NoError(t, s.Init(ctx, search.Metadata{}))
	require.NoError(t, s.CreateIndex(ctx, &search.CreateIndexRequest{Index: "idx"}))
	_, err := s.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: "idx", Documents: []search.Document{
		{ID: "d", Content: []byte(`{"a":{"b":1,"c":2},"year":2020}`)},
	}})
	require.NoError(t, err)

	for _, fields := range [][]string{{"a", "a.b"}, {"a.b", "a"}} {
		resp, err := s.Search(ctx, &search.SearchRequest{Index: "idx", Text: "2020", IncludeContent: true, ReturnFields: fields})
		require.NoError(t, err)
		require.Len(t, resp.Hits, 1)
		assert.JSONEq(t, `{"a":{"b":1,"c":2}}`, string(resp.Hits[0].Document.Content))
	}
}
