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

package search

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/tests/conformance/utils"
)

type TestConfig struct{ utils.CommonConfig }

func NewTestConfig(componentName string) TestConfig {
	return TestConfig{CommonConfig: utils.CommonConfig{ComponentType: "search", ComponentName: componentName}}
}

func ConformanceTests(t *testing.T, props map[string]string, s search.Search, component string) {
	ctx := t.Context()

	t.Run("init", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, s.Init(c, search.Metadata{Base: metadata.Base{Properties: props}}))
	})
	if t.Failed() {
		t.Fatal("init failed")
	}

	t.Run("init negative", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, s.Init(c, search.Metadata{Base: metadata.Base{Properties: props}}))
	})

	indexName := "conf-" + component + "-" + randSuffix()
	cleanupIndex := true
	t.Cleanup(func() {
		if cleanupIndex {
			_ = s.DropIndex(ctx, &search.DropIndexRequest{Index: indexName})
		}
	})

	t.Run("create index", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: indexName, Fields: searchConformanceFields()})
		require.NoError(t, err)
	})

	t.Run("idempotent CreateIndex", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: indexName, Fields: searchConformanceFields()})
		require.NoError(t, err)

		resp, err := s.ListIndexes(c, &search.ListIndexesRequest{})
		require.NoError(t, err)
		assert.Equal(t, 1, countString(resp.Indexes, indexName))
	})

	t.Run("describe index", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := s.DescribeIndex(c, &search.DescribeIndexRequest{Index: indexName})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, indexName, resp.Index)
	})

	t.Run("list indexes contains it", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := s.ListIndexes(c, &search.ListIndexesRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Indexes, indexName)
	})

	t.Run("index documents durable", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: indexName, Ack: search.IndexAckDurable, Documents: searchConformanceDocuments()})
		require.NoError(t, err)
		require.Len(t, resp.Results, 5)
		assert.Equal(t, search.IndexAckDurable, resp.Ack)
		for _, result := range resp.Results {
			assert.True(t, result.Success, result.ErrorMessage)
		}
	})

	t.Run("ack queued", func(t *testing.T) {
		ackIndex := "conf-" + component + "-ackq-" + randSuffix()
		t.Cleanup(func() { _ = s.DropIndex(ctx, &search.DropIndexRequest{Index: ackIndex}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: ackIndex, Fields: searchConformanceFields()})
		require.NoError(t, err)
		resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: ackIndex, Ack: search.IndexAckQueued, Documents: []search.Document{{ID: "ack-queued", Content: map[string]any{"title": "Hyundai Sonata queued", "body": "queued car listing", "category": "sedan", "price": 11.0}}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, search.IndexAckQueued, resp.Ack)
	})

	t.Run("ack unspecified", func(t *testing.T) {
		ackIndex := "conf-" + component + "-acku-" + randSuffix()
		t.Cleanup(func() { _ = s.DropIndex(ctx, &search.DropIndexRequest{Index: ackIndex}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: ackIndex, Fields: searchConformanceFields()})
		require.NoError(t, err)
		resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: ackIndex, Ack: search.IndexAckUnspecified, Documents: []search.Document{{ID: "ack-unspecified", Content: map[string]any{"title": "Toyota Camry unspecified", "body": "unspecified car listing", "category": "sedan", "price": 12.0}}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, search.IndexAckUnspecified, resp.Ack)
	})

	t.Run("search by text", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{Text: "hyundai", TopK: 5, IncludeContent: true})
		require.NotEmpty(t, resp.Hits)
	})

	t.Run("search with filter eq", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{Text: "", Filter: map[string]any{"category": map[string]any{"$eq": "sedan"}}, TopK: 5, IncludeContent: true})
		require.NotEmpty(t, resp.Hits)
		for _, hit := range resp.Hits {
			assert.Equal(t, "sedan", hit.Document.Content["category"])
		}
	})

	t.Run("search with filter in and gt", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{Filter: map[string]any{"$and": []any{map[string]any{"category": map[string]any{"$in": []string{"sedan", "suv"}}}, map[string]any{"price": map[string]any{"$gt": 20}}}}, TopK: 5, IncludeContent: true})
		require.NotEmpty(t, resp.Hits)
		for _, hit := range resp.Hits {
			assert.Contains(t, []string{"sedan", "suv"}, hit.Document.Content["category"])
			assert.Greater(t, hit.Document.Content["price"].(float64), float64(20))
		}
	})

	t.Run("filter operator matrix", func(t *testing.T) {
		tests := []struct {
			name   string
			filter map[string]any
			check  func(t *testing.T, hits []search.Hit)
		}{
			{name: "$ne", filter: map[string]any{"category": map[string]any{"$ne": "sedan"}}, check: func(t *testing.T, hits []search.Hit) {
				for _, hit := range hits {
					assert.NotEqual(t, "sedan", hit.Document.Content["category"])
				}
			}},
			{name: "$lte", filter: map[string]any{"price": map[string]any{"$lte": 25}}, check: func(t *testing.T, hits []search.Hit) {
				for _, hit := range hits {
					assert.LessOrEqual(t, hit.Document.Content["price"].(float64), float64(25))
				}
			}},
			{name: "$exists", filter: map[string]any{"price": map[string]any{"$exists": true}}, check: func(t *testing.T, hits []search.Hit) {
				for _, hit := range hits {
					assert.Contains(t, hit.Document.Content, "price")
				}
			}},
			{name: "$or", filter: map[string]any{"$or": []any{map[string]any{"category": map[string]any{"$eq": "sedan"}}, map[string]any{"price": map[string]any{"$gt": 45}}}}, check: func(t *testing.T, hits []search.Hit) {
				for _, hit := range hits {
					assert.True(t, hit.Document.Content["category"] == "sedan" || hit.Document.Content["price"].(float64) > 45)
				}
			}},
			{name: "$not", filter: map[string]any{"$not": map[string]any{"category": map[string]any{"$eq": "sedan"}}}, check: func(t *testing.T, hits []search.Hit) {
				for _, hit := range hits {
					assert.NotEqual(t, "sedan", hit.Document.Content["category"])
				}
			}},
			{name: "$regex", filter: map[string]any{"title": map[string]any{"$regex": "Hyundai"}}, check: func(t *testing.T, hits []search.Hit) {
				for _, hit := range hits {
					assert.Contains(t, fmt.Sprint(hit.Document.Content["title"]), "Hyundai")
				}
			}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				resp, err := s.Search(c, &search.SearchRequest{Index: indexName, Filter: tt.filter, TopK: 10, IncludeContent: true})
				if err != nil && strings.Contains(strings.ToLower(err.Error()), "unsupported") {
					t.Skipf("provider does not support %s filter: %v", tt.name, err)
				}
				require.NoError(t, err)
				require.NotEmpty(t, resp.Hits)
				tt.check(t, resp.Hits)
			})
		}
	})

	t.Run("field type matrix", func(t *testing.T) {
		tests := []struct {
			name  string
			field search.IndexFieldSchema
			doc   search.Document
			req   search.SearchRequest
			check func(t *testing.T, hits []search.Hit)
		}{
			{
				name:  "bool",
				field: search.IndexFieldSchema{Name: "inStock", Type: search.IndexFieldTypeBool, Filterable: true},
				doc:   search.Document{ID: "typed-bool", Content: map[string]any{"title": "Hyundai Sonata in stock", "body": "typed car listing", "category": "sedan", "price": 1.0, "inStock": true}},
				req:   search.SearchRequest{Filter: map[string]any{"inStock": map[string]any{"$eq": true}}, TopK: 5, IncludeContent: true},
				check: func(t *testing.T, hits []search.Hit) { assert.Equal(t, true, hits[0].Document.Content["inStock"]) },
			},
			{
				name:  "datetime",
				field: search.IndexFieldSchema{Name: "listedAt", Type: search.IndexFieldTypeDateTime, Filterable: true, Sortable: true},
				doc:   search.Document{ID: "typed-datetime", Content: map[string]any{"title": "Toyota Camry listed", "body": "typed car listing", "category": "sedan", "price": 1.0, "listedAt": "2026-01-02T03:04:05Z"}},
				req:   search.SearchRequest{Filter: map[string]any{"listedAt": map[string]any{"$gte": "2026-01-01T00:00:00Z"}}, Sort: []search.SortClause{{Field: "listedAt", Order: search.SortOrderAsc}}, TopK: 5, IncludeContent: true},
				check: func(t *testing.T, hits []search.Hit) {
					assert.Equal(t, "2026-01-02T03:04:05Z", hits[0].Document.Content["listedAt"])
				},
			},
			{
				name:  "geopoint",
				field: search.IndexFieldSchema{Name: "dealership", Type: search.IndexFieldTypeGeoPoint, Filterable: true},
				doc:   search.Document{ID: "typed-geo", Content: map[string]any{"title": "Ford Mustang at dealership", "body": "typed car listing", "category": "sedan", "price": 1.0, "dealership": map[string]any{"lat": 47.61, "lng": -122.33}}},
				req:   search.SearchRequest{Filter: map[string]any{"dealership": map[string]any{"$exists": true}}, TopK: 5, IncludeContent: true},
				check: func(t *testing.T, hits []search.Hit) { assert.Contains(t, hits[0].Document.Content, "dealership") },
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				typedIndex := "conf-" + component + "-typed-" + tt.name + "-" + randSuffix()
				t.Cleanup(func() { _ = s.DropIndex(ctx, &search.DropIndexRequest{Index: typedIndex}) })

				fields := append(searchConformanceFields(), tt.field)
				c, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: typedIndex, Fields: fields})
				if skipIfUnsupported(t, err, "create index with "+tt.name) {
					return
				}
				require.NoError(t, err)

				resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: typedIndex, Ack: search.IndexAckDurable, Documents: []search.Document{tt.doc}})
				if skipIfUnsupported(t, err, "index "+tt.name+" document") {
					return
				}
				require.NoError(t, err)
				require.Len(t, resp.Results, 1)
				require.True(t, resp.Results[0].Success, resp.Results[0].ErrorMessage)

				searchReq := tt.req
				searchResp := requireSearch(t, s, typedIndex, &searchReq)
				require.NotEmpty(t, searchResp.Hits)
				tt.check(t, searchResp.Hits)
			})
		}
	})

	t.Run("idempotency key resubmit", func(t *testing.T) {
		idempotentIndex := "conf-" + component + "-idem-" + randSuffix()
		t.Cleanup(func() { _ = s.DropIndex(ctx, &search.DropIndexRequest{Index: idempotentIndex}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: idempotentIndex, Fields: searchConformanceFields()})
		require.NoError(t, err)

		docs := []search.Document{
			{ID: "idem-1", Content: map[string]any{"title": "Hyundai Sonata idempotent one", "body": "idempotent car listing", "category": "sedan", "price": 1.0}},
			{ID: "idem-2", Content: map[string]any{"title": "Toyota Camry idempotent two", "body": "idempotent car listing", "category": "sedan", "price": 2.0}},
		}
		key := "idem-" + randSuffix()
		for range 2 {
			resp, indexErr := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: idempotentIndex, Ack: search.IndexAckDurable, Documents: docs, IdempotencyKey: key})
			require.NoError(t, indexErr)
			require.Len(t, resp.Results, len(docs))
			for _, result := range resp.Results {
				assert.True(t, result.Success, result.ErrorMessage)
			}
		}
		desc, err := s.DescribeIndex(c, &search.DescribeIndexRequest{Index: idempotentIndex})
		require.NoError(t, err)
		if desc.DocumentCount == 0 {
			t.Skip("provider returned zero document count; cannot verify idempotency count best-effort")
		}
		assert.Equal(t, uint64(len(docs)), desc.DocumentCount)
	})

	t.Run("pagination via continuation token", func(t *testing.T) {
		pageIndex := "conf-" + component + "-page-" + randSuffix()
		t.Cleanup(func() { _ = s.DropIndex(ctx, &search.DropIndexRequest{Index: pageIndex}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: pageIndex, Fields: searchConformanceFields()})
		require.NoError(t, err)
		docs := make([]search.Document, 12)
		wantIDs := make([]string, len(docs))
		for i := range docs {
			id := fmt.Sprintf("page-%02d", i)
			wantIDs[i] = id
			docs[i] = search.Document{ID: id, Content: map[string]any{"title": fmt.Sprintf("car listing %02d", i), "body": "pagination", "category": "page", "price": float64(i)}}
		}
		_, err = s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: pageIndex, Ack: search.IndexAckDurable, Documents: docs})
		require.NoError(t, err)

		gotIDs := make([]string, 0, len(docs))
		token := ""
		for {
			resp, err := s.Search(c, &search.SearchRequest{Index: pageIndex, TopK: 5, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderAsc}}, ContinuationToken: token})
			require.NoError(t, err)
			if resp.TotalHits != 0 {
				assert.Equal(t, uint64(len(docs)), resp.TotalHits)
			}
			for _, hit := range resp.Hits {
				gotIDs = append(gotIDs, hit.Document.ID)
			}
			if resp.ContinuationToken == "" {
				break
			}
			token = resp.ContinuationToken
		}
		sort.Strings(gotIDs)
		assert.Equal(t, wantIDs, gotIDs)
	})

	t.Run("multi-field sort + score ordering", func(t *testing.T) {
		sortIndex := "conf-" + component + "-sort-" + randSuffix()
		t.Cleanup(func() { _ = s.DropIndex(ctx, &search.DropIndexRequest{Index: sortIndex}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: sortIndex, Fields: searchConformanceFields()})
		require.NoError(t, err)
		docs := []search.Document{
			{ID: "sort-c", Content: map[string]any{"title": "Toyota Camry", "body": "sort", "category": "sort", "price": 20.0}},
			{ID: "sort-a", Content: map[string]any{"title": "Honda Civic", "body": "sort", "category": "sort", "price": 20.0}},
			{ID: "sort-b", Content: map[string]any{"title": "Ford Fiesta", "body": "sort", "category": "sort", "price": 10.0}},
		}
		_, err = s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: sortIndex, Ack: search.IndexAckDurable, Documents: docs})
		require.NoError(t, err)
		resp := requireSearch(t, s, sortIndex, &search.SearchRequest{TopK: 10, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderDesc}, {Field: "title", Order: search.SortOrderAsc}}})
		require.Len(t, resp.Hits, len(docs))
		for i := 1; i < len(resp.Hits); i++ {
			prev := resp.Hits[i-1].Document.Content
			cur := resp.Hits[i].Document.Content
			prevPrice := prev["price"].(float64)
			curPrice := cur["price"].(float64)
			if prevPrice == curPrice {
				assert.LessOrEqual(t, fmt.Sprint(prev["title"]), fmt.Sprint(cur["title"]))
				continue
			}
			assert.GreaterOrEqual(t, prevPrice, curPrice)
		}
	})

	t.Run("search fields restriction", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{Text: "second", SearchFields: []string{"title"}, TopK: 5, IncludeContent: true})
		assert.Empty(t, resp.Hits)
	})

	t.Run("native passthrough", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.Search(c, &search.SearchRequest{Index: indexName, Text: "hyundai", TopK: 1, Native: map[string]any{"_dummy": true}})
		if err == nil {
			return
		}
		msg := strings.ToLower(err.Error())
		assert.True(t, strings.Contains(msg, "not supported") || strings.Contains(msg, "native"), "unexpected native passthrough error: %v", err)
	})

	t.Run("search with sort and topK", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{TopK: 2, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderDesc}}})
		require.Len(t, resp.Hits, 2)
		assert.GreaterOrEqual(t, resp.Hits[0].Document.Content["price"].(float64), resp.Hits[1].Document.Content["price"].(float64))
	})

	t.Run("search returnFields subset", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{Text: "hyundai", TopK: 1, IncludeContent: true, ReturnFields: []string{"title"}})
		require.NotEmpty(t, resp.Hits)
		assert.Contains(t, resp.Hits[0].Document.Content, "title")
		assert.NotContains(t, resp.Hits[0].Document.Content, "price")
	})

	t.Run("search includeContent false", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{Text: "hyundai", TopK: 1, IncludeContent: false})
		require.NotEmpty(t, resp.Hits)
		assert.Empty(t, resp.Hits[0].Document.Content)
	})

	t.Run("search highlights", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{Text: "hyundai", TopK: 1, IncludeContent: true, HighlightFields: []string{"title"}})
		require.NotEmpty(t, resp.Hits)
		assert.NotEmpty(t, resp.Hits[0].Highlights)
	})

	t.Run("concurrent indexing+search race-safety", func(t *testing.T) {
		concurrentIndex := "conf-" + component + "-conc-" + randSuffix()
		t.Cleanup(func() { _ = s.DropIndex(ctx, &search.DropIndexRequest{Index: concurrentIndex}) })
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := s.CreateIndex(c, &search.CreateIndexRequest{Index: concurrentIndex, Fields: searchConformanceFields()})
		require.NoError(t, err)
		_, err = s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: concurrentIndex, Ack: search.IndexAckDurable, Documents: []search.Document{{ID: "seed", Content: map[string]any{"title": "Hyundai seed car", "body": "seed", "category": "seed", "price": 0.0}}}})
		require.NoError(t, err)

		var wg sync.WaitGroup
		errCh := make(chan error, 16)
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: concurrentIndex, Ack: search.IndexAckDurable, Documents: []search.Document{{ID: fmt.Sprintf("conc-%02d", i), Content: map[string]any{"title": fmt.Sprintf("Hyundai concurrent %02d", i), "body": "concurrent", "category": "race", "price": float64(i)}}}})
				if err != nil {
					errCh <- err
				}
			}()
		}
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.Search(ctx, &search.SearchRequest{Index: concurrentIndex, Text: "concurrent", TopK: 5, IncludeContent: true})
				if err != nil {
					errCh <- err
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			assert.NoError(t, err)
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := s.Search(canceled, &search.SearchRequest{Index: indexName, Text: "alpha", TopK: 1})
		assertContextCanceled(t, err)

		_, err = s.IndexDocuments(canceled, &search.IndexDocumentsRequest{Index: indexName, Ack: search.IndexAckDurable, Documents: []search.Document{{ID: "cancelled", Content: map[string]any{"title": "Hyundai cancelled car", "body": "cancelled", "category": "sedan", "price": 13.0}}}})
		assertContextCanceled(t, err)
	})

	t.Run("delete documents", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resp, err := s.DeleteDocuments(c, &search.DeleteDocumentsRequest{Index: indexName, IDs: []string{"car-1", "car-2"}, Ack: search.IndexAckDurable})
		require.NoError(t, err)
		require.Len(t, resp.Results, 2)
		for _, result := range resp.Results {
			assert.True(t, result.Success, result.ErrorMessage)
		}
	})

	t.Run("search after delete returns fewer", func(t *testing.T) {
		resp := requireSearch(t, s, indexName, &search.SearchRequest{TopK: 10, IncludeContent: true})
		assert.LessOrEqual(t, len(resp.Hits), 3)
	})

	t.Run("drop index", func(t *testing.T) {
		require.NoError(t, s.DropIndex(ctx, &search.DropIndexRequest{Index: indexName}))
		cleanupIndex = false
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, s.Close()) })

	t.Run("post-close calls error", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("post-close calls must return errors, not panic: %v", r)
			}
		}()

		_, err := s.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: indexName, Documents: []search.Document{{ID: "post-close", Content: map[string]any{"title": "post close"}}}})
		require.Error(t, err)
		_, err = s.Search(ctx, &search.SearchRequest{Index: indexName, Text: "post"})
		require.Error(t, err)
	})
}

func searchConformanceFields() []search.IndexFieldSchema {
	return []search.IndexFieldSchema{
		{Name: "title", Type: search.IndexFieldTypeText, Searchable: true, Sortable: true},
		{Name: "body", Type: search.IndexFieldTypeText, Searchable: true},
		{Name: "category", Type: search.IndexFieldTypeKeyword, Filterable: true, Searchable: true},
		{Name: "price", Type: search.IndexFieldTypeDouble, Filterable: true, Sortable: true},
	}
}

func searchConformanceDocuments() []search.Document {
	return []search.Document{
		{ID: "car-1", Content: map[string]any{"title": "Hyundai Sonata", "body": "first hyundai sedan listing", "category": "sedan", "price": 10.0}},
		{ID: "car-2", Content: map[string]any{"title": "Toyota Camry", "body": "second sedan listing", "category": "sedan", "price": 25.0}},
		{ID: "car-3", Content: map[string]any{"title": "Hyundai Tucson", "body": "hyundai suv reference", "category": "suv", "price": 30.0}},
		{ID: "car-4", Content: map[string]any{"title": "Ford Ranger", "body": "truck listing", "category": "truck", "price": 40.0}},
		{ID: "car-5", Content: map[string]any{"title": "Hyundai Palisade", "body": "another hyundai suv note", "category": "suv", "price": 50.0}},
	}
}

func requireSearch(t *testing.T, s search.Search, indexName string, req *search.SearchRequest) *search.SearchResponse {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	req.Index = indexName
	resp, err := s.Search(c, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func countString(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func skipIfUnsupported(t *testing.T, err error, action string) bool {
	t.Helper()
	if err == nil {
		return false
	}
	if strings.Contains(strings.ToLower(err.Error()), "not supported") {
		t.Skipf("provider does not support %s: %v", action, err)
		return true
	}
	return false
}

func assertContextCanceled(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	if errors.Is(err, context.Canceled) {
		return
	}
	assert.Contains(t, strings.ToLower(err.Error()), "context canceled")
}

func randSuffix() string { return uuid.NewString()[:8] }
