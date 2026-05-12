//go:build certtests
// +build certtests

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

package meilisearch_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	searchmeilisearch "github.com/dapr/components-contrib/search/meilisearch"
	"github.com/dapr/kit/logger"
)

func TestMeilisearchSearchCertification(t *testing.T) {
	host := os.Getenv("MEILISEARCH_HOST")
	if host == "" {
		t.Skip("MEILISEARCH_HOST is required for Meilisearch certification tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	component := searchmeilisearch.NewMeilisearch(logger.NewLogger("cert"))
	require.NoError(t, component.Init(ctx, search.Metadata{Base: metadata.Base{Properties: map[string]string{"host": host, "apiKey": os.Getenv("MEILISEARCH_API_KEY")}}}))

	index := "cert-search-" + uuid.NewString()[:8]
	t.Cleanup(func() { _ = component.DropIndex(context.Background(), &search.DropIndexRequest{Index: index}) })

	t.Run("auth_failure", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("auth failure path panicked: %v", r)
			}
		}()

		bad := searchmeilisearch.NewMeilisearch(logger.NewLogger("cert.bad"))
		badCtx, badCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer badCancel()
		err := bad.Init(badCtx, search.Metadata{Base: metadata.Base{Properties: map[string]string{"host": host, "apiKey": "wrong-key"}}})
		if err == nil {
			_, err = bad.CreateIndex(badCtx, &search.CreateIndexRequest{Index: "cert-search-auth-" + uuid.NewString()[:8], Fields: searchCertificationFields()})
		}
		require.Error(t, err)
	})

	t.Run("component_metadata_contract", func(t *testing.T) {
		metadataProvider, ok := component.(interface{ GetComponentMetadata() metadata.MetadataMap })
		require.True(t, ok)
		assertMeilisearchMetadataContract(t, metadataProvider.GetComponentMetadata())
	})

	t.Run("unicode_content", func(t *testing.T) {
		unicodeIndex := "cert-search-unicode-" + uuid.NewString()[:8]
		t.Cleanup(func() { _ = component.DropIndex(context.Background(), &search.DropIndexRequest{Index: unicodeIndex}) })
		_, err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: unicodeIndex, Fields: searchCertificationFields()})
		require.NoError(t, err)

		docs := []search.Document{
			{ID: "unicode-doc-1", Content: map[string]any{"title": "현대 소나타 🚗", "body": "Hyundai Sonata sedan listing 🚗", "category": "sedan", "tag": "i18n", "price": float64(1)}},
			{ID: "unicode-doc-2", Content: map[string]any{"title": "Hyundai Sonata", "body": "ฮุนได โซนาต้า รถเก๋ง 🚗", "category": "sedan", "tag": "i18n", "price": float64(2)}},
		}
		resp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: unicodeIndex, Ack: search.IndexAckDurable, Documents: docs, IdempotencyKey: uuid.NewString()})
		require.NoError(t, err)
		requireSearchResultsSucceeded(t, resp.Results)

		searchResp, err := component.Search(ctx, &search.SearchRequest{Index: unicodeIndex, Text: "Hyundai", TopK: 5, IncludeContent: true})
		require.NoError(t, err)
		require.NotEmpty(t, searchResp.Hits)
		found := false
		for _, hit := range searchResp.Hits {
			if hit.Document.ID == "unicode-doc-1" || hit.Document.ID == "unicode-doc-2" {
				found = true
				break
			}
		}
		require.True(t, found, "expected a unicode document in search hits: %#v", searchResp.Hits)
	})

	t.Run("large_payload_batch", func(t *testing.T) {
		largeIndex := "cert-search-large-" + uuid.NewString()[:8]
		t.Cleanup(func() { _ = component.DropIndex(context.Background(), &search.DropIndexRequest{Index: largeIndex}) })
		size := indexLargestSearchBatch(t, ctx, component, largeIndex, 5000)
		if size < 5000 {
			t.Logf("meilisearch accepted %d documents in the largest successful single-call batch below 5000", size)
		}
		desc, err := component.DescribeIndex(ctx, &search.DescribeIndexRequest{Index: largeIndex})
		require.NoError(t, err)
		require.Equal(t, uint64(size), desc.DocumentCount)
	})

	t.Run("soak_no_leak", func(t *testing.T) {
		soakIndex := "cert-search-soak-" + uuid.NewString()[:8]
		t.Cleanup(func() { _ = component.DropIndex(context.Background(), &search.DropIndexRequest{Index: soakIndex}) })
		_, err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: soakIndex, Fields: searchCertificationFields()})
		require.NoError(t, err)

		runtime.GC()
		baseline := runtime.NumGoroutine()
		for i := range 50 {
			docs := make([]search.Document, 5)
			ids := make([]string, 5)
			for j := range docs {
				id := fmt.Sprintf("soak-%03d-%03d", i, j)
				ids[j] = id
				docs[j] = search.Document{ID: id, Content: map[string]any{"title": "soak car listing", "body": "short soak body", "category": "sedan", "tag": fmt.Sprintf("tag-%d", j), "price": float64(i*5 + j)}}
			}
			idxResp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: soakIndex, Ack: search.IndexAckDurable, Documents: docs, IdempotencyKey: uuid.NewString()})
			require.NoError(t, err)
			requireSearchResultsSucceeded(t, idxResp.Results)
			delResp, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: soakIndex, IDs: ids, Ack: search.IndexAckDurable})
			require.NoError(t, err)
			requireSearchResultsSucceeded(t, delResp.Results)
		}
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
		after := runtime.NumGoroutine()
		delta := after - baseline
		t.Logf("goroutines before=%d after=%d delta=%d", baseline, after, delta)
		require.LessOrEqual(t, delta, 20)
	})

	t.Run("create index", func(t *testing.T) {
		_, err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Fields: searchCertificationFields()})
		require.NoError(t, err)
	})

	t.Run("idempotent create", func(t *testing.T) {
		_, err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Fields: searchCertificationFields()})
		require.NoError(t, err)
	})

	t.Run("bulk index durable batches", func(t *testing.T) {
		docs := searchCertificationDocuments()
		for i := 0; i < len(docs); i += 25 {
			end := i + 25
			if end > len(docs) {
				end = len(docs)
			}
			resp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: index, Ack: search.IndexAckDurable, Documents: docs[i:end], IdempotencyKey: uuid.NewString()})
			require.NoError(t, err)
			require.Len(t, resp.Results, end-i)
			for _, result := range resp.Results {
				require.True(t, result.Success, result.ErrorMessage)
			}
		}
	})

	t.Run("describe", func(t *testing.T) {
		resp, err := component.DescribeIndex(ctx, &search.DescribeIndexRequest{Index: index})
		require.NoError(t, err)
		require.Equal(t, index, resp.Index)
		require.GreaterOrEqual(t, resp.DocumentCount, uint64(100))
	})

	t.Run("list", func(t *testing.T) {
		resp, err := component.ListIndexes(ctx, &search.ListIndexesRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Indexes, index)
	})

	t.Run("text search", func(t *testing.T) {
		resp, err := component.Search(ctx, &search.SearchRequest{Index: index, Text: "hyundai", TopK: 10, IncludeContent: true})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Hits)
	})

	t.Run("filter variants", func(t *testing.T) {
		tests := []struct {
			name    string
			filter  map[string]any
			wantErr bool
		}{
			{name: "eq", filter: map[string]any{"category": map[string]any{"$eq": "suv"}}},
			{name: "ne", filter: map[string]any{"category": map[string]any{"$ne": "missing"}}},
			{name: "gt", filter: map[string]any{"price": map[string]any{"$gt": 90}}},
			{name: "gte", filter: map[string]any{"price": map[string]any{"$gte": 99}}},
			{name: "lt", filter: map[string]any{"price": map[string]any{"$lt": 5}}},
			{name: "lte", filter: map[string]any{"price": map[string]any{"$lte": 0}}},
			{name: "in", filter: map[string]any{"category": map[string]any{"$in": []string{"suv", "sedan"}}}},
			{name: "nin", filter: map[string]any{"category": map[string]any{"$nin": []string{"missing"}}}},
			{name: "exists", filter: map[string]any{"optional": map[string]any{"$exists": true}}},
			{name: "and", filter: map[string]any{"$and": []any{map[string]any{"category": map[string]any{"$eq": "suv"}}, map[string]any{"price": map[string]any{"$gt": 90}}}}},
			{name: "or", filter: map[string]any{"$or": []any{map[string]any{"price": map[string]any{"$lt": 1}}, map[string]any{"price": map[string]any{"$gt": 98}}}}},
			{name: "not", filter: map[string]any{"$not": map[string]any{"category": map[string]any{"$eq": "suv"}}}},
			{name: "regex unsupported", filter: map[string]any{"title": map[string]any{"$regex": "hyundai"}}, wantErr: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := component.Search(ctx, &search.SearchRequest{Index: index, Filter: tt.filter, TopK: 10, IncludeContent: true})
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, resp.Hits)
			})
		}
	})

	t.Run("sort asc desc", func(t *testing.T) {
		asc, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 5, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderAsc}}})
		require.NoError(t, err)
		desc, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 5, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderDesc}}})
		require.NoError(t, err)
		require.NotEmpty(t, asc.Hits)
		require.NotEmpty(t, desc.Hits)
		require.LessOrEqual(t, asc.Hits[0].Document.Content["price"].(float64), asc.Hits[len(asc.Hits)-1].Document.Content["price"].(float64))
		require.GreaterOrEqual(t, desc.Hits[0].Document.Content["price"].(float64), desc.Hits[len(desc.Hits)-1].Document.Content["price"].(float64))
	})

	t.Run("pagination via continuation token", func(t *testing.T) {
		first, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 10, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderAsc}}})
		require.NoError(t, err)
		require.NotEmpty(t, first.ContinuationToken)
		second, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 10, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderAsc}}, ContinuationToken: first.ContinuationToken})
		require.NoError(t, err)
		require.NotEmpty(t, second.Hits)
		require.NotEqual(t, first.Hits[0].Document.ID, second.Hits[0].Document.ID)
	})

	t.Run("delete subset", func(t *testing.T) {
		ids := make([]string, 20)
		for i := range ids {
			ids[i] = fmt.Sprintf("car-%03d", i)
		}
		resp, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: index, IDs: ids, Ack: search.IndexAckDurable})
		require.NoError(t, err)
		require.Len(t, resp.Results, 20)
	})

	t.Run("search returns fewer after delete", func(t *testing.T) {
		resp, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 200, IncludeContent: true})
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Hits), 80)
	})

	t.Run("drop index", func(t *testing.T) {
		require.NoError(t, component.DropIndex(ctx, &search.DropIndexRequest{Index: index}))
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, component.Close()) })
}

func indexLargestSearchBatch(t *testing.T, ctx context.Context, component search.Search, index string, target int) int {
	t.Helper()

	for size := target; size >= 1; size /= 2 {
		_ = component.DropIndex(context.Background(), &search.DropIndexRequest{Index: index})
		_, err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Fields: searchCertificationFields()})
		require.NoError(t, err)
		docs := makeSearchBatch(size)
		resp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: index, Ack: search.IndexAckDurable, Documents: docs, IdempotencyKey: uuid.NewString()})
		if err == nil && !hasFailedSearchResult(resp.Results) {
			return size
		}
		t.Logf("single-call search batch size %d rejected: err=%v results=%#v", size, err, resultSample(resp))
	}
	t.Fatal("no successful search batch size found")
	return 0
}

func makeSearchBatch(size int) []search.Document {
	docs := make([]search.Document, size)
	for i := range docs {
		docs[i] = search.Document{ID: fmt.Sprintf("large-car-%05d", i), Content: map[string]any{"title": fmt.Sprintf("large car listing %05d", i), "body": "small provider agnostic batch car listing", "category": "sedan", "tag": fmt.Sprintf("tag-%d", i%10), "price": float64(i)}}
	}
	return docs
}

func resultSample(resp *search.IndexDocumentsResponse) []search.OperationResult {
	if resp == nil || len(resp.Results) == 0 {
		return nil
	}
	if len(resp.Results) < 3 {
		return resp.Results
	}
	return resp.Results[:3]
}

func requireSearchResultsSucceeded(t *testing.T, results []search.OperationResult) {
	t.Helper()
	require.NotEmpty(t, results)
	for _, result := range results {
		require.True(t, result.Success, result.ErrorMessage)
	}
}

func hasFailedSearchResult(results []search.OperationResult) bool {
	if len(results) == 0 {
		return true
	}
	for _, result := range results {
		if !result.Success {
			return true
		}
	}
	return false
}

func assertMeilisearchMetadataContract(t *testing.T, got metadata.MetadataMap) {
	t.Helper()

	actual := make(map[string]struct{}, len(got))
	for key := range got {
		actual[strings.ToLower(key)] = struct{}{}
	}

	mdType := reflect.TypeOf(commonmeilisearch.MeilisearchMetadata{})
	var missing []string
	for i := range mdType.NumField() {
		field := mdType.Field(i)
		if !field.IsExported() || field.Tag.Get("mapstructure") == "-" || strings.EqualFold(field.Tag.Get("mdignore"), "true") {
			continue
		}
		key := field.Name
		if tag := strings.Split(field.Tag.Get("mapstructure"), ",")[0]; tag != "" {
			key = tag
		}
		if _, ok := actual[strings.ToLower(key)]; !ok {
			missing = append(missing, key)
		}
	}
	require.Empty(t, missing, "component metadata missing fields: %s", strings.Join(missing, ", "))
}

func searchCertificationFields() []search.IndexFieldSchema {
	return []search.IndexFieldSchema{
		{Name: "title", Type: search.IndexFieldTypeText, Searchable: true, Filterable: true},
		{Name: "body", Type: search.IndexFieldTypeText, Searchable: true},
		{Name: "category", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "tag", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "optional", Type: search.IndexFieldTypeKeyword, Filterable: true},
		{Name: "price", Type: search.IndexFieldTypeDouble, Filterable: true, Sortable: true},
	}
}

func searchCertificationDocuments() []search.Document {
	docs := make([]search.Document, 100)
	titles := []string{
		"Hyundai Sonata",
		"현대 소나타",
		"ฮุนได โซนาต้า",
		"Toyota Camry",
		"Ford Ranger truck",
	}
	for i := range docs {
		category := "sedan"
		if i%2 == 0 {
			category = "suv"
		}
		content := map[string]any{
			"title":    fmt.Sprintf("%s %03d", titles[i%len(titles)], i),
			"body":     "certification car listing for portable search filters",
			"category": category,
			"tag":      fmt.Sprintf("tag-%d", i%5),
			"price":    float64(i),
		}
		if i%2 == 0 {
			content["optional"] = "present"
		}
		docs[i] = search.Document{ID: fmt.Sprintf("car-%03d", i), Content: content}
	}
	return docs
}
