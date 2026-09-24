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

package opensearch_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	common "github.com/dapr/components-contrib/common/component/aws/opensearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	component "github.com/dapr/components-contrib/search/aws/opensearch"
	"github.com/dapr/kit/logger"
)

func properties(t *testing.T) map[string]string {
	t.Helper()
	endpoint := os.Getenv("OPENSEARCH_ENDPOINT")
	if endpoint == "" {
		t.Skip("OPENSEARCH_ENDPOINT is required for real OpenSearch certification")
	}
	return map[string]string{
		"endpoint": endpoint, "region": "us-east-1",
		"accessKey": "test", "secretKey": "test", "timeout": "3m",
	}
}

func newComponent(t *testing.T) search.Search {
	t.Helper()
	c := component.NewOpenSearch(logger.NewLogger("cert.search.opensearch"))
	require.NoError(t, c.Init(t.Context(), search.Metadata{Base: metadata.Base{Properties: properties(t)}}))
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	return c
}

func newIndex(t *testing.T, c search.Search) string {
	t.Helper()
	name := "cert-search-" + uuid.NewString()
	require.NoError(t, c.CreateIndex(t.Context(), &search.CreateIndexRequest{
		Index: name, Metadata: map[string]string{"settings": `{"number_of_shards":1,"number_of_replicas":0}`},
	}))
	t.Cleanup(func() {
		// Cleanup is independent of the test's canceled context and closed component.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cleanup := component.NewOpenSearch(logger.NewLogger("cert.search.cleanup"))
		if err := cleanup.Init(ctx, search.Metadata{Base: metadata.Base{Properties: properties(t)}}); !assert.NoError(t, err) {
			return
		}
		defer func() { assert.NoError(t, cleanup.Close()) }()
		assert.NoError(t, cleanup.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: name}))
	})
	return name
}

func waitOptions() search.IndexingOptions {
	return search.IndexingOptions{
		Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 4 * time.Minute,
		OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
	}
}

func content(t *testing.T, object map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(object)
	require.NoError(t, err)
	return data
}

func indexDocuments(t *testing.T, c search.Search, index string, docs []search.Document) {
	t.Helper()
	result, err := c.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: index, Documents: docs, Options: waitOptions()})
	require.NoError(t, err)
	require.Equal(t, search.IndexAckCompleted, result.Ack)
	require.Empty(t, result.FailedItems)
}

func query(t *testing.T, c search.Search, req *search.SearchRequest) *search.SearchResponse {
	t.Helper()
	result, err := c.Search(t.Context(), req)
	require.NoError(t, err)
	return result
}

func TestLargeBatchStablePagination(t *testing.T) {
	c := newComponent(t)
	index := newIndex(t, c)
	const count = 301
	docs := make([]search.Document, count)
	payload := strings.Repeat("x", 8192)
	for i := range docs {
		docs[i] = search.Document{
			ID: fmt.Sprintf("doc-%04d", i),
			Content: content(t, map[string]any{
				"title": "pagination certification", "group": i % 3, "payload": payload,
			}),
		}
	}
	indexDocuments(t, c, index, docs)
	stats, err := c.GetIndex(t.Context(), &search.GetIndexRequest{Index: index})
	require.NoError(t, err)
	assert.EqualValues(t, count, stats.DocumentCount)
	assert.Len(t, query(t, c, &search.SearchRequest{Index: index}).Hits, 20, "zero TopK defaults to 20")

	for _, direction := range []search.SortOrder{search.SortOrderAsc, search.SortOrderDesc} {
		t.Run(fmt.Sprint(direction), func(t *testing.T) {
			req := &search.SearchRequest{Index: index, Text: "certification", TopK: 37, Sort: []search.SortClause{{Field: "group", Order: direction}}}
			var ids []string
			tokens := make(map[string]bool)
			for page := 0; ; page++ {
				require.Less(t, page, 20, "pagination must terminate")
				result := query(t, c, req)
				require.NotNil(t, result.TotalHits)
				assert.EqualValues(t, count, *result.TotalHits)
				assert.Equal(t, search.TotalHitsRelationExact, result.TotalHitsRelation)
				for _, hit := range result.Hits {
					assert.Empty(t, hit.Document.Content)
					ids = append(ids, hit.Document.ID)
				}
				if result.ContinuationToken == "" {
					break
				}
				require.False(t, tokens[result.ContinuationToken], "cursor repeated")
				tokens[result.ContinuationToken] = true
				req.ContinuationToken = result.ContinuationToken
			}
			expected := make([]string, 0, count)
			for group := range 3 {
				if direction == search.SortOrderDesc {
					group = 2 - group
				}
				for i := range count {
					if i%3 == group {
						expected = append(expected, docs[i].ID)
					}
				}
			}
			assert.Equal(t, expected, ids, "non-unique sorts need stable ascending ID tie-breakers")
		})
	}
	got, err := c.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: index, IDs: []string{"doc-0300", "missing", "doc-0000"}, IncludeContent: true})
	require.NoError(t, err)
	require.Len(t, got.Documents, 2)
	assert.Equal(t, docs[300].Content, got.Documents[0].Content)
	assert.Equal(t, docs[0].Content, got.Documents[1].Content)
}

func TestExactContentAndOpaqueMetadata(t *testing.T) {
	c := newComponent(t)
	index := newIndex(t, c)
	raw := []byte(" {\n \"title\": \"\u65e5\u672c\u8a9e café 📚\", \"large\":9007199254740993, \"binary\":\"AAEC/w==\", \"escaped\":\"\\u0000\", \"number\":1.2300e+2 }\n")
	doc := search.Document{ID: "id/\u65e5\u672c\u8a9e? #📚", Content: raw, Metadata: map[string]string{
		"nested.field": "opaque\x00value", "unicode": "ภาษาไทย 📚", "json": `{"not":"indexed"}`, "empty": "",
	}}
	indexDocuments(t, c, index, []search.Document{doc})
	got, err := c.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: index, IDs: []string{doc.ID}, IncludeContent: true})
	require.NoError(t, err)
	require.Equal(t, []search.Document{doc}, got.Documents)
	result := query(t, c, &search.SearchRequest{Index: index, Text: "café", IncludeContent: true})
	require.Len(t, result.Hits, 1)
	assert.Equal(t, doc, result.Hits[0].Document)
	result = query(t, c, &search.SearchRequest{Index: index, IncludeContent: false})
	require.Len(t, result.Hits, 1)
	assert.Nil(t, result.Hits[0].Document.Content)
	assert.Equal(t, doc.Metadata, result.Hits[0].Document.Metadata)
	result = query(t, c, &search.SearchRequest{Index: index, Native: map[string]any{
		"query": map[string]any{"exists": map[string]any{"field": "metadata.unicode"}},
	}})
	assert.Empty(t, result.Hits, "opaque metadata must not be indexed")
}

func TestMappingConflictAttributionAndRecovery(t *testing.T) {
	c := newComponent(t)
	index := newIndex(t, c)
	indexDocuments(t, c, index, []search.Document{{ID: "schema", Content: []byte(`{"price":10,"title":"schema"}`)}})
	result, err := c.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{
		Index: index, Options: waitOptions(), Documents: []search.Document{
			{ID: "good-first", Content: []byte(`{"price":11,"title":"accepted"}`)},
			{ID: "bad-string", Content: []byte(`{"price":"not-a-number","title":"rejected"}`)},
			{ID: "good-last", Content: []byte(`{"price":12,"title":"accepted"}`)},
			{ID: "bad-object", Content: []byte(`{"price":{"nested":1},"title":"rejected"}`)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, result.Ack)
	require.Len(t, result.FailedItems, 2)
	failures := make(map[string]codes.Code)
	for _, failure := range result.FailedItems {
		require.NotNil(t, failure.Error)
		failures[failure.ID] = failure.Error.Code()
	}
	assert.Equal(t, map[string]codes.Code{"bad-string": codes.InvalidArgument, "bad-object": codes.InvalidArgument}, failures)
	got, err := c.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: index, IDs: []string{"bad-object", "good-last", "bad-string", "good-first"}})
	require.NoError(t, err)
	require.Len(t, got.Documents, 2)
	assert.Equal(t, "good-last", got.Documents[0].ID)
	assert.Equal(t, "good-first", got.Documents[1].ID)
	indexDocuments(t, c, index, []search.Document{
		{ID: "bad-string", Content: []byte(`{"price":13,"title":"recovered"}`)},
		{ID: "bad-object", Content: []byte(`{"price":14,"title":"recovered"}`)},
	})
	recovered := query(t, c, &search.SearchRequest{Index: index, Text: "recovered"})
	assert.Len(t, recovered.Hits, 2)
	stats, err := c.GetIndex(t.Context(), &search.GetIndexRequest{Index: index})
	require.NoError(t, err)
	assert.EqualValues(t, 5, stats.DocumentCount)
}

func TestWriteAcknowledgementsAndUnsupportedAsync(t *testing.T) {
	c := newComponent(t)
	index := newIndex(t, c)
	for i, options := range []search.IndexingOptions{{}, {Mode: search.IndexingModeReturnOnAcceptance}, waitOptions()} {
		id := strconv.Itoa(i)
		result, err := c.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: index, Options: options, Documents: []search.Document{{ID: id, Content: []byte(`{"title":"visible"}`)}}})
		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, result.Ack)
		require.Empty(t, result.FailedItems)
		assert.Len(t, query(t, c, &search.SearchRequest{Index: index, Text: "visible"}).Hits, 1, "completed writes must already be searchable")
		deleted, err := c.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: index, IDs: []string{id, id, "missing"}, Options: options})
		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, deleted.Ack)
		assert.Empty(t, query(t, c, &search.SearchRequest{Index: index}).Hits)
	}
	options := waitOptions()
	options.OnWaitTimeout = search.IndexingWaitTimeoutActionContinueAsync
	_, err := c.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: index, Options: options, Documents: []search.Document{{ID: "rejected", Content: []byte(`{}`)}}})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: index, Options: options, IDs: []string{"rejected"}})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Empty(t, query(t, c, &search.SearchRequest{Index: index}).Hits)
}

func TestPersistenceIsolationAndOwnership(t *testing.T) {
	first := newComponent(t)
	index := newIndex(t, first)
	other := newIndex(t, first)
	indexDocuments(t, first, index, []search.Document{{ID: "same", Content: []byte(`{"title":"persisted"}`)}})
	indexDocuments(t, first, other, []search.Document{{ID: "same", Content: []byte(`{"title":"isolated"}`)}})
	require.NoError(t, first.Close())
	second := newComponent(t)
	assert.Len(t, query(t, second, &search.SearchRequest{Index: index, Text: "persisted"}).Hits, 1)
	assert.Empty(t, query(t, second, &search.SearchRequest{Index: other, Text: "persisted"}).Hits)
	assert.Len(t, query(t, second, &search.SearchRequest{Index: other, Text: "isolated"}).Hits, 1)
	raw, err := common.NewClient(t.Context(), properties(t), logger.NewLogger("cert.search.raw"))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, raw.Close()) })
	for _, kind := range []string{"vector", ""} {
		name := "cert-search-foreign-" + uuid.NewString()
		mapping := map[string]any{"properties": map[string]any{}}
		if kind != "" {
			mapping["_meta"] = map[string]any{"dapr_kind": kind}
		}
		require.NoError(t, raw.Do(t.Context(), "PUT", "/"+name, map[string]any{"mappings": mapping}, nil))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			assert.NoError(t, raw.Do(ctx, "DELETE", "/"+name, nil, nil))
		})
		t.Run("ownership-"+kind, func(t *testing.T) {
			_, err = second.GetIndex(t.Context(), &search.GetIndexRequest{Index: name})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = second.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: name, Documents: []search.Document{{ID: "same", Content: []byte(`{}`)}}})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = second.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: name, IDs: []string{"same"}})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = second.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: name, IDs: []string{"same"}})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = second.Search(t.Context(), &search.SearchRequest{Index: name})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Equal(t, codes.FailedPrecondition, status.Code(second.DeleteIndex(t.Context(), &search.DeleteIndexRequest{Index: name})))
			list, listErr := second.ListIndexes(t.Context(), &search.ListIndexesRequest{})
			require.NoError(t, listErr)
			assert.NotContains(t, list.Indexes, name)
			require.NoError(t, raw.Do(t.Context(), "GET", "/"+name+"/_mapping", nil, nil), "barriers must not delete foreign data")
		})
	}
	missing := "cert-search-missing-" + uuid.NewString()
	_, err = second.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: missing, Documents: []search.Document{{ID: "one", Content: []byte(`{}`)}}})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, codes.NotFound, status.Code(raw.Do(t.Context(), "GET", "/"+missing+"/_mapping", nil, nil)), "bulk must not implicitly create indexes")
}

func TestNestedQueryAndStringMappings(t *testing.T) {
	c := newComponent(t)
	index := newIndex(t, c)
	long := strings.Repeat("\u9577", 300)
	for i, name := range []string{"Zulu", "Alpha", "Beta"} {
		indexDocuments(t, c, index, []search.Document{{ID: strconv.Itoa(i), Content: content(t, map[string]any{
			"title": "certification orchids", "author": map[string]any{"name": name, "age": 20 + i},
			"labels": []any{map[string]any{"name": name, "hidden": true}},
			"long":   long, "date": "2026-09-24", "secret": "unreturned",
		})}})
	}
	req := &search.SearchRequest{
		Index: index, Text: "orchids", SearchFields: []string{"title"}, TopK: 10,
		Filter: map[string]any{"$and": []any{
			map[string]any{"author.age": map[string]any{"$gte": 21}},
			map[string]any{"$or": []any{map[string]any{"author.name": "Alpha"}, map[string]any{"author.name": "Beta"}}},
		}},
		Sort:         []search.SortClause{{Field: "author.name", Order: search.SortOrderAsc}},
		ReturnFields: []string{"author.name", "labels.name"}, HighlightFields: []string{"title"},
	}
	result := query(t, c, req)
	require.Len(t, result.Hits, 2)
	assert.Equal(t, "1", result.Hits[0].Document.ID)
	assert.Equal(t, "2", result.Hits[1].Document.ID)
	assert.JSONEq(t, `{"author":{"name":"Alpha"},"labels":[{"name":"Alpha"}]}`, string(result.Hits[0].Document.Content))
	assert.Contains(t, result.Hits[0].Highlights["title"], "<em>orchids</em>")
	assert.NotContains(t, result.Hits[0].Highlights, "content.title")
	req.Sort = []search.SortClause{{Field: "author.age", Order: search.SortOrderDesc}}
	assert.Equal(t, "2", query(t, c, req).Hits[0].Document.ID)
	for _, filter := range []map[string]any{{"long": long}, {"date": "2026-09-24"}} {
		result = query(t, c, &search.SearchRequest{Index: index, Filter: filter})
		assert.Len(t, result.Hits, 3, "long and date-like strings must have searchable keyword subfields")
	}
	result = query(t, c, &search.SearchRequest{Index: index, Native: map[string]any{"query": map[string]any{
		"match_phrase": map[string]any{"content.title": "certification orchids"},
	}}, Filter: map[string]any{"author.name": "Beta"}, IncludeContent: true})
	require.Len(t, result.Hits, 1)
	assert.Equal(t, "2", result.Hits[0].Document.ID)
	// No mapping changes or duplicate data are required to filter a different date-shaped string.
	indexDocuments(t, c, index, []search.Document{{ID: "not-a-date", Content: []byte(`{"date":"not-a-date","title":"ordinary string"}`)}})
	assert.Len(t, query(t, c, &search.SearchRequest{Index: index, Filter: map[string]any{"date": "not-a-date"}}).Hits, 1)
}

func TestContinuationBoundaries(t *testing.T) {
	c := newComponent(t)
	index := newIndex(t, c)
	indexDocuments(t, c, index, []search.Document{
		{ID: "a", Content: []byte(`{"title":"cursor orchid","n":9007199254740992}`)},
		{ID: "b", Content: []byte(`{"title":"cursor orchid","n":9007199254740993}`)},
		{ID: "c", Content: []byte(`{"title":"cursor orchid","n":9007199254740994}`)},
	})
	base := search.SearchRequest{Index: index, Text: "orchid", TopK: 1, Sort: []search.SortClause{{Field: "n"}}}
	first := query(t, c, &base)
	require.NotEmpty(t, first.ContinuationToken)
	token := first.ContinuationToken
	next := base
	next.ContinuationToken = token
	second := query(t, c, &next)
	require.Len(t, second.Hits, 1)
	assert.Equal(t, "b", second.Hits[0].Document.ID, "large integer sort values must retain precision")
	next.ContinuationToken = second.ContinuationToken
	third := query(t, c, &next)
	require.Len(t, third.Hits, 1)
	assert.Equal(t, "c", third.Hits[0].Document.ID)
	assert.Empty(t, third.ContinuationToken)
	assert.Len(t, query(t, c, &search.SearchRequest{Index: index, TopK: 9999}).Hits, 3, "largest supported page size must succeed")
	data, err := base64.RawURLEncoding.DecodeString(token)
	require.NoError(t, err)
	var cursor map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &cursor))
	badTokens := make([]string, 0, 8)
	badTokens = append(badTokens, "not-base64!", strings.Repeat("x", 16385), base64.RawURLEncoding.EncodeToString(append(slices.Clone(data), []byte(" {}")...)))
	for _, after := range []string{`[]`, `[1]`, `[{}, "a"]`, `[[1], "a"]`, `null`} {
		cursor["a"] = json.RawMessage(after)
		encoded, marshalErr := json.Marshal(cursor)
		require.NoError(t, marshalErr)
		badTokens = append(badTokens, base64.RawURLEncoding.EncodeToString(encoded))
	}
	for i, bad := range badTokens {
		t.Run(fmt.Sprintf("malformed-%d", i), func(t *testing.T) {
			req := base
			req.ContinuationToken = bad
			_, err = c.Search(t.Context(), &req)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	for name, change := range map[string]func(*search.SearchRequest){
		"text":          func(r *search.SearchRequest) { r.Text = "cursor" },
		"size":          func(r *search.SearchRequest) { r.TopK = 2 },
		"content":       func(r *search.SearchRequest) { r.IncludeContent = true },
		"fields":        func(r *search.SearchRequest) { r.ReturnFields = []string{"title"} },
		"filter":        func(r *search.SearchRequest) { r.Filter = map[string]any{"title": "cursor orchid"} },
		"sort":          func(r *search.SearchRequest) { r.Sort = []search.SortClause{{Field: "n", Order: search.SortOrderDesc}} },
		"highlights":    func(r *search.SearchRequest) { r.HighlightFields = []string{"title"} },
		"search-fields": func(r *search.SearchRequest) { r.SearchFields = []string{"title"} },
	} {
		t.Run("bound-"+name, func(t *testing.T) {
			req := base
			req.ContinuationToken = token
			change(&req)
			_, err = c.Search(t.Context(), &req)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	_, err = c.Search(t.Context(), &search.SearchRequest{Index: index, TopK: 10000})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCancellationAndClose(t *testing.T) {
	c := newComponent(t)
	index := newIndex(t, c)
	indexDocuments(t, c, index, []search.Document{{ID: "one", Content: []byte(`{"title":"unchanged"}`)}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	operations := map[string]func(context.Context) error{
		"search": func(ctx context.Context) error {
			_, err := c.Search(ctx, &search.SearchRequest{Index: index})
			return err
		},
		"get": func(ctx context.Context) error {
			_, err := c.GetDocuments(ctx, &search.GetDocumentsRequest{Index: index, IDs: []string{"one"}})
			return err
		},
		"index": func(ctx context.Context) error {
			_, err := c.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: index, Documents: []search.Document{{ID: "two", Content: []byte(`{}`)}}})
			return err
		},
		"delete": func(ctx context.Context) error {
			_, err := c.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: index, IDs: []string{"one"}})
			return err
		},
		"get-index": func(ctx context.Context) error {
			_, err := c.GetIndex(ctx, &search.GetIndexRequest{Index: index})
			return err
		},
		"delete-index": func(ctx context.Context) error { return c.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: index}) },
		"list": func(ctx context.Context) error {
			_, err := c.ListIndexes(ctx, &search.ListIndexesRequest{})
			return err
		},
	}
	for name, call := range operations {
		t.Run("canceled-"+name, func(t *testing.T) { assert.Equal(t, codes.Canceled, status.Code(call(ctx))) })
	}
	expired, expireCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer expireCancel()
	for name, call := range operations {
		t.Run("deadline-"+name, func(t *testing.T) { assert.Equal(t, codes.DeadlineExceeded, status.Code(call(expired))) })
	}
	assert.Len(t, query(t, c, &search.SearchRequest{Index: index, Text: "unchanged"}).Hits, 1)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	for name, call := range operations {
		t.Run("closed-"+name, func(t *testing.T) { assert.Equal(t, codes.FailedPrecondition, status.Code(call(t.Context()))) })
	}
	reopened := newComponent(t)
	assert.Len(t, query(t, reopened, &search.SearchRequest{Index: index, Text: "unchanged"}).Hits, 1)
}
