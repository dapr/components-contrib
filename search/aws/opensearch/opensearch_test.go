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

package opensearch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	contribmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/kit/logger"
)

func newTestComponent(t *testing.T, handler http.HandlerFunc, customMapping ...bool) *OpenSearch {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "AWS4-HMAC-SHA256")
		assert.Contains(t, r.Header.Get("Authorization"), "test-access/")
		assert.Contains(t, r.Header.Get("Authorization"), "/us-east-1/es/aws4_request")
		assert.NotEmpty(t, r.Header.Get("X-Amz-Date"))
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/books/_mapping" && len(customMapping) == 0 {
			writeJSON(t, w, map[string]any{"books": map[string]any{"mappings": map[string]any{"_meta": map[string]any{"dapr_kind": "search"}}}})
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	component := NewOpenSearch(logger.NewLogger("opensearch-test")).(*OpenSearch)
	require.NoError(t, component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{
		"endpoint": server.URL, "region": "us-east-1", "accessKey": "test-access", "secretKey": "test-secret",
	}}}))
	t.Cleanup(func() { require.NoError(t, component.Close()) })
	return component
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	assert.NoError(t, json.NewEncoder(w).Encode(value))
}

func source(t *testing.T, id, content string) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(envelope{ID: id, Content: json.RawMessage(content), Raw: []byte(content), Metadata: map[string]string{"opaque": "value"}})
	require.NoError(t, err)
	return data
}

func TestLifecycle(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.Method + " " + r.URL.Path {
		case "PUT /books":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			mappings := body["mappings"].(map[string]any)
			assert.Equal(t, map[string]any{"dapr_kind": "search"}, mappings["_meta"])
			assert.Equal(t, false, mappings["date_detection"])
			properties := mappings["properties"].(map[string]any)
			assert.Equal(t, map[string]any{"enabled": false, "type": "object"}, properties["metadata"])
			assert.Equal(t, map[string]any{"type": "binary"}, properties["raw"])
			assert.Contains(t, mappings["dynamic_templates"].([]any)[0].(map[string]any), "strings")
			assert.Equal(t, map[string]any{"number_of_shards": float64(1)}, body["settings"])
			writeJSON(t, w, map[string]any{"acknowledged": true})
		case "GET /books/_count":
			writeJSON(t, w, map[string]any{"count": 7})
		case "GET /_mapping":
			writeJSON(t, w, map[string]any{
				"books":   map[string]any{"mappings": map[string]any{"_meta": map[string]any{"dapr_kind": "search"}}},
				"vector":  map[string]any{"mappings": map[string]any{"_meta": map[string]any{"dapr_kind": "vector"}}},
				"foreign": map[string]any{"mappings": map[string]any{}},
			})
		case "DELETE /books":
			writeJSON(t, w, map[string]any{"acknowledged": true})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	require.NoError(t, component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "books", Metadata: map[string]string{"settings": `{"number_of_shards":1}`}}))
	index, err := component.GetIndex(t.Context(), &search.GetIndexRequest{Index: "books"})
	require.NoError(t, err)
	assert.Equal(t, uint64(7), index.DocumentCount)
	assert.Equal(t, "books", index.Index)
	indexes, err := component.ListIndexes(t.Context(), &search.ListIndexesRequest{})
	require.NoError(t, err)
	assert.Equal(t, []string{"books"}, indexes.Indexes)
	require.NoError(t, component.DeleteIndex(t.Context(), &search.DeleteIndexRequest{Index: "books"}))
	assert.EqualValues(t, 4, requests.Load())
	require.NoError(t, component.Close())
	require.NoError(t, component.Close())
	_, err = component.GetIndex(t.Context(), &search.GetIndexRequest{Index: "books"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestDocumentRoundTrip(t *testing.T) {
	t.Parallel()
	content := " { \"title\" : \"Dapr\", \"large\":9007199254740993, \"nested\":{\"author\":\"Ada\"} }\n"
	var stored json.RawMessage
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/books/_bulk":
			assert.Equal(t, http.MethodPut, r.Method)
			assert.Equal(t, "wait_for", r.URL.Query().Get("refresh"))
			assert.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
			decoder := json.NewDecoder(r.Body)
			var action map[string]map[string]string
			require.NoError(t, decoder.Decode(&action))
			if _, ok := action["index"]; ok {
				assert.Equal(t, "one", action["index"]["_id"])
				require.NoError(t, decoder.Decode(&stored))
				var doc envelope
				require.NoError(t, json.Unmarshal(stored, &doc))
				assert.Equal(t, content, string(doc.Raw))
				assert.JSONEq(t, content, string(doc.Content))
				assert.Equal(t, map[string]string{"private": "a", "dots.and spaces": "b"}, doc.Metadata)
				writeJSON(t, w, map[string]any{"items": []any{map[string]any{"index": map[string]any{"_id": "one", "status": 201}}}})
			} else {
				assert.Equal(t, "missing", action["delete"]["_id"])
				writeJSON(t, w, map[string]any{"items": []any{map[string]any{"delete": map[string]any{"_id": "missing", "status": 404, "result": "not_found"}}}})
			}
		case "/books/_mget":
			writeJSON(t, w, map[string]any{"docs": []any{
				map[string]any{"_id": "missing", "found": false},
				map[string]any{"_id": "one", "found": true, "_source": stored},
			}})
		default:
			t.Errorf("unexpected request %s", r.URL)
		}
	})
	response, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{{
		ID: "one", Content: []byte(content), Metadata: map[string]string{"private": "a", "dots.and spaces": "b"},
	}}})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, response.Ack)
	assert.Empty(t, response.FailedItems)
	for _, include := range []bool{true, false} {
		got, getErr := component.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: "books", IDs: []string{"missing", "one"}, IncludeContent: include})
		require.NoError(t, getErr)
		require.Len(t, got.Documents, 1)
		assert.Equal(t, "one", got.Documents[0].ID)
		assert.Equal(t, map[string]string{"private": "a", "dots.and spaces": "b"}, got.Documents[0].Metadata)
		if include {
			assert.Equal(t, []byte(content), got.Documents[0].Content)
		} else {
			assert.Nil(t, got.Documents[0].Content)
		}
	}
	deleted, err := component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"missing"}})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, deleted.Ack)
}

func TestWriteFailures(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"index", "delete"} {
		t.Run(operation, func(t *testing.T) {
			component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
				writeJSON(t, w, map[string]any{"errors": true, "items": []any{
					map[string]any{operation: map[string]any{"_id": "one", "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "invalid field"}}},
				}})
			})
			if operation == "index" {
				result, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{{ID: "one", Content: []byte(`{}`)}}})
				require.NoError(t, err)
				require.Len(t, result.FailedItems, 1)
				assert.Equal(t, "one", result.FailedItems[0].ID)
				assert.Equal(t, codes.InvalidArgument, result.FailedItems[0].Error.Code())
				assert.Equal(t, search.IndexAckCompleted, result.Ack)
			} else {
				result, err := component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"one"}})
				assert.Nil(t, result)
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
			}
		})
	}
}

func TestSearchPaginationProjectionHighlights(t *testing.T) {
	t.Parallel()
	var page atomic.Int32
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/books/_search", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.EqualValues(t, 3, body["size"])
		assert.Equal(t, true, body["track_scores"])
		assert.Equal(t, true, body["track_total_hits"])
		assert.Equal(t, []any{map[string]any{"_score": "desc"}, map[string]any{"id": "asc"}}, body["sort"])
		assert.Equal(t, map[string]any{"fields": map[string]any{"content.title": map[string]any{}}}, body["highlight"])
		query := body["query"].(map[string]any)["bool"].(map[string]any)
		multi := query["must"].([]any)[0].(map[string]any)["multi_match"].(map[string]any)
		assert.Equal(t, "Dapr", multi["query"])
		assert.Equal(t, []any{"content.title"}, multi["fields"])
		assert.NotEmpty(t, query["filter"])
		hits := []any{}
		if page.Add(1) == 1 {
			assert.NotContains(t, body, "search_after")
			for _, id := range []string{"a", "b", "c"} {
				hits = append(hits, map[string]any{"_id": id, "_source": source(t, id, `{"title":"Dapr","nested":{"author":"Ada","ignored":42},"big":9007199254740993}`), "_score": 1.5, "sort": []any{1.5, id}, "highlight": map[string]any{"content.title": []string{"<em>Dapr</em>"}}})
			}
		} else {
			assert.Equal(t, []any{1.5, "b"}, body["search_after"])
			hits = append(hits, map[string]any{"_id": "c", "_source": source(t, "c", `{"title":"Dapr"}`), "_score": 1.5, "sort": []any{1.5, "c"}})
		}
		writeJSON(t, w, map[string]any{"hits": map[string]any{"total": map[string]any{"value": 3, "relation": "eq"}, "hits": hits}})
	})
	req := &search.SearchRequest{Index: "books", Text: "Dapr", TopK: 2, SearchFields: []string{"title"}, ReturnFields: []string{"nested.author", "big"}, HighlightFields: []string{"title"}, Filter: map[string]any{"title": map[string]any{"$eq": "Dapr"}}}
	first, err := component.Search(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, first.Hits, 2)
	assert.JSONEq(t, `{"nested":{"author":"Ada"},"big":9007199254740993}`, string(first.Hits[0].Document.Content))
	assert.Equal(t, map[string]string{"opaque": "value"}, first.Hits[0].Document.Metadata)
	assert.Equal(t, 1.5, first.Hits[0].Score)
	assert.Equal(t, map[string]string{"title": "<em>Dapr</em>"}, first.Hits[0].Highlights)
	assert.EqualValues(t, 3, *first.TotalHits)
	assert.Equal(t, search.TotalHitsRelationExact, first.TotalHitsRelation)
	require.NotEmpty(t, first.ContinuationToken)
	req.ContinuationToken = first.ContinuationToken
	second, err := component.Search(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, second.Hits, 1)
	assert.Equal(t, "c", second.Hits[0].Document.ID)
	assert.Empty(t, second.ContinuationToken)
	req.Text = "changed"
	_, err = component.Search(t.Context(), req)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.EqualValues(t, 2, page.Load())
}

func TestSortMappingAndNative(t *testing.T) {
	t.Parallel()
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_mapping") {
			writeJSON(t, w, map[string]any{"books": map[string]any{"mappings": map[string]any{"_meta": map[string]any{"dapr_kind": "search"}, "properties": map[string]any{"content": map[string]any{"properties": map[string]any{
				"title": map[string]any{"type": "text", "fields": map[string]any{"keyword": map[string]any{"type": "keyword"}}},
				"year":  map[string]any{"type": "long"},
			}}}}}})
			return
		}
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, map[string]any{"match": map[string]any{"content.title": "native"}}, body["query"])
		assert.Equal(t, []any{
			map[string]any{"content.title.keyword": map[string]any{"order": "asc", "unmapped_type": "keyword"}},
			map[string]any{"content.year": map[string]any{"order": "desc", "unmapped_type": "keyword"}},
			map[string]any{"id": "asc"},
		}, body["sort"])
		writeJSON(t, w, map[string]any{"hits": map[string]any{"total": map[string]any{"value": 12, "relation": "gte"}, "hits": []any{
			map[string]any{"_id": "one", "_source": source(t, "one", " { \"title\" : \"native\" } "), "_score": nil},
		}}})
	}, true)
	req := &search.SearchRequest{Index: "books", Native: map[string]any{"query": map[string]any{"match": map[string]any{"content.title": "native"}}}, Sort: []search.SortClause{{Field: "title"}, {Field: "year", Order: search.SortOrderDesc}}}
	result, err := component.Search(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, result.Hits, 1)
	assert.Nil(t, result.Hits[0].Document.Content)
	assert.Equal(t, search.TotalHitsRelationLowerBound, result.TotalHitsRelation)
	req.IncludeContent = true
	result, err = component.Search(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, []byte(" { \"title\" : \"native\" } "), result.Hits[0].Document.Content) //nolint:testifylint // Verify byte-for-byte preservation, including whitespace.
}

func TestRequestValidation(t *testing.T) {
	t.Parallel()
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("invalid request reached provider: %s", r.URL)
	})
	for _, name := range []string{"", "*", "a,b", "../books", "books/_search"} {
		t.Run("index/"+name, func(t *testing.T) {
			err := component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: name})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	for _, documents := range [][]search.Document{
		{{ID: "", Content: []byte(`{}`)}},
		{{ID: "a", Content: []byte(`{}`)}, {ID: "a", Content: []byte(`{}`)}},
		{{ID: "a", Content: []byte(`null`)}},
		{{ID: "a", Content: []byte(`[]`)}},
		{{ID: "a", Content: []byte(`{"broken":`)}},
	} {
		_, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Documents: documents})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	for _, options := range []search.IndexingOptions{
		{Mode: search.IndexingModeWaitForCompletion},
		{Mode: search.IndexingModeReturnOnAcceptance, WaitTimeout: time.Second},
		{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync},
	} {
		_, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Options: options})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		_, err = component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", Options: options})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	for _, md := range []map[string]string{{"unknown": "x"}, {"settings": `[]`}, {"settings": `null`}, {"settings": "invalid"}} {
		assert.Equal(t, codes.InvalidArgument, status.Code(component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "books", Metadata: md})))
	}
	for _, req := range []*search.SearchRequest{
		{Text: "text", Native: map[string]any{"query": map[string]any{"match_all": map[string]any{}}}},
		{TopK: 10000},
		{SearchFields: []string{"title^3"}},
		{ReturnFields: []string{"nested..title"}},
		{HighlightFields: []string{"*"}},
		{Sort: []search.SortClause{{Field: "title", Order: search.SortOrder(42)}}},
		{Filter: map[string]any{"unsupported": map[string]any{"title": "text"}}},
		{Native: map[string]any{"from": 10}},
		{Native: map[string]any{"q": "hyundai", "offset": 10, "limit": 5}},
		{Native: map[string]any{"sort": []string{"title"}}},
		{Native: map[string]any{"query": "invalid"}},
		{Native: map[string]any{"query": map[string]any{"match_all": map[string]any{}}}, SearchFields: []string{"title"}},
		{ContinuationToken: "bad-token"},
	} {
		req.Index = "books"
		_, err := component.Search(t.Context(), req)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "%+v: %v", req, err)
	}
	_, err := component.Search(t.Context(), &search.SearchRequest{Index: "books", Native: map[string]any{"aggs": map[string]any{}}})
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestNilAndUninitialized(t *testing.T) {
	t.Parallel()
	component := NewOpenSearch(logger.NewLogger("test")).(*OpenSearch)
	ctx := t.Context()
	assert.Equal(t, codes.InvalidArgument, status.Code(component.CreateIndex(ctx, nil)))
	assert.Equal(t, codes.InvalidArgument, status.Code(component.DeleteIndex(ctx, nil)))
	_, err := component.GetIndex(ctx, nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.ListIndexes(ctx, nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.IndexDocuments(ctx, nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.GetDocuments(ctx, nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.DeleteDocuments(ctx, nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.Search(ctx, nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Equal(t, codes.FailedPrecondition, status.Code(component.CreateIndex(ctx, &search.CreateIndexRequest{Index: "books"})))
	require.NoError(t, component.Close())
	require.Error(t, component.Init(ctx, search.Metadata{}))
}

func TestProviderErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		body string
		code codes.Code
	}{
		{"timeout", `{"timed_out":true}`, codes.DeadlineExceeded},
		{"failed shard", `{"_shards":{"failed":1}}`, codes.Unavailable},
		{"bad total relation", `{"hits":{"total":{"value":1,"relation":"bad"}}}`, codes.Internal},
		{"bad source", `{"hits":{"hits":[{"_id":"a","_source":{}}]}}`, codes.Internal},
		{"missing raw", `{"hits":{"hits":[{"_id":"a","_source":{"content":{}}}]}}`, codes.Internal},
		{"malformed", `{`, codes.Internal},
		{"missing hits", `{}`, codes.Internal},
	} {
		t.Run(test.name, func(t *testing.T) {
			component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, test.body) })
			_, err := component.Search(t.Context(), &search.SearchRequest{Index: "books", IncludeContent: true})
			assert.Equal(t, test.code, status.Code(err))
		})
	}
	t.Run("not found", func(t *testing.T) {
		component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			writeJSON(t, w, map[string]any{"error": map[string]any{"type": "index_not_found_exception", "reason": "missing"}})
		})
		_, err := component.GetIndex(t.Context(), &search.GetIndexRequest{Index: "missing"})
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.Equal(t, codes.NotFound, status.Code(component.DeleteIndex(t.Context(), &search.DeleteIndexRequest{Index: "missing"})))
	})
	t.Run("already exists", func(t *testing.T) {
		component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(t, w, map[string]any{"error": map[string]any{"type": "resource_already_exists_exception", "reason": "exists"}})
		})
		assert.Equal(t, codes.AlreadyExists, status.Code(component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "books"})))
	})
}

func TestCancellationAndWriteModes(t *testing.T) {
	t.Parallel()
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"items": []any{map[string]any{"index": map[string]any{"_id": "one", "status": 201}}}})
	})
	for _, options := range []search.IndexingOptions{
		{},
		{Mode: search.IndexingModeReturnOnAcceptance},
		{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
	} {
		result, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{{ID: "one", Content: []byte(`{}`)}}, Options: options})
		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, result.Ack)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := component.Search(ctx, &search.SearchRequest{Index: "books"})
	assert.Equal(t, codes.Canceled, status.Code(err))
	_, err = component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{{ID: "one", Content: []byte(`{}`)}}})
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestProjectionAndContinuation(t *testing.T) {
	t.Parallel()
	doc, err := decodeDocument("one", source(t, "one", `{"authors":[{"name":"Ada","age":42},{"name":"Bob","age":30}],"nested":{"keep":1,"remove":2}}`), false, []string{"authors.name", "nested.keep"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"authors":[{"name":"Ada"},{"name":"Bob"}],"nested":{"keep":1}}`, string(doc.Content))
	token := base64.RawURLEncoding.EncodeToString([]byte(`{"q":"fingerprint","a":[9007199254740993,"one"]}`))
	after, err := decodeContinuation(token, "fingerprint", 2)
	require.NoError(t, err)
	assert.Equal(t, "9007199254740993", string(after[0]))
	for _, bad := range []string{
		`{"q":"wrong","a":[1,"one"]}`,
		`{"q":"fingerprint","a":[{},"one"]}`,
		`{"q":"fingerprint","a":[1]}`,
		`{"q":"fingerprint","a":[1,"one"]} {}`,
	} {
		_, err = decodeContinuation(base64.RawURLEncoding.EncodeToString([]byte(bad)), "fingerprint", 2)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestUnacknowledgedLifecycle(t *testing.T) {
	t.Parallel()
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"acknowledged": false})
	})
	assert.Equal(t, codes.DeadlineExceeded, status.Code(component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "books"})))
	assert.Equal(t, codes.DeadlineExceeded, status.Code(component.DeleteIndex(t.Context(), &search.DeleteIndexRequest{Index: "books"})))
}

func TestSortUnsupportedMappings(t *testing.T) {
	t.Parallel()
	for _, mapping := range []map[string]any{
		{"type": "text"},
		{"type": "nested", "properties": map[string]any{"field": map[string]any{"type": "keyword"}}},
	} {
		component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
			assert.True(t, strings.HasSuffix(r.URL.Path, "/_mapping"))
			writeJSON(t, w, map[string]any{"books": map[string]any{"mappings": map[string]any{"_meta": map[string]any{"dapr_kind": "search"}, "properties": map[string]any{
				"content": map[string]any{"properties": map[string]any{"title": mapping}},
			}}}})
		}, true)
		_, err := component.Search(t.Context(), &search.SearchRequest{Index: "books", Sort: []search.SortClause{{Field: "title"}}})
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	}
}

func TestEmptyWritesAndClosedCalls(t *testing.T) {
	t.Parallel()
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected provider request %s", r.URL)
	})
	ctx := t.Context()
	indexed, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: "books"})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, indexed.Ack)
	deleted, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: "books"})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, deleted.Ack)
	require.NoError(t, component.Close())
	assert.Equal(t, codes.FailedPrecondition, status.Code(component.CreateIndex(ctx, &search.CreateIndexRequest{Index: "books"})))
	assert.Equal(t, codes.FailedPrecondition, status.Code(component.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: "books"})))
	_, err = component.ListIndexes(ctx, &search.ListIndexesRequest{})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: "books", IDs: []string{"one"}})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: "books"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: "books"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = component.Search(ctx, &search.SearchRequest{Index: "books"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestMissingSortValuesAreErrors(t *testing.T) {
	t.Parallel()
	component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"hits": map[string]any{"hits": []any{
			map[string]any{"_id": "a", "_source": source(t, "a", `{}`)},
			map[string]any{"_id": "b", "_source": source(t, "b", `{}`)},
		}}})
	})
	_, err := component.Search(t.Context(), &search.SearchRequest{Index: "books", TopK: 1})
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestCrossTypeIndexIsRejected(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"vector", ""} {
		t.Run(kind, func(t *testing.T) {
			component := newTestComponent(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "/books/_mapping", r.URL.Path)
				writeJSON(t, w, map[string]any{"books": map[string]any{"mappings": map[string]any{"_meta": map[string]any{"dapr_kind": kind}}}})
			}, true)
			ctx := t.Context()
			_, err := component.GetIndex(ctx, &search.GetIndexRequest{Index: "books"})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Equal(t, codes.FailedPrecondition, status.Code(component.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: "books"})))
			_, err = component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{{ID: "one", Content: []byte(`{}`)}}})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: "books", IDs: []string{"one"}})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"one"}})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = component.Search(ctx, &search.SearchRequest{Index: "books"})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}

func TestComponentMetadata(t *testing.T) {
	t.Parallel()
	component := NewOpenSearch(logger.NewLogger("test")).(*OpenSearch)
	info := component.GetComponentMetadata()
	require.Contains(t, info, "endpoint")
	require.Contains(t, info, "timeout")
	require.Contains(t, info, "region")
	for name, field := range info {
		switch strings.ToLower(name) {
		case "logger", "properties":
			t.Errorf("internal metadata field %q exposed", name)
		case "endpoint", "timeout":
			assert.False(t, field.Ignored, name)
		default:
			assert.True(t, field.Ignored, name)
		}
	}
}
