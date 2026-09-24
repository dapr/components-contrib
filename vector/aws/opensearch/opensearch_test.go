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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	"github.com/dapr/kit/logger"
)

func testComponent(t *testing.T, handler http.HandlerFunc) *OpenSearch {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	component := NewOpenSearch(logger.NewLogger("opensearch-vector-test")).(*OpenSearch)
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, component.Init(ctx, vector.Metadata{Base: metadata.Base{Properties: map[string]string{
		"endpoint": server.URL, "region": "us-east-1", "accessKey": "test", "secretKey": "test",
	}}}))
	cancel() // Init's context must not become the client's lifetime context.
	t.Cleanup(func() { require.NoError(t, component.Close()) })
	return component
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	assert.NoError(t, json.NewEncoder(w).Encode(value))
}

func mapping(dimensions uint32, metric vector.DistanceMetric) map[string]any {
	return map[string]any{"mappings": map[string]any{
		"_meta":      collectionInfo{Kind: "vector", Dimensions: dimensions, Metric: metric},
		"properties": map[string]any{"values": map[string]any{"type": "knn_vector", "dimension": dimensions}},
	}}
}

func withMapping(t *testing.T, metric vector.DistanceMetric, handler http.HandlerFunc) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/vectors/_mapping" {
			writeJSON(t, w, map[string]any{"vectors": mapping(2, metric)})
			return
		}
		handler(w, r)
	}
}

func TestCollectionLifecycle(t *testing.T) {
	var stored map[string]any
	component := testComponent(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "/es/aws4_request")
		switch r.Method + " " + r.URL.Path {
		case "PUT /vectors":
			if stored != nil {
				w.WriteHeader(http.StatusBadRequest)
				writeJSON(t, w, map[string]any{"error": map[string]any{"type": "resource_already_exists_exception"}})
				return
			}
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&stored))
			writeJSON(t, w, map[string]any{"acknowledged": true})
		case "GET /vectors/_mapping":
			if stored == nil {
				w.WriteHeader(http.StatusNotFound)
				writeJSON(t, w, map[string]any{"error": map[string]any{"type": "index_not_found_exception"}})
				return
			}
			writeJSON(t, w, map[string]any{"vectors": stored})
		case "GET /vectors/_count":
			writeJSON(t, w, map[string]any{"count": 42})
		case "GET /_mapping":
			writeJSON(t, w, map[string]any{"vectors": stored, "documents": map[string]any{
				"mappings": map[string]any{"_meta": map[string]any{"dapr_kind": "search"}},
			}})
		case "DELETE /vectors":
			stored = nil
			writeJSON(t, w, map[string]any{"acknowledged": true})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	ctx := t.Context()
	require.NoError(t, component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: "vectors", Dimensions: 2}))
	mappings := stored["mappings"].(map[string]any)
	assert.Equal(t, float64(vector.DistanceMetricCosine), mappings["_meta"].(map[string]any)["metric"])
	assert.Equal(t, false, mappings["date_detection"], "date-shaped metadata strings must retain their keyword subfield")
	properties := mappings["properties"].(map[string]any)
	assert.Equal(t, "knn_vector", properties["values"].(map[string]any)["type"])
	assert.Equal(t, float64(2), properties["values"].(map[string]any)["dimension"])
	assert.Equal(t, "binary", properties["payload"].(map[string]any)["type"])
	assert.Equal(t, true, properties["metadata"].(map[string]any)["dynamic"])
	assert.Contains(t, fmt.Sprint(mappings["dynamic_templates"]), "keyword")
	assert.NotContains(t, fmt.Sprint(mappings["dynamic_templates"]), "ignore_above")
	err := component.CreateCollection(ctx, &vector.CreateCollectionRequest{Collection: "vectors", Dimensions: 2})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	info, err := component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: "vectors"})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), info.Dimensions)
	assert.Equal(t, vector.DistanceMetricCosine, info.Metric)
	assert.Equal(t, uint64(42), info.RecordCount)
	list, err := component.ListCollections(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"vectors"}, list.Collections)
	require.NoError(t, component.DeleteCollection(ctx, &vector.DeleteCollectionRequest{Collection: "vectors"}))
	_, err = component.GetCollection(ctx, &vector.GetCollectionRequest{Collection: "vectors"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestMappingValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"missing index", map[string]any{}},
		{"missing metadata", map[string]any{"vectors": map[string]any{"mappings": map[string]any{}}}},
		{"zero dimensions", map[string]any{"vectors": mapping(0, vector.DistanceMetricCosine)}},
		{"unspecified metric", map[string]any{"vectors": mapping(2, vector.DistanceMetricUnspecified)}},
		{"unknown metric", map[string]any{"vectors": mapping(2, vector.DistanceMetric(42))}},
		{"wrong field dimensions", map[string]any{"vectors": map[string]any{"mappings": map[string]any{
			"_meta":      collectionInfo{Kind: "vector", Dimensions: 2, Metric: vector.DistanceMetricCosine},
			"properties": map[string]any{"values": map[string]any{"type": "knn_vector", "dimension": 3}},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := testComponent(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(t, w, tc.body) })
			_, err := component.GetCollection(t.Context(), &vector.GetCollectionRequest{Collection: "vectors"})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			err = component.DeleteCollection(t.Context(), &vector.DeleteCollectionRequest{Collection: "vectors"})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err), "must not delete non-vector indexes")
		})
	}
}

func TestCollectionAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		body string
		code codes.Code
	}{
		{`{"acknowledged":false}`, codes.DeadlineExceeded},
		{`{}`, codes.Internal},
	} {
		t.Run(tc.body, func(t *testing.T) {
			component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			err := component.CreateCollection(t.Context(), &vector.CreateCollectionRequest{Collection: "vectors", Dimensions: 2})
			assert.Equal(t, tc.code, status.Code(err))
			err = component.DeleteCollection(t.Context(), &vector.DeleteCollectionRequest{Collection: "vectors"})
			assert.Equal(t, tc.code, status.Code(err))
		})
	}
}

func TestRecordRoundTripAndPartialFailures(t *testing.T) {
	sources := map[string]json.RawMessage{}
	component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/vectors/_bulk":
			assert.Equal(t, "wait_for", r.URL.Query().Get("refresh"))
			decoder := json.NewDecoder(r.Body)
			var items []any
			for {
				var action map[string]struct {
					ID string `json:"_id"`
				}
				if err := decoder.Decode(&action); err == io.EOF {
					break
				} else if !assert.NoError(t, err) {
					return
				}
				if index, ok := action["index"]; ok {
					var source json.RawMessage
					assert.NoError(t, decoder.Decode(&source))
					if index.ID == "bad" {
						items = append(items, map[string]any{"index": map[string]any{
							"_id": index.ID, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception"},
						}})
					} else {
						sources[index.ID] = source
						items = append(items, map[string]any{"index": map[string]any{"_id": index.ID, "status": 201}})
					}
				} else {
					id := action["delete"].ID
					delete(sources, id)
					items = append(items, map[string]any{"delete": map[string]any{"_id": id, "status": 404}})
				}
			}
			writeJSON(t, w, map[string]any{"items": items})
		case "/vectors/_mget":
			var request struct {
				IDs []string `json:"ids"`
			}
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			docs := make([]any, 0, len(request.IDs))
			for _, id := range request.IDs {
				source, found := sources[id]
				docs = append(docs, map[string]any{"_id": id, "found": found, "_source": source})
			}
			writeJSON(t, w, map[string]any{"docs": docs})
		default:
			t.Errorf("unexpected request %s", r.URL)
		}
	}))
	records := []vector.Record{
		{ID: "first", Values: []float32{1, 0}, Payload: []byte{0, 255, 128}, Metadata: map[string]any{"tag": "blue", "count": float64(2)}},
		{ID: "second", Values: []float32{0, 1}},
		{ID: "bad", Values: []float32{1, 1}},
	}
	upsert, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "vectors", Records: records})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, upsert.Ack)
	require.Len(t, upsert.FailedItems, 1)
	assert.Equal(t, "bad", upsert.FailedItems[0].ID)
	assert.Equal(t, codes.InvalidArgument, upsert.FailedItems[0].Error.Code())
	assert.Contains(t, string(sources["first"]), `"payload":"AP+A"`)
	get, err := component.Get(t.Context(), &vector.GetRequest{Collection: "vectors", IDs: []string{"second", "missing", "first"}, IncludeValues: true})
	require.NoError(t, err)
	assert.Equal(t, []vector.Record{records[1], records[0]}, get.Records)
	get, err = component.Get(t.Context(), &vector.GetRequest{Collection: "vectors", IDs: []string{"first"}})
	require.NoError(t, err)
	require.Len(t, get.Records, 1)
	assert.Nil(t, get.Records[0].Values)
	assert.Equal(t, records[0].Payload, get.Records[0].Payload)
	records[0].Values = []float32{0.5, 0.5}
	_, err = component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "vectors", Records: records[:1]})
	require.NoError(t, err)
	get, err = component.Get(t.Context(), &vector.GetRequest{Collection: "vectors", IDs: []string{"first"}, IncludeValues: true})
	require.NoError(t, err)
	assert.Equal(t, records[:1], get.Records)
	deleted, err := component.Delete(t.Context(), &vector.DeleteRequest{Collection: "vectors", IDs: []string{"first", "missing", "first"}})
	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, deleted.Ack)
	get, err = component.Get(t.Context(), &vector.GetRequest{Collection: "vectors", IDs: []string{"first"}})
	require.NoError(t, err)
	assert.Empty(t, get.Records)
}

func TestWriteValidationAndErrors(t *testing.T) {
	var writes atomic.Int32
	component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
		writes.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(t, w, map[string]any{"error": map[string]any{"type": "unavailable_shards_exception"}})
	}))
	for _, records := range [][]vector.Record{
		{{Values: []float32{1, 0}}},
		{{ID: "a", Values: []float32{1, 0}}, {ID: "a", Values: []float32{0, 1}}},
		{{ID: "a", Values: []float32{1}}},
		{{ID: "a", Values: []float32{0, 0}}},
		{{ID: "a", Values: []float32{float32(math.NaN()), 1}}},
		{{ID: "a", Values: []float32{float32(math.Inf(1)), 1}}},
		{{ID: "a", Values: []float32{1, 0}, Metadata: map[string]any{"invalid": make(chan int)}}},
	} {
		result, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "vectors", Records: records})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Nil(t, result)
	}
	opts := search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync}
	_, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "vectors", Options: opts})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.Delete(t.Context(), &vector.DeleteRequest{Collection: "vectors", Options: opts})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Zero(t, writes.Load())
	result, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "vectors", Records: []vector.Record{{ID: "a", Values: []float32{1, 0}}}})
	assert.Nil(t, result)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestQueriesScoresAndThresholds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		metric    vector.DistanceMetric
		scores    []float64
		expected  []float64
		threshold float64
	}{
		{"cosine negative", vector.DistanceMetricCosine, []float64{2, 1, 0.5, 0}, []float64{1, 0, -0.5, -1}, -0.5},
		{"dot negative", vector.DistanceMetricDotProduct, []float64{4, 1, 0.5, 0.25}, []float64{3, 0, -1, -3}, -1},
		{"euclidean not squared", vector.DistanceMetricEuclidean, []float64{1, 0.5, 0.2, 0.1}, []float64{0, 1, 2, 3}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/vectors/_search", r.URL.Path)
				assert.Empty(t, r.URL.RawQuery, "AWS OpenSearch ignores query parameters on signed POST requests")
				var request map[string]any
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				scoring := request["query"].(map[string]any)["script_score"].(map[string]any)
				script := scoring["script"].(map[string]any)
				if tc.metric == vector.DistanceMetricCosine {
					assert.Equal(t, "painless", script["lang"])
					assert.Equal(t, "1.0 + cosineSimilarity(params.query_value, doc[params.field])", script["source"])
				} else {
					assert.Equal(t, "knn_score", script["source"])
					space, spaceErr := metricSpace(tc.metric)
					assert.NoError(t, spaceErr)
					assert.Equal(t, space, script["params"].(map[string]any)["space_type"])
				}
				assert.Equal(t, float64(4), request["size"])
				assert.Contains(t, fmt.Sprint(scoring["query"]), "metadata.tag.keyword")
				hits := make([]any, len(tc.scores))
				for i, score := range tc.scores {
					hits[i] = map[string]any{"_id": strconv.Itoa(i), "_score": score, "_source": storedRecord{
						Values: []float32{1, 0}, Payload: []byte{0, 255}, Metadata: map[string]any{"tag": "blue"},
					}}
				}
				writeJSON(t, w, map[string]any{"hits": map[string]any{"hits": hits}})
			}))
			request := &vector.QueryRequest{
				Collection: "vectors", Vector: &vector.Record{Values: []float32{1, 0}}, TopK: 4, Metric: tc.metric,
				Filter: map[string]any{"tag": "blue"}, IncludeValues: true, IncludePayload: true,
			}
			response, err := component.Query(t.Context(), request)
			require.NoError(t, err)
			assert.Equal(t, tc.metric, response.Metric)
			require.Len(t, response.Matches, 4)
			for i, match := range response.Matches {
				assert.InDelta(t, tc.expected[i], match.Score, 1e-6)
				assert.Equal(t, []float32{1, 0}, match.Record.Values)
				assert.Equal(t, []byte{0, 255}, match.Record.Payload)
			}
			request.ScoreThreshold = &tc.threshold
			request.IncludeValues, request.IncludePayload = false, false
			response, err = component.Query(t.Context(), request)
			require.NoError(t, err)
			require.Len(t, response.Matches, 3, "inclusive boundary is retained")
			assert.InDelta(t, tc.threshold, response.Matches[2].Score, 1e-6)
			assert.Empty(t, response.Matches[0].Record.Values)
			assert.Empty(t, response.Matches[0].Record.Payload)
			assert.Equal(t, "blue", response.Matches[0].Record.Metadata["tag"])
		})
	}
}

func TestQueryByIDAndBatch(t *testing.T) {
	component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, strings.HasPrefix(r.URL.Path, "/vectors/"), "batch collection overrides individual collections")
		if r.URL.Path == "/vectors/_mget" {
			var request struct {
				IDs []string `json:"ids"`
			}
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			if request.IDs[0] == "missing" {
				writeJSON(t, w, map[string]any{"docs": []any{map[string]any{"_id": "missing", "found": false}}})
				return
			}
			writeJSON(t, w, map[string]any{"docs": []any{map[string]any{
				"_id": "self", "found": true, "_source": storedRecord{Values: []float32{1, 0}},
			}}})
			return
		}
		var request map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		scoring := request["query"].(map[string]any)["script_score"].(map[string]any)
		boolQuery := scoring["query"].(map[string]any)["bool"].(map[string]any)
		assert.Contains(t, fmt.Sprint(boolQuery["must_not"]), "self")
		assert.Equal(t, float64(10), request["size"])
		writeJSON(t, w, map[string]any{"hits": map[string]any{"hits": []any{map[string]any{
			"_id": "other", "_score": 1, "_source": storedRecord{Values: []float32{0, 1}},
		}}}})
	}))
	response, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "vectors", Queries: []vector.QueryRequest{
		{Collection: "ignored", ByID: "self"},
		{ByID: "missing"},
		{Vector: &vector.Record{Values: []float32{1}}},
		{ByID: "self", Vector: &vector.Record{Values: []float32{1, 0}}},
	}})
	require.NoError(t, err)
	require.Len(t, response.Results, 4)
	require.NotNil(t, response.Results[0].Response)
	require.NoError(t, response.Results[0].Error)
	assert.Equal(t, "other", response.Results[0].Response.Matches[0].Record.ID)
	for i, code := range []codes.Code{codes.NotFound, codes.InvalidArgument, codes.InvalidArgument} {
		assert.Equal(t, code, status.Code(response.Results[i+1].Error))
		assert.Nil(t, response.Results[i+1].Response)
	}
	empty, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "vectors"})
	require.NoError(t, err)
	assert.Empty(t, empty.Results)
}

func TestQueryValidationAndBackendErrors(t *testing.T) {
	var searches atomic.Int32
	component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
		searches.Add(1)
		w.WriteHeader(http.StatusForbidden)
		writeJSON(t, w, map[string]any{"error": map[string]any{"type": "security_exception"}})
	}))
	nan := math.NaN()
	for _, request := range []vector.QueryRequest{
		{},
		{Vector: &vector.Record{}},
		{Vector: &vector.Record{Values: []float32{0, 0}}},
		{Vector: &vector.Record{Values: []float32{1, 0}}, Metric: vector.DistanceMetric(99)},
		{Vector: &vector.Record{Values: []float32{1, 0}}, ScoreThreshold: &nan},
		{Vector: &vector.Record{Values: []float32{1, 0}}, Filter: map[string]any{"$INVALID": "value"}},
	} {
		request.Collection = "vectors"
		_, err := component.Query(t.Context(), &request)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	assert.Zero(t, searches.Load())
	response, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "vectors", Queries: []vector.QueryRequest{
		{Vector: &vector.Record{Values: []float32{1, 0}}},
	}})
	require.NoError(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(response.Results[0].Error))
	assert.Nil(t, response.Results[0].Response)
}

func TestSearchIncompleteResponses(t *testing.T) {
	for _, tc := range []struct {
		body string
		code codes.Code
	}{
		{`{"timed_out":true,"hits":{"hits":[]}}`, codes.DeadlineExceeded},
		{`{"_shards":{"failed":1},"hits":{"hits":[]}}`, codes.Internal},
		{`{}`, codes.Internal},
		{`{"hits":{"hits":[{"_id":"x","_source":{}}]}}`, codes.Internal},
		{`{"hits":{"hits":[{"_id":"x","_score":1,"_source":null}]}}`, codes.Internal},
		{`{"hits":{"hits":[{"_id":"x","_score":-1,"_source":{}}]}}`, codes.Internal},
		{`{"hits":{"hits":[{"_id":"x","_score":1,"_source":{"payload":"not base64!"}}]}}`, codes.Internal},
	} {
		t.Run(tc.body, func(t *testing.T) {
			component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			_, err := component.Query(t.Context(), &vector.QueryRequest{Collection: "vectors", Vector: &vector.Record{Values: []float32{1, 0}}})
			assert.Equal(t, tc.code, status.Code(err))
		})
	}
}

func TestWriteTimeoutAndDeleteFailure(t *testing.T) {
	component := testComponent(t, withMapping(t, vector.DistanceMetricCosine, func(w http.ResponseWriter, r *http.Request) {
		var action map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&action))
		if _, ok := action["delete"]; ok {
			writeJSON(t, w, map[string]any{"items": []any{map[string]any{"delete": map[string]any{
				"_id": "a", "status": 403, "error": map[string]any{"type": "security_exception"},
			}}}})
			return
		}
		<-r.Context().Done()
	}))
	_, err := component.Upsert(t.Context(), &vector.UpsertRequest{
		Collection: "vectors", Records: []vector.Record{{ID: "a", Values: []float32{1, 0}}},
		Options: search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 30 * time.Millisecond, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
	})
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	result, err := component.Delete(t.Context(), &vector.DeleteRequest{Collection: "vectors", IDs: []string{"a"}})
	assert.Nil(t, result)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestMissingCollectionAndCancellation(t *testing.T) {
	component := testComponent(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/vectors/_mapping", r.URL.Path, "missing collections must never be auto-created by bulk writes")
		w.WriteHeader(http.StatusNotFound)
		writeJSON(t, w, map[string]any{"error": map[string]any{"type": "index_not_found_exception"}})
	})
	_, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "vectors", Queries: []vector.QueryRequest{{ByID: "id"}}})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "vectors", Records: []vector.Record{{ID: "a", Values: []float32{1, 0}}}})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = component.Delete(t.Context(), &vector.DeleteRequest{Collection: "vectors", IDs: []string{"a"}})
	assert.Equal(t, codes.NotFound, status.Code(err))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = component.Query(ctx, &vector.QueryRequest{Collection: "vectors", Vector: &vector.Record{Values: []float32{1, 0}}})
	assert.Equal(t, codes.Canceled, status.Code(err))
}

func TestReadyAndConcurrentClose(t *testing.T) {
	uninitialized := NewOpenSearch(logger.NewLogger("opensearch-vector-test")).(*OpenSearch)
	_, err := uninitialized.ListCollections(t.Context(), nil)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.NoError(t, uninitialized.Close())
	require.NoError(t, uninitialized.Close())
	assert.Equal(t, codes.FailedPrecondition, status.Code(uninitialized.Init(t.Context(), vector.Metadata{})))
	component := testComponent(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(t, w, map[string]any{}) })
	require.NoError(t, component.Init(t.Context(), vector.Metadata{}), "initialization is idempotent")
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, readErr := component.ListCollections(t.Context(), nil)
			if readErr != nil {
				assert.Equal(t, codes.FailedPrecondition, status.Code(readErr))
			}
		})
		wg.Go(func() { assert.NoError(t, component.Close()) })
	}
	wg.Wait()
	_, err = component.ListCollections(t.Context(), nil)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestInvalidRequests(t *testing.T) {
	component := testComponent(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("invalid requests must not reach the backend: %s", r.URL)
	})
	for _, req := range []*vector.CreateCollectionRequest{
		nil, {Collection: "vectors"}, {Collection: "*", Dimensions: 2},
		{Collection: "one,two", Dimensions: 2}, {Collection: "one/two", Dimensions: 2},
		{Collection: "vectors", Dimensions: 2, Metric: vector.DistanceMetric(99)},
	} {
		assert.Equal(t, codes.InvalidArgument, status.Code(component.CreateCollection(t.Context(), req)))
	}
	_, err := component.GetCollection(t.Context(), nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Equal(t, codes.InvalidArgument, status.Code(component.DeleteCollection(t.Context(), nil)))
	_, err = component.Upsert(t.Context(), nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.Get(t.Context(), nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.Delete(t.Context(), nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.Query(t.Context(), nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = component.BatchQuery(t.Context(), nil)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestInvalidScores(t *testing.T) {
	for _, metric := range []vector.DistanceMetric{vector.DistanceMetricCosine, vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
		for _, score := range []float64{math.NaN(), math.Inf(1), -1} {
			_, err := metricScore(score, metric)
			assert.Equal(t, codes.Internal, status.Code(err))
		}
	}
	for _, metric := range []vector.DistanceMetric{vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
		_, err := metricScore(0, metric)
		assert.Equal(t, codes.Internal, status.Code(err))
	}
	_, err := metricScore(2, vector.DistanceMetricEuclidean)
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestComponentMetadata(t *testing.T) {
	component := NewOpenSearch(logger.NewLogger("opensearch-vector-test")).(*OpenSearch)
	info := component.GetComponentMetadata()
	assert.NotContains(t, info, "Logger")
	assert.NotContains(t, info, "Properties")
	for _, name := range []string{"endpoint", "timeout"} {
		require.Contains(t, info, name)
		assert.False(t, info[name].Ignored)
	}
	for _, name := range []string{"region", "accessKey", "secretKey", "sessionToken", "assumeRoleArn", "assumeRoleSessionName", "trustAnchorArn", "trustProfileArn"} {
		require.Contains(t, info, name)
		assert.True(t, info[name].Ignored, "AWS authentication metadata is documented by the built-in profile")
	}
}
