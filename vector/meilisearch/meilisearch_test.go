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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	contribmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	kitlogger "github.com/dapr/kit/logger"
)

func TestVectorCreateCollection(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes":
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexCreation"})
		case "/tasks/1":
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": 1, "status": "succeeded", "type": "indexCreation"})
		case "/tasks/2":
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": 2, "status": "succeeded", "type": "settingsUpdate"})
		case "/indexes/cars/settings":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			embedders := body["embedders"].(map[string]any)
			def := embedders["default"].(map[string]any)
			require.Equal(t, "userProvided", def["source"])
			require.Equal(t, float64(3), def["dimensions"])
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 2, "status": "enqueued", "type": "settingsUpdate"})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	_, err := component.CreateCollection(t.Context(), &vector.CreateCollectionRequest{Collection: "cars", Dimension: 3, Metric: vector.DistanceMetricCosine})
	require.NoError(t, err)
}

func TestVectorUpsertQueryGetDelete(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes/cars/documents":
			var docs []map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&docs))
			require.Equal(t, "1", docs[0]["id"])
			require.Contains(t, docs[0], "_vectors")
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 10, "status": "enqueued", "type": "documentAdditionOrUpdate"})
		case "/tasks/10", "/tasks/11":
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": 10, "status": "succeeded", "type": "documentAdditionOrUpdate"})
		case "/indexes/cars/search":
			var req map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.Equal(t, []any{1.0, 2.0, 3.0}, req["vector"])
			writeJSON(t, w, http.StatusOK, map[string]any{"hits": []map[string]any{{"id": "1", "make": "hyundai", "_rankingScore": 0.9, "_vectors": map[string]any{"default": []float32{1, 2, 3}}}}, "estimatedTotalHits": 1, "limit": 1})
		case "/indexes/cars/documents/1":
			require.Equal(t, "true", r.URL.Query().Get("retrieveVectors"))
			writeJSON(t, w, http.StatusOK, map[string]any{"id": "1", "make": "hyundai", "_vectors": map[string]any{"default": []float32{1, 2, 3}}})
		case "/indexes/cars/documents/delete-batch":
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 11, "status": "enqueued", "type": "documentDeletion"})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	upsert, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "cars", Ack: search.IndexAckDurable, Vectors: []vector.Vec{{ID: "1", Values: []float32{1, 2, 3}, Metadata: map[string]any{"make": "hyundai"}}}})
	require.NoError(t, err)
	require.True(t, upsert.Results[0].Success)
	query, err := component.Query(t.Context(), &vector.QueryRequest{Collection: "cars", QueryVector: []float32{1, 2, 3}, TopK: 1, IncludeValues: true, IncludeMetadata: true})
	require.NoError(t, err)
	require.Len(t, query.Hits, 1)
	require.Equal(t, "1", query.Hits[0].Vector.ID)
	require.InDelta(t, 0.1, query.Hits[0].Distance, 0.0001)
	require.Equal(t, []float32{1, 2, 3}, query.Hits[0].Vector.Values)
	require.Equal(t, "hyundai", query.Hits[0].Vector.Metadata["make"])
	got, err := component.Get(t.Context(), &vector.GetRequest{Collection: "cars", IDs: []string{"1"}, IncludeValues: true, IncludeMetadata: true})
	require.NoError(t, err)
	require.Len(t, got.Vectors, 1)
	deleted, err := component.Delete(t.Context(), &vector.DeleteRequest{Collection: "cars", Selector: vector.DeleteSelector{IDs: []string{"1"}}, Ack: search.IndexAckDurable})
	require.NoError(t, err)
	require.Equal(t, uint64(1), deleted.DeletedCount)
}

func TestVectorLifecycle(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes":
			writeJSON(t, w, http.StatusOK, map[string]any{"results": []map[string]any{{"uid": "cars", "primaryKey": "id"}}, "limit": 20, "total": 1})
		case "/indexes/cars":
			if r.Method == http.MethodDelete {
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexDeletion"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": "cars", "primaryKey": "id"})
		case "/indexes/cars/stats":
			writeJSON(t, w, http.StatusOK, map[string]any{"numberOfDocuments": 7})
		case "/indexes/cars/settings":
			writeJSON(t, w, http.StatusOK, map[string]any{"embedders": map[string]any{"default": map[string]any{"source": "userProvided", "dimensions": 3}}, "filterableAttributes": []string{"make"}})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	list, err := component.ListCollections(t.Context(), &vector.ListCollectionsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"cars"}, list.Collections)
	desc, err := component.DescribeCollection(t.Context(), &vector.DescribeCollectionRequest{Collection: "cars"})
	require.NoError(t, err)
	require.Equal(t, uint32(3), desc.Dimension)
	require.Equal(t, uint64(7), desc.VectorCount)
	require.NoError(t, component.DropCollection(t.Context(), &vector.DropCollectionRequest{Collection: "cars"}))
}

func TestVectorBatchQuery(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/indexes/cars/search" {
			t.Fatalf("unexpected %s", r.URL.Path)
		}
		call := calls.Add(1)
		if call == 1 {
			writeJSON(t, w, http.StatusInternalServerError, map[string]any{"message": "boom", "code": "internal"})
			return
		}
		writeJSON(t, w, http.StatusOK, map[string]any{"hits": []map[string]any{{"id": "ok", "_rankingScore": 1}}, "estimatedTotalHits": 1})
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	res, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "cars", Queries: []vector.QueryRequest{{QueryVector: []float32{1}}, {QueryVector: []float32{2}}}})
	require.NoError(t, err)
	require.Len(t, res.Results, 2)
	errs := 0
	successes := 0
	for _, result := range res.Results {
		if result.ErrorMessage != "" {
			errs++
			require.True(t, strings.Contains(result.ErrorMessage, "boom") || strings.Contains(result.ErrorMessage, "500"))
		} else {
			successes++
			require.Equal(t, "ok", result.Hits[0].Vector.ID)
		}
	}
	require.Equal(t, 1, errs)
	require.Equal(t, 1, successes)
}

func initializedVector(t *testing.T, host string) vector.Vector {
	t.Helper()
	component := NewMeilisearch(kitlogger.NewLogger("test"))
	err := component.Init(t.Context(), vector.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": host}}})
	require.NoError(t, err)
	return component
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(body))
}
