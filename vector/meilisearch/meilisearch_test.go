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
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	"github.com/dapr/components-contrib/vector"
	kitlogger "github.com/dapr/kit/logger"
)

var waitForCompletion = search.IndexingOptions{
	Mode:          search.IndexingModeWaitForCompletion,
	WaitTimeout:   5 * time.Second,
	OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
}

func TestCreateCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		req            *vector.CreateCollectionRequest
		wantFilterable []any
		wantSortable   any
	}{
		{
			name:           "dimensions and the default metric",
			req:            &vector.CreateCollectionRequest{Collection: "books", Dimensions: 3},
			wantFilterable: []any{"daprMetadata"},
		},
		{
			name: "explicit cosine with meilisearch settings",
			req: &vector.CreateCollectionRequest{Collection: "books", Dimensions: 3, Metric: vector.DistanceMetricCosine, Metadata: map[string]string{
				"filterableAttributes": "id, daprMetadata",
				"sortableAttributes":   "id",
			}},
			wantFilterable: []any{"daprMetadata", "id"},
			wantSortable:   []any{"id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var settings map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/indexes":
					var body map[string]any
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					assert.Equal(t, "id", body["primaryKey"])
					writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexCreation"})
				case "/tasks/1", "/tasks/2":
					writeJSON(t, w, http.StatusOK, map[string]any{"uid": 1, "status": "succeeded", "type": "indexCreation"})
				case "/indexes/books/settings":
					require.NoError(t, json.NewDecoder(r.Body).Decode(&settings))
					writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 2, "status": "enqueued", "type": "settingsUpdate"})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				}
			}))
			defer server.Close()

			component := initializedVector(t, server.URL)
			require.NoError(t, component.CreateCollection(t.Context(), tt.req))

			embedders, ok := settings["embedders"].(map[string]any)
			require.True(t, ok)
			require.Len(t, embedders, 1, "a collection has exactly one embedder")
			def, ok := embedders["default"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "userProvided", def["source"])
			assert.InDelta(t, 3, def["dimensions"], 0)
			assert.Equal(t, tt.wantFilterable, settings["filterableAttributes"], "record metadata is always filterable")
			assert.Equal(t, tt.wantSortable, settings["sortableAttributes"], "unset settings are not sent")
		})
	}
}

func TestCreateCollectionRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		req          *vector.CreateCollectionRequest
		wantContains string
	}{
		{name: "missing collection", req: &vector.CreateCollectionRequest{Dimensions: 3}, wantContains: "collection is required"},
		{name: "missing dimensions", req: &vector.CreateCollectionRequest{Collection: "books"}, wantContains: "dimensions must be greater than zero"},
		{name: "euclidean metric", req: &vector.CreateCollectionRequest{Collection: "books", Dimensions: 3, Metric: vector.DistanceMetricEuclidean}, wantContains: "cosine"},
		{name: "dot product metric", req: &vector.CreateCollectionRequest{Collection: "books", Dimensions: 3, Metric: vector.DistanceMetricDotProduct}, wantContains: "cosine"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
			}))
			defer server.Close()

			component := initializedVector(t, server.URL)
			err := component.CreateCollection(t.Context(), tt.req)

			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
		})
	}
}

func TestCreateCollectionReportsAnExistingCollection(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes":
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexCreation"})
		case "/tasks/1":
			writeJSON(t, w, http.StatusOK, map[string]any{
				"uid": 1, "status": "failed", "type": "indexCreation", "indexUid": "books",
				"error": map[string]any{"message": "Index `books` already exists.", "code": "index_already_exists", "type": "invalid_request"},
			})
		default:
			t.Errorf("the settings of an existing collection are not reconciled: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	err := component.CreateCollection(t.Context(), &vector.CreateCollectionRequest{Collection: "books", Dimensions: 3})

	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), `"books" already exists`)
}

func TestCollectionLifecycle(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes":
			writeJSON(t, w, http.StatusOK, map[string]any{"results": []map[string]any{{"uid": "books", "primaryKey": "id"}}, "limit": 20, "total": 1})
		case "/indexes/books":
			if r.Method == http.MethodDelete {
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexDeletion"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": "books", "primaryKey": "id"})
		case "/indexes/books/stats":
			writeJSON(t, w, http.StatusOK, map[string]any{"numberOfDocuments": 7})
		case "/indexes/books/settings":
			writeJSON(t, w, http.StatusOK, map[string]any{
				"embedders":            map[string]any{"default": map[string]any{"source": "userProvided", "dimensions": 3}},
				"filterableAttributes": []string{"daprMetadata"},
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)

	list, err := component.ListCollections(t.Context(), &vector.ListCollectionsRequest{})
	require.NoError(t, err)
	assert.Equal(t, []string{"books"}, list.Collections)

	got, err := component.GetCollection(t.Context(), &vector.GetCollectionRequest{Collection: "books"})
	require.NoError(t, err)
	assert.Equal(t, "books", got.Collection)
	assert.Equal(t, uint64(7), got.RecordCount)
	assert.Equal(t, uint32(3), got.Dimensions)
	assert.Equal(t, vector.DistanceMetricCosine, got.Metric, "the effective metric is always concrete")
	assert.Equal(t, map[string]string{"primaryKey": "id", "filterableAttributes": "daprMetadata"}, got.Properties)

	require.NoError(t, component.DeleteCollection(t.Context(), &vector.DeleteCollectionRequest{Collection: "books"}))
}

func TestUpsert(t *testing.T) {
	t.Parallel()

	var posted []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/documents", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
		writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 10, "status": "enqueued", "type": "documentAdditionOrUpdate"})
	}))
	defer server.Close()

	metadata := map[string]any{"author": "pride-and-prejudice", "year": float64(2024), "publisher": map[string]any{"city": "london"}}
	component := initializedVector(t, server.URL)
	res, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "books", Records: []vector.Record{{
		ID:       "1",
		Values:   []float32{1, 2, 3},
		Payload:  []byte(`{"title":"a"}`),
		Metadata: metadata,
	}}})

	require.NoError(t, err)
	assert.Equal(t, search.IndexAckQueued, res.Ack)
	assert.Empty(t, res.FailedItems)

	require.Len(t, posted, 1)
	assert.Equal(t, "1", posted[0]["id"])
	assert.Equal(t, map[string]any{"default": []any{1.0, 2.0, 3.0}}, posted[0]["_vectors"])
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(`{"title":"a"}`)), posted[0]["daprPayload"], "the payload stays opaque")
	assert.Equal(t, metadata, posted[0]["daprMetadata"], "structured metadata is stored as an object")
}

func TestUpsertRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		req          *vector.UpsertRequest
		wantContains string
	}{
		{name: "missing collection", req: &vector.UpsertRequest{Records: []vector.Record{{ID: "1", Values: []float32{1}}}}, wantContains: "collection is required"},
		{name: "empty id", req: &vector.UpsertRequest{Collection: "books", Records: []vector.Record{{Values: []float32{1}}}}, wantContains: "empty id"},
		{name: "duplicate ids", req: &vector.UpsertRequest{Collection: "books", Records: []vector.Record{{ID: "1", Values: []float32{1}}, {ID: "1", Values: []float32{2}}}}, wantContains: "duplicate id"},
		{
			name:         "wait fields without wait mode",
			req:          &vector.UpsertRequest{Collection: "books", Records: []vector.Record{{ID: "1", Values: []float32{1}}}, Options: search.IndexingOptions{WaitTimeout: time.Second}},
			wantContains: "INDEXING_MODE_WAIT_FOR_COMPLETION",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
			}))
			defer server.Close()

			component := initializedVector(t, server.URL)
			_, err := component.Upsert(t.Context(), tt.req)

			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
		})
	}
}

func TestUpsertReportsRecordsWithoutVectorsBeforeEnqueue(t *testing.T) {
	t.Parallel()

	t.Run("every record fails", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		res, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "books", Records: []vector.Record{{ID: "1"}}})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, res.Ack)
		require.Len(t, res.FailedItems, 1)
		assert.Equal(t, "1", res.FailedItems[0].ID)
		assert.Equal(t, codes.InvalidArgument, res.FailedItems[0].Error.Code())
	})

	t.Run("valid records are still enqueued", func(t *testing.T) {
		t.Parallel()

		var posted []map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 10, "status": "enqueued", "type": "documentAdditionOrUpdate"})
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		res, err := component.Upsert(t.Context(), &vector.UpsertRequest{Collection: "books", Records: []vector.Record{
			{ID: "1"},
			{ID: "2", Values: []float32{1}},
		}})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckQueued, res.Ack)
		require.Len(t, res.FailedItems, 1)
		assert.Equal(t, "1", res.FailedItems[0].ID)
		require.Len(t, posted, 1)
		assert.Equal(t, "2", posted[0]["id"])
	})
}

func TestUpsertWaitForCompletion(t *testing.T) {
	t.Parallel()

	t.Run("polls the task status when the stream is unavailable", func(t *testing.T) {
		t.Parallel()

		var streamDials, statusReads atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/tasks/stream":
				streamDials.Add(1)
				writeJSON(t, w, http.StatusBadRequest, map[string]any{
					"message": "getting task changes requires enabling the tasks streaming route",
					"code":    "feature_not_enabled",
					"type":    "invalid_request",
				})
			case "/indexes/books/documents":
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 12, "status": "enqueued", "type": "documentAdditionOrUpdate"})
			case "/tasks/12":
				taskStatus := "processing"
				if statusReads.Add(1) >= 2 {
					taskStatus = "succeeded"
				}
				writeJSON(t, w, http.StatusOK, map[string]any{"uid": 12, "status": taskStatus, "type": "documentAdditionOrUpdate"})
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			}
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		for range 2 {
			res, err := component.Upsert(t.Context(), &vector.UpsertRequest{
				Collection: "books",
				Records:    []vector.Record{{ID: "1", Values: []float32{1, 2, 3}}},
				Options:    waitForCompletion,
			})
			require.NoError(t, err)
			assert.Equal(t, search.IndexAckCompleted, res.Ack)
		}

		assert.GreaterOrEqual(t, statusReads.Load(), int32(3))
		assert.Equal(t, int32(1), streamDials.Load(), "the unavailable stream is remembered")
	})

	t.Run("streams the terminal task change", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/tasks/stream":
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			case r.URL.Path == "/indexes/books/documents":
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 13, "status": "enqueued", "type": "documentAdditionOrUpdate"})
			case strings.HasPrefix(r.URL.Path, "/tasks/"):
				writeJSON(t, w, http.StatusOK, map[string]any{"uid": 13, "status": "succeeded", "type": "documentAdditionOrUpdate"})
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			}
		}))
		t.Cleanup(server.Close)

		component := initializedVector(t, server.URL)
		res, err := component.Upsert(t.Context(), &vector.UpsertRequest{
			Collection: "books",
			Records:    []vector.Record{{ID: "1", Values: []float32{1, 2, 3}}},
			Options:    waitForCompletion,
		})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, res.Ack)
	})

	t.Run("fails before enqueueing when the stream rejects the credentials", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/tasks/stream" {
				t.Errorf("nothing must be enqueued: %s %s", r.Method, r.URL.Path)
				return
			}
			writeJSON(t, w, http.StatusForbidden, map[string]any{"message": "the provided API key is invalid", "code": "invalid_api_key", "type": "auth"})
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		_, err := component.Upsert(t.Context(), &vector.UpsertRequest{
			Collection: "books",
			Records:    []vector.Record{{ID: "1", Values: []float32{1, 2, 3}}},
			Options:    waitForCompletion,
		})

		require.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestGetReturnsFoundRecordsInRequestOrder(t *testing.T) {
	t.Parallel()

	var fetch map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/documents/fetch", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&fetch))
		writeJSON(t, w, http.StatusOK, map[string]any{"results": []map[string]any{
			{
				"id":           "1",
				"daprMetadata": map[string]any{"author": "pride-and-prejudice", "publisher": map[string]any{"city": "london"}},
				"daprPayload":  base64.StdEncoding.EncodeToString([]byte("payload")),
				"_vectors":     map[string]any{"default": []float32{1, 2, 3}},
			},
			{"id": "3", "_vectors": map[string]any{"default": map[string]any{"embeddings": []any{[]any{4.0}}, "regenerate": false}}},
		}, "total": 2})
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	got, err := component.Get(t.Context(), &vector.GetRequest{Collection: "books", IDs: []string{"3", "2", "1"}, IncludeValues: true})

	require.NoError(t, err)
	assert.Equal(t, true, fetch["retrieveVectors"])
	assert.Equal(t, []any{"3", "2", "1"}, fetch["ids"])
	require.Len(t, got.Records, 2, "records that are not found are omitted")
	assert.Equal(t, "3", got.Records[0].ID)
	assert.Equal(t, []float32{4}, got.Records[0].Values)
	assert.Equal(t, "1", got.Records[1].ID)
	assert.Equal(t, []float32{1, 2, 3}, got.Records[1].Values)
	assert.Equal(t, []byte("payload"), got.Records[1].Payload)
	assert.Equal(t, map[string]any{"author": "pride-and-prejudice", "publisher": map[string]any{"city": "london"}}, got.Records[1].Metadata)
}

func TestDelete(t *testing.T) {
	t.Parallel()

	t.Run("returns queued without waiting", func(t *testing.T) {
		t.Parallel()

		var deleted []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/indexes/books/documents/delete-batch", r.URL.Path)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&deleted))
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 11, "status": "enqueued", "type": "documentDeletion"})
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		res, err := component.Delete(t.Context(), &vector.DeleteRequest{Collection: "books", IDs: []string{"1", "missing"}})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckQueued, res.Ack)
		assert.Equal(t, []string{"1", "missing"}, deleted, "missing ids are not an error")
	})

	t.Run("waits for the deletion task", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name       string
			taskStatus string
			opts       search.IndexingOptions
			wantAck    search.IndexAck
			wantCode   codes.Code
		}{
			{name: "succeeded", taskStatus: "succeeded", opts: waitForCompletion, wantAck: search.IndexAckCompleted},
			{
				name:       "still processing with continue async",
				taskStatus: "processing",
				opts:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 30 * time.Millisecond, OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync},
				wantAck:    search.IndexAckQueued,
			},
			{
				name:       "still processing with fail request",
				taskStatus: "processing",
				opts:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: 30 * time.Millisecond, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
				wantCode:   codes.DeadlineExceeded,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/tasks/stream":
						writeJSON(t, w, http.StatusNotFound, map[string]any{"message": "not found", "code": "not_found", "type": "invalid_request"})
					case "/indexes/books/documents/delete-batch":
						writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 14, "status": "enqueued", "type": "documentDeletion"})
					case "/tasks/14":
						writeJSON(t, w, http.StatusOK, map[string]any{"uid": 14, "status": tt.taskStatus, "type": "documentDeletion"})
					default:
						t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					}
				}))
				defer server.Close()

				component := initializedVector(t, server.URL)
				res, err := component.Delete(t.Context(), &vector.DeleteRequest{Collection: "books", IDs: []string{"1"}, Options: tt.opts})

				if tt.wantCode != codes.OK {
					require.Error(t, err)
					assert.Equal(t, tt.wantCode, status.Code(err))
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.wantAck, res.Ack)
			})
		}
	})

	t.Run("nothing to delete completes without the provider", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		res, err := component.Delete(t.Context(), &vector.DeleteRequest{Collection: "books"})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, res.Ack)
	})

	t.Run("rejects invalid options", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		_, err := component.Delete(t.Context(), &vector.DeleteRequest{Collection: "books", IDs: []string{"1"}, Options: search.IndexingOptions{WaitTimeout: time.Second}})

		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, status.Convert(err).Message(), "INDEXING_MODE_WAIT_FOR_COMPLETION")
	})
}

func TestQueryTranslatesScoresAndThresholds(t *testing.T) {
	t.Parallel()

	var sent map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/search", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		writeJSON(t, w, http.StatusOK, map[string]any{"hits": []map[string]any{{
			"id":            "1",
			"daprMetadata":  map[string]any{"author": "pride-and-prejudice", "year": float64(2024)},
			"daprPayload":   base64.StdEncoding.EncodeToString([]byte("payload")),
			"_rankingScore": 0.9,
			"_vectors":      map[string]any{"default": []float32{1, 2, 3}},
		}}, "estimatedTotalHits": 1, "limit": 1})
	}))
	defer server.Close()

	threshold := 0.5
	component := initializedVector(t, server.URL)
	res, err := component.Query(t.Context(), &vector.QueryRequest{
		Collection:     "books",
		Vector:         &vector.Record{Values: []float32{1, 2, 3}},
		TopK:           1,
		Filter:         map[string]any{"author": "pride-and-prejudice", "year": map[string]any{"$gte": 2020}},
		IncludeValues:  true,
		IncludePayload: true,
		ScoreThreshold: &threshold,
	})

	require.NoError(t, err)
	assert.Equal(t, []any{1.0, 2.0, 3.0}, sent["vector"])
	assert.Equal(t, map[string]any{"embedder": "default", "semanticRatio": 1.0}, sent["hybrid"])
	assert.Equal(t, `daprMetadata.author = "pride-and-prejudice" AND daprMetadata.year >= 2020`, sent["filter"], "vector filters address record metadata")
	// A cosine threshold of 0.5 is (1 + 0.5) / 2 on Meilisearch's normalized
	// ranking score scale.
	assert.InDelta(t, 0.75, sent["rankingScoreThreshold"], 0.0001)

	assert.Equal(t, vector.DistanceMetricCosine, res.Metric)
	require.Len(t, res.Matches, 1)
	assert.Equal(t, "1", res.Matches[0].Record.ID)
	// A ranking score of 0.9 is a cosine similarity of 2*0.9 - 1.
	assert.InDelta(t, 0.8, res.Matches[0].Score, 0.0001)
	assert.Equal(t, []float32{1, 2, 3}, res.Matches[0].Record.Values)
	assert.Equal(t, []byte("payload"), res.Matches[0].Record.Payload)
	assert.Equal(t, map[string]any{"author": "pride-and-prejudice", "year": float64(2024)}, res.Matches[0].Record.Metadata)
}

func TestQueryOmitsValuesAndPayloadUnlessRequested(t *testing.T) {
	t.Parallel()

	var sent map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		writeJSON(t, w, http.StatusOK, map[string]any{"hits": []map[string]any{{
			"id":            "1",
			"daprPayload":   base64.StdEncoding.EncodeToString([]byte("payload")),
			"_rankingScore": 1.0,
			"_vectors":      map[string]any{"default": []float32{1, 2, 3}},
		}}})
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	res, err := component.Query(t.Context(), &vector.QueryRequest{Collection: "books", Vector: &vector.Record{Values: []float32{1, 2, 3}}})

	require.NoError(t, err)
	_, hasFilter := sent["filter"]
	assert.False(t, hasFilter)
	require.Len(t, res.Matches, 1)
	assert.Nil(t, res.Matches[0].Record.Values)
	assert.Nil(t, res.Matches[0].Record.Payload)
	assert.InDelta(t, 1.0, res.Matches[0].Score, 0.0001)
}

func TestQueryByIDUsesSimilarDocuments(t *testing.T) {
	t.Parallel()

	var sent map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/similar", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		writeJSON(t, w, http.StatusOK, map[string]any{"hits": []map[string]any{{"id": "2", "_rankingScore": 1.0}}, "id": "1"})
	}))
	defer server.Close()

	component := initializedVector(t, server.URL)
	res, err := component.Query(t.Context(), &vector.QueryRequest{
		Collection: "books", ByID: "1", TopK: 5,
		Filter: map[string]any{"author": "pride-and-prejudice"},
	})

	require.NoError(t, err)
	assert.Equal(t, "1", sent["id"])
	assert.Equal(t, "default", sent["embedder"])
	assert.Equal(t, `daprMetadata.author = "pride-and-prejudice"`, sent["filter"])
	assert.Equal(t, vector.DistanceMetricCosine, res.Metric)
	require.Len(t, res.Matches, 1)
	assert.Equal(t, "2", res.Matches[0].Record.ID)
	assert.InDelta(t, 1.0, res.Matches[0].Score, 0.0001)
}

func TestQueryRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		req          *vector.QueryRequest
		wantContains string
	}{
		{
			name:         "euclidean metric",
			req:          &vector.QueryRequest{Collection: "books", Vector: &vector.Record{Values: []float32{1}}, Metric: vector.DistanceMetricEuclidean},
			wantContains: "cosine",
		},
		{
			name:         "dot product metric",
			req:          &vector.QueryRequest{Collection: "books", Vector: &vector.Record{Values: []float32{1}}, Metric: vector.DistanceMetricDotProduct},
			wantContains: "cosine",
		},
		{
			name:         "neither vector nor id",
			req:          &vector.QueryRequest{Collection: "books"},
			wantContains: "exactly one of vector or id",
		},
		{
			name:         "both vector and id",
			req:          &vector.QueryRequest{Collection: "books", Vector: &vector.Record{Values: []float32{1}}, ByID: "1"},
			wantContains: "exactly one of vector or id",
		},
		{
			name:         "query vector without values",
			req:          &vector.QueryRequest{Collection: "books", Vector: &vector.Record{}},
			wantContains: "no values",
		},
		{
			name:         "unsupported filter operator",
			req:          &vector.QueryRequest{Collection: "books", Vector: &vector.Record{Values: []float32{1}}, Filter: map[string]any{"author": map[string]any{"$regex": "^h"}}},
			wantContains: "regular expressions",
		},
		{
			name:         "missing collection",
			req:          &vector.QueryRequest{},
			wantContains: "collection is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
			}))
			defer server.Close()

			component := initializedVector(t, server.URL)
			_, err := component.Query(t.Context(), tt.req)

			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
		})
	}
}

func TestBatchQuery(t *testing.T) {
	t.Parallel()

	t.Run("returns one result per query in request order", func(t *testing.T) {
		t.Parallel()

		var searches atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/indexes/books":
				writeJSON(t, w, http.StatusOK, map[string]any{"uid": "books", "primaryKey": "id"})
			case "/indexes/books/search":
				var sent map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
				searches.Add(1)
				values, _ := sent["vector"].([]any)
				if len(values) > 0 && values[0] == 3.0 {
					// The provider rejects one query of the batch.
					writeJSON(t, w, http.StatusBadRequest, map[string]any{"message": "invalid vector dimensions", "code": "invalid_search_vector", "type": "invalid_request"})
					return
				}
				writeJSON(t, w, http.StatusOK, map[string]any{
					"hits":               []map[string]any{{"id": fmt.Sprintf("hit-%v", values[0]), "_rankingScore": 1.0}},
					"estimatedTotalHits": 1,
				})
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			}
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		res, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "books", Queries: []vector.QueryRequest{
			{Vector: &vector.Record{Values: []float32{1}}},
			{Vector: &vector.Record{Values: []float32{2}}, ByID: "1"},
			{Vector: &vector.Record{Values: []float32{3}}},
			{Vector: &vector.Record{Values: []float32{4}}, Collection: "ignored"},
		}})

		require.NoError(t, err)
		require.Len(t, res.Results, 4)

		require.NotNil(t, res.Results[0].Response)
		require.NoError(t, res.Results[0].Error)
		assert.Equal(t, "hit-1", res.Results[0].Response.Matches[0].Record.ID)
		assert.Equal(t, vector.DistanceMetricCosine, res.Results[0].Response.Metric)

		assert.Nil(t, res.Results[1].Response, "a query that fails validation is an error result")
		assert.Equal(t, codes.InvalidArgument, status.Code(res.Results[1].Error))

		assert.Nil(t, res.Results[2].Response, "a query the provider rejects is an error result")
		assert.Equal(t, codes.InvalidArgument, status.Code(res.Results[2].Error))

		require.NotNil(t, res.Results[3].Response, "the remaining queries still run")
		assert.Equal(t, "hit-4", res.Results[3].Response.Matches[0].Record.ID)
		assert.Equal(t, int32(3), searches.Load(), "an invalid query never reaches the provider")
	})

	t.Run("a missing collection fails the call", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/indexes/books" {
				t.Errorf("no query must run: %s %s", r.Method, r.URL.Path)
				return
			}
			writeJSON(t, w, http.StatusNotFound, map[string]any{"message": "Index `books` not found.", "code": "index_not_found", "type": "invalid_request"})
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		_, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "books", Queries: []vector.QueryRequest{
			{Vector: &vector.Record{Values: []float32{1}}},
		}})

		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("an empty batch never invokes the provider", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
		}))
		defer server.Close()

		component := initializedVector(t, server.URL)
		res, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "books"})

		require.NoError(t, err)
		assert.Empty(t, res.Results)
	})

	t.Run("a closed component fails the call", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
		}))
		defer server.Close()

		component := NewMeilisearch(kitlogger.NewLogger("test"))
		require.NoError(t, component.Init(t.Context(), vector.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": server.URL}}}))
		require.NoError(t, component.Close())

		_, err := component.BatchQuery(t.Context(), &vector.BatchQueryRequest{Collection: "books", Queries: []vector.QueryRequest{
			{Vector: &vector.Record{Values: []float32{1}}},
		}})

		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

func TestValuesFromVectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  []float32
	}{
		{
			name:  "raw embedding array",
			value: map[string]any{"default": []any{1.0, 2.0}},
			want:  []float32{1, 2},
		},
		{
			name:  "embeddings object",
			value: map[string]any{"default": map[string]any{"embeddings": []any{[]any{1.0, 2.0}}, "regenerate": false}},
			want:  []float32{1, 2},
		},
		{
			name:  "flat embeddings object",
			value: map[string]any{"default": map[string]any{"embeddings": []any{1.0, 2.0}}},
			want:  []float32{1, 2},
		},
		{
			name:  "other embedders are ignored",
			value: map[string]any{"title": []any{3.0}},
		},
		{
			name:  "not an object",
			value: "nope",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, valuesFromVectors(tt.value))
		})
	}
}

func TestScoreTranslationRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		cosine             float64
		wantRankingScore   float64
		rankingScore       float64
		wantCosineFromRank float64
	}{
		{name: "perfect match", cosine: 1, wantRankingScore: 1, rankingScore: 1, wantCosineFromRank: 1},
		{name: "orthogonal", cosine: 0, wantRankingScore: 0.5, rankingScore: 0.5, wantCosineFromRank: 0},
		{name: "opposite", cosine: -1, wantRankingScore: 0, rankingScore: 0, wantCosineFromRank: -1},
		{name: "clamps above the range", cosine: 2, wantRankingScore: 1, rankingScore: 1, wantCosineFromRank: 1},
		{name: "clamps below the range", cosine: -2, wantRankingScore: 0, rankingScore: 0, wantCosineFromRank: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.InDelta(t, tt.wantRankingScore, rankingScoreThreshold(tt.cosine), 0.0001)
			assert.InDelta(t, tt.wantCosineFromRank, cosineFromRankingScore(tt.rankingScore), 0.0001)
		})
	}
}

func initializedVector(t *testing.T, host string) vector.Vector {
	t.Helper()
	component := NewMeilisearch(kitlogger.NewLogger("test"))
	require.NoError(t, component.Init(t.Context(), vector.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": host}}}))
	t.Cleanup(func() { require.NoError(t, component.Close()) })
	return component
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(body))
}
