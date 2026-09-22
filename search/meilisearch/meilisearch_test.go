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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	contribmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	kitlogger "github.com/dapr/kit/logger"
)

var waitForCompletion = search.IndexingOptions{
	Mode:          search.IndexingModeWaitForCompletion,
	WaitTimeout:   5 * time.Second,
	OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
}

func TestInit(t *testing.T) {
	t.Parallel()

	t.Run("happy path with api key", func(t *testing.T) {
		t.Parallel()
		var seenAuth atomic.Value
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seenAuth.Store(r.Header.Get("Authorization"))
			require.Equal(t, "/indexes", r.URL.Path)
			writeJSON(t, w, http.StatusOK, map[string]any{"results": []any{}, "limit": 20})
		}))
		defer server.Close()

		component := NewMeilisearch(kitlogger.NewLogger("test"))
		err := component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": server.URL, "apiKey": "secret"}}})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, component.Close()) })

		_, err = component.ListIndexes(t.Context(), &search.ListIndexesRequest{})
		require.NoError(t, err)
		assert.Equal(t, "Bearer secret", seenAuth.Load())
	})

	t.Run("missing host", func(t *testing.T) {
		t.Parallel()
		component := NewMeilisearch(kitlogger.NewLogger("test"))
		err := component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{}}})
		require.ErrorContains(t, err, "host is required")
	})
}

func TestIndexLifecycle(t *testing.T) {
	t.Parallel()

	var settings map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes":
			if r.Method == http.MethodPost {
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexCreation"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"results": []map[string]any{{"uid": "books", "primaryKey": "id"}}, "limit": 20, "total": 1})
		case "/tasks/1", "/tasks/2", "/tasks/3":
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": 1, "status": "succeeded", "type": "indexCreation"})
		case "/indexes/books/settings":
			if r.Method == http.MethodPatch {
				require.NoError(t, json.NewDecoder(r.Body).Decode(&settings))
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 2, "status": "enqueued", "type": "settingsUpdate"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"filterableAttributes": []string{"author"}, "sortableAttributes": []string{"year", "id"}, "searchableAttributes": []string{"title"}})
		case "/indexes/books":
			if r.Method == http.MethodDelete {
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 3, "status": "enqueued", "type": "indexDeletion"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": "books", "primaryKey": "id"})
		case "/indexes/books/stats":
			writeJSON(t, w, http.StatusOK, map[string]any{"numberOfDocuments": 2})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)

	require.NoError(t, component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "books", Metadata: map[string]string{
		"searchableAttributes": "title",
		"filterableAttributes": "author",
		"sortableAttributes":   "year",
	}}))
	assert.Equal(t, []any{"title"}, settings["searchableAttributes"])
	assert.Equal(t, []any{"author"}, settings["filterableAttributes"])
	assert.Equal(t, []any{"year", "id"}, settings["sortableAttributes"], "the document id is always sortable for the pagination tie-breaker")

	got, err := component.GetIndex(t.Context(), &search.GetIndexRequest{Index: "books"})
	require.NoError(t, err)
	assert.Equal(t, "books", got.Index)
	assert.Equal(t, uint64(2), got.DocumentCount)
	assert.Equal(t, "id", got.Properties["primaryKey"])
	assert.Equal(t, "author", got.Properties["filterableAttributes"])
	assert.Equal(t, "year,id", got.Properties["sortableAttributes"])

	indexes, err := component.ListIndexes(t.Context(), &search.ListIndexesRequest{})
	require.NoError(t, err)
	assert.Equal(t, []string{"books"}, indexes.Indexes)

	require.NoError(t, component.DeleteIndex(t.Context(), &search.DeleteIndexRequest{Index: "books"}))
}

func TestDeleteIndexReportsMissingIndex(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes/books":
			require.Equal(t, http.MethodDelete, r.Method)
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexDeletion"})
		case "/tasks/1":
			writeJSON(t, w, http.StatusOK, map[string]any{
				"uid": 1, "status": "failed", "type": "indexDeletion", "indexUid": "books",
				"error": map[string]any{"message": "Index `books` not found.", "code": "index_not_found", "type": "invalid_request"},
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	err := component.DeleteIndex(t.Context(), &search.DeleteIndexRequest{Index: "books"})

	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestCreateIndexAlwaysDeclaresTheIDSortable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		md   map[string]string
		want []any
	}{
		{name: "no settings", md: nil, want: []any{"id"}},
		{name: "caller settings are merged", md: map[string]string{"sortableAttributes": "year, price"}, want: []any{"year", "price", "id"}},
		{name: "id is not duplicated", md: map[string]string{"sortableAttributes": "id,year"}, want: []any{"id", "year"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var settings map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/indexes":
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

			component := initializedSearch(t, server.URL)
			require.NoError(t, component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "books", Metadata: tt.md}))

			assert.Equal(t, tt.want, settings["sortableAttributes"])
			_, hasFilterable := settings["filterableAttributes"]
			assert.False(t, hasFilterable, "unset settings are not sent")
		})
	}
}

func TestCreateIndexReportsAnExistingIndex(t *testing.T) {
	t.Parallel()

	var settingsUpdated atomic.Int32
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
			settingsUpdated.Add(1)
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	err := component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "books", Metadata: map[string]string{"filterableAttributes": "author"}})

	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), `"books" already exists`)
	assert.Zero(t, settingsUpdated.Load(), "the settings of an existing index are not reconciled")
}

func TestIndexDocumentsReturnsQueuedWithoutWaiting(t *testing.T) {
	t.Parallel()

	var posted []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes/books/documents":
			require.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 9, "status": "enqueued", "type": "documentAdditionOrUpdate"})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{
		Index: "books",
		Documents: []search.Document{{
			ID:       "1",
			Content:  []byte(`{"title":"Pride and Prejudice"}`),
			Metadata: map[string]string{"tenant": "acme"},
		}},
	})

	require.NoError(t, err)
	assert.Equal(t, search.IndexAckQueued, res.Ack)
	assert.Empty(t, res.FailedItems)

	require.Len(t, posted, 1)
	assert.Equal(t, "1", posted[0]["id"])
	assert.Equal(t, "Pride and Prejudice", posted[0]["title"])
	assert.Equal(t, map[string]any{"tenant": "acme"}, posted[0]["daprMetadata"])
}

func TestIndexDocumentsRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		req          *search.IndexDocumentsRequest
		wantCode     codes.Code
		wantContains string
	}{
		{
			name:         "missing index",
			req:          &search.IndexDocumentsRequest{},
			wantCode:     codes.InvalidArgument,
			wantContains: "index is required",
		},
		{
			name:         "empty id",
			req:          &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{{Content: []byte(`{}`)}}},
			wantCode:     codes.InvalidArgument,
			wantContains: "empty id",
		},
		{
			name: "duplicate ids",
			req: &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{
				{ID: "1", Content: []byte(`{}`)},
				{ID: "1", Content: []byte(`{}`)},
			}},
			wantCode:     codes.InvalidArgument,
			wantContains: "duplicate id",
		},
		{
			name: "wait fields without wait mode",
			req: &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{{ID: "1", Content: []byte(`{}`)}},
				Options: search.IndexingOptions{WaitTimeout: time.Second}},
			wantCode:     codes.InvalidArgument,
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

			component := initializedSearch(t, server.URL)
			_, err := component.IndexDocuments(t.Context(), tt.req)

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
		})
	}
}

func TestIndexDocumentsReportsContentFailuresBeforeEnqueue(t *testing.T) {
	t.Parallel()

	var posted []map[string]any
	var enqueued atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/documents", r.URL.Path)
		enqueued.Add(1)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
		writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 4, "status": "enqueued", "type": "documentAdditionOrUpdate"})
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{
		{ID: "1", Content: []byte(`{"title":"a"}`)},
		{ID: "2", Content: []byte(`["not an object"]`)},
		{ID: "3", Content: []byte(`{`)},
		{ID: "4"},
	}})

	require.NoError(t, err)
	assert.Equal(t, search.IndexAckQueued, res.Ack)
	require.Len(t, res.FailedItems, 3)
	wantMessage := status.Convert(search.ValidateDocumentContent(nil)).Message()
	for i, id := range []string{"2", "3", "4"} {
		assert.Equal(t, id, res.FailedItems[i].ID)
		assert.Equal(t, codes.InvalidArgument, res.FailedItems[i].Error.Code())
		assert.Equal(t, wantMessage, res.FailedItems[i].Error.Message(), "the item failure has the shape of the shared validator")
	}

	assert.Equal(t, int32(1), enqueued.Load())
	require.Len(t, posted, 1, "only valid documents are enqueued")
	assert.Equal(t, "1", posted[0]["id"])
}

func TestIndexDocumentsSkipsTheProviderWhenEveryItemFails(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Documents: []search.Document{
		{ID: "1", Content: []byte(`"scalar"`)},
	}})

	require.NoError(t, err)
	assert.Equal(t, search.IndexAckCompleted, res.Ack)
	require.Len(t, res.FailedItems, 1)
}

func TestIndexDocumentsWaitForCompletion(t *testing.T) {
	t.Parallel()

	t.Run("streams the terminal task change", func(t *testing.T) {
		t.Parallel()

		var enqueued atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/tasks/stream":
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			case "/indexes/books/documents":
				enqueued.Add(1)
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 15, "status": "enqueued", "type": "documentAdditionOrUpdate"})
			case "/tasks/15":
				// Reconciliation after registration already finds the task
				// terminal.
				writeJSON(t, w, http.StatusOK, map[string]any{"uid": 15, "status": "succeeded", "type": "documentAdditionOrUpdate"})
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			}
		}))
		// The stream handler only returns once the component closes its
		// connection, so the server must be closed after the component.
		t.Cleanup(server.Close)

		component := initializedSearch(t, server.URL)
		res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{
			Index:     "books",
			Documents: []search.Document{{ID: "1", Content: []byte(`{"title":"a"}`)}},
			Options:   waitForCompletion,
		})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, res.Ack)
		assert.Equal(t, int32(1), enqueued.Load())
	})

	t.Run("polls the task status when the stream is unavailable", func(t *testing.T) {
		t.Parallel()

		var enqueued, streamDials, statusReads atomic.Int32
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
				enqueued.Add(1)
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 16, "status": "enqueued", "type": "documentAdditionOrUpdate"})
			case "/tasks/16":
				taskStatus := "processing"
				if statusReads.Add(1) >= 3 {
					taskStatus = "succeeded"
				}
				writeJSON(t, w, http.StatusOK, map[string]any{"uid": 16, "status": taskStatus, "type": "documentAdditionOrUpdate"})
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			}
		}))
		defer server.Close()

		component := initializedSearch(t, server.URL)
		for range 2 {
			res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{
				Index:     "books",
				Documents: []search.Document{{ID: "1", Content: []byte(`{"title":"a"}`)}},
				Options:   waitForCompletion,
			})
			require.NoError(t, err)
			assert.Equal(t, search.IndexAckCompleted, res.Ack)
		}

		assert.Equal(t, int32(2), enqueued.Load())
		assert.GreaterOrEqual(t, statusReads.Load(), int32(4), "the task status is polled until the task is terminal")
		assert.Equal(t, int32(1), streamDials.Load(), "the unavailable stream is remembered")
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

		component := initializedSearch(t, server.URL)
		_, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{
			Index:     "books",
			Documents: []search.Document{{ID: "1", Content: []byte(`{"title":"a"}`)}},
			Options:   waitForCompletion,
		})

		require.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestGetDocumentsReturnsFoundDocumentsInRequestOrder(t *testing.T) {
	t.Parallel()

	var fetch map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/documents/fetch", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&fetch))
		// Meilisearch returns documents in its own order.
		writeJSON(t, w, http.StatusOK, map[string]any{
			"results": []map[string]any{
				{"id": "1", "title": "Pride and Prejudice", "daprMetadata": map[string]any{"tenant": "acme"}},
				{"id": "3", "title": "Frankenstein"},
			},
			"total": 2, "limit": 3, "offset": 0,
		})
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	got, err := component.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: "books", IDs: []string{"3", "2", "1"}, IncludeContent: true})

	require.NoError(t, err)
	assert.Equal(t, []any{"3", "2", "1"}, fetch["ids"])
	require.Len(t, got.Documents, 2, "documents that are not found are omitted")
	assert.Equal(t, "3", got.Documents[0].ID)
	assert.JSONEq(t, `{"title":"Frankenstein"}`, string(got.Documents[0].Content))
	assert.Nil(t, got.Documents[0].Metadata)
	assert.Equal(t, "1", got.Documents[1].ID)
	assert.JSONEq(t, `{"title":"Pride and Prejudice"}`, string(got.Documents[1].Content))
	assert.Equal(t, map[string]string{"tenant": "acme"}, got.Documents[1].Metadata)
}

func TestGetDocumentsWithoutContent(t *testing.T) {
	t.Parallel()

	var fetch map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/documents/fetch", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&fetch))
		writeJSON(t, w, http.StatusOK, map[string]any{"results": []map[string]any{{"id": "1"}}, "total": 1})
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	got, err := component.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: "books", IDs: []string{"1"}})

	require.NoError(t, err)
	assert.Equal(t, []any{"id", "daprMetadata"}, fetch["fields"])
	require.Len(t, got.Documents, 1)
	assert.Empty(t, got.Documents[0].Content)
}

func TestDeleteDocuments(t *testing.T) {
	t.Parallel()

	t.Run("returns queued without waiting", func(t *testing.T) {
		t.Parallel()

		var deleted []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/indexes/books/documents/delete-batch", r.URL.Path)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&deleted))
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 5, "status": "enqueued", "type": "documentDeletion"})
		}))
		defer server.Close()

		component := initializedSearch(t, server.URL)
		res, err := component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"1", "missing"}})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckQueued, res.Ack)
		assert.Equal(t, []string{"1", "missing"}, deleted, "missing ids are not an error")
	})

	t.Run("waits for the deletion task", func(t *testing.T) {
		t.Parallel()

		var statusReads atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/tasks/stream":
				writeJSON(t, w, http.StatusNotFound, map[string]any{"message": "not found", "code": "not_found", "type": "invalid_request"})
			case "/indexes/books/documents/delete-batch":
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 6, "status": "enqueued", "type": "documentDeletion"})
			case "/tasks/6":
				statusReads.Add(1)
				writeJSON(t, w, http.StatusOK, map[string]any{"uid": 6, "status": "succeeded", "type": "documentDeletion"})
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			}
		}))
		defer server.Close()

		component := initializedSearch(t, server.URL)
		res, err := component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"1"}, Options: waitForCompletion})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, res.Ack)
		assert.GreaterOrEqual(t, statusReads.Load(), int32(1))
	})

	t.Run("a failed deletion task fails the request", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/tasks/stream":
				writeJSON(t, w, http.StatusNotFound, map[string]any{"message": "not found", "code": "not_found", "type": "invalid_request"})
			case "/indexes/books/documents/delete-batch":
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 7, "status": "enqueued", "type": "documentDeletion"})
			case "/tasks/7":
				writeJSON(t, w, http.StatusOK, map[string]any{
					"uid": 7, "status": "failed", "type": "documentDeletion",
					"error": map[string]any{"message": "Index `books` not found.", "code": "index_not_found", "type": "invalid_request"},
				})
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			}
		}))
		defer server.Close()

		component := initializedSearch(t, server.URL)
		_, err := component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"1"}, Options: waitForCompletion})

		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("nothing to delete completes without the provider", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
		}))
		defer server.Close()

		component := initializedSearch(t, server.URL)
		res, err := component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", Options: waitForCompletion})

		require.NoError(t, err)
		assert.Equal(t, search.IndexAckCompleted, res.Ack)
	})

	t.Run("rejects invalid requests", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			req          *search.DeleteDocumentsRequest
			wantContains string
		}{
			{name: "missing index", req: &search.DeleteDocumentsRequest{IDs: []string{"1"}}, wantContains: "index is required"},
			{
				name:         "wait fields without wait mode",
				req:          &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"1"}, Options: search.IndexingOptions{OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest}},
				wantContains: "INDEXING_MODE_WAIT_FOR_COMPLETION",
			},
			{
				name:         "wait mode without a timeout action",
				req:          &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"1"}, Options: search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: time.Second}},
				wantContains: "on_wait_timeout is required",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
				}))
				defer server.Close()

				component := initializedSearch(t, server.URL)
				_, err := component.DeleteDocuments(t.Context(), tt.req)

				require.Error(t, err)
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
			})
		}
	})
}

func TestSearch(t *testing.T) {
	t.Parallel()

	var sent map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/search", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		writeJSON(t, w, http.StatusOK, map[string]any{
			"hits": []map[string]any{{
				"id":            "1",
				"title":         "Pride and Prejudice",
				"daprMetadata":  map[string]any{"tenant": "acme"},
				"_rankingScore": 0.87,
				"_formatted":    map[string]any{"title": "<em>Pride</em> and Prejudice"},
			}},
			"estimatedTotalHits": 5,
			"offset":             0,
			"limit":              1,
		})
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	res, err := component.Search(t.Context(), &search.SearchRequest{
		Index: "books", Text: "Pride and Prejudice", IncludeContent: true, HighlightFields: []string{"title"}, TopK: 1,
		Filter: map[string]any{"author": "jane-austen"},
		Sort:   []search.SortClause{{Field: "year", Order: search.SortOrderDesc}},
	})

	require.NoError(t, err)
	assert.Equal(t, "Pride and Prejudice", sent["q"])
	assert.Equal(t, []any{"title"}, sent["attributesToHighlight"])
	assert.Equal(t, `author = "jane-austen"`, sent["filter"], "search filters address content fields directly")
	assert.Equal(t, []any{"year:desc", "id:asc"}, sent["sort"], "the document id is appended as a stable tie-breaker")

	require.Len(t, res.Hits, 1)
	assert.Equal(t, "1", res.Hits[0].Document.ID)
	assert.JSONEq(t, `{"title":"Pride and Prejudice"}`, string(res.Hits[0].Document.Content))
	assert.Equal(t, map[string]string{"tenant": "acme"}, res.Hits[0].Document.Metadata)
	assert.InDelta(t, 0.87, res.Hits[0].Score, 0.0001)
	assert.Equal(t, map[string]string{"title": "<em>Pride</em> and Prejudice"}, res.Hits[0].Highlights)

	require.NotNil(t, res.TotalHits)
	assert.Equal(t, uint64(5), *res.TotalHits)
	assert.Equal(t, search.TotalHitsRelationEstimate, res.TotalHitsRelation)
	assert.NotEmpty(t, res.ContinuationToken)
}

func TestSearchPagination(t *testing.T) {
	t.Parallel()

	var offsets []any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sent map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		offsets = append(offsets, sent["offset"])
		writeJSON(t, w, http.StatusOK, map[string]any{
			"hits":      []map[string]any{{"id": "1", "_rankingScore": 1}},
			"totalHits": 2,
			"offset":    sent["offset"],
			"limit":     1,
		})
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	req := &search.SearchRequest{Index: "books", Text: "pride-and-prejudice", TopK: 1}

	first, err := component.Search(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, search.TotalHitsRelationExact, first.TotalHitsRelation)
	require.NotEmpty(t, first.ContinuationToken)

	req.ContinuationToken = first.ContinuationToken
	second, err := component.Search(t.Context(), req)
	require.NoError(t, err)
	assert.Empty(t, second.ContinuationToken, "the last page has no token")
	assert.Equal(t, []any{nil, float64(1)}, offsets)
}

func TestSearchRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	fingerprintOfAnotherQuery := commonmeilisearch.EncodeContinuationToken(commonmeilisearch.QueryFingerprint("search", "other"), 10)

	tests := []struct {
		name         string
		req          *search.SearchRequest
		wantCode     codes.Code
		wantContains string
	}{
		{
			name:         "missing index",
			req:          &search.SearchRequest{},
			wantCode:     codes.InvalidArgument,
			wantContains: "index is required",
		},
		{
			name:         "text and native together",
			req:          &search.SearchRequest{Index: "books", Text: "a", Native: map[string]any{"matchingStrategy": "all"}},
			wantCode:     codes.InvalidArgument,
			wantContains: "mutually exclusive",
		},
		{
			name:         "native query with its own pagination",
			req:          &search.SearchRequest{Index: "books", Native: map[string]any{"offset": 10}},
			wantCode:     codes.InvalidArgument,
			wantContains: "must not set",
		},
		{
			name:         "native query with its own sort",
			req:          &search.SearchRequest{Index: "books", Native: map[string]any{"sort": []any{"year:asc"}}},
			wantCode:     codes.InvalidArgument,
			wantContains: "must not set",
		},
		{
			name:         "malformed continuation token",
			req:          &search.SearchRequest{Index: "books", Text: "a", ContinuationToken: "nonsense token"},
			wantCode:     codes.InvalidArgument,
			wantContains: "malformed",
		},
		{
			name:         "continuation token of another query",
			req:          &search.SearchRequest{Index: "books", Text: "a", ContinuationToken: fingerprintOfAnotherQuery},
			wantCode:     codes.InvalidArgument,
			wantContains: "does not match the request",
		},
		{
			name:         "unsupported filter operator",
			req:          &search.SearchRequest{Index: "books", Text: "a", Filter: map[string]any{"title": map[string]any{"$regex": "^a"}}},
			wantCode:     codes.InvalidArgument,
			wantContains: "regular expressions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
			}))
			defer server.Close()

			component := initializedSearch(t, server.URL)
			_, err := component.Search(t.Context(), tt.req)

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
		})
	}
}

func TestProviderErrorsBecomeStatusErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		body       map[string]any
		wantCode   codes.Code
	}{
		{
			name:       "missing index",
			statusCode: http.StatusNotFound,
			body:       map[string]any{"message": "index books not found", "code": "index_not_found", "type": "invalid_request"},
			wantCode:   codes.NotFound,
		},
		{
			name:       "invalid api key",
			statusCode: http.StatusForbidden,
			body:       map[string]any{"message": "invalid api key", "code": "invalid_api_key", "type": "auth"},
			wantCode:   codes.Unauthenticated,
		},
		{
			name:       "internal provider error",
			statusCode: http.StatusInternalServerError,
			body:       map[string]any{"message": "boom", "code": "internal", "type": "internal"},
			wantCode:   codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(t, w, tt.statusCode, tt.body)
			}))
			defer server.Close()

			component := initializedSearch(t, server.URL)
			_, err := component.Search(t.Context(), &search.SearchRequest{Index: "books", Text: "a"})

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
		})
	}
}

func TestClosedComponentRejectsRequests(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	component := NewMeilisearch(kitlogger.NewLogger("test"))
	require.NoError(t, component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": server.URL}}}))
	require.NoError(t, component.Close())

	_, err := component.ListIndexes(t.Context(), &search.ListIndexesRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "books", IDs: []string{"1"}})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func initializedSearch(t *testing.T, host string) search.Search {
	t.Helper()
	component := NewMeilisearch(kitlogger.NewLogger("test"))
	require.NoError(t, component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": host}}}))
	t.Cleanup(func() { require.NoError(t, component.Close()) })
	return component
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(body))
}
