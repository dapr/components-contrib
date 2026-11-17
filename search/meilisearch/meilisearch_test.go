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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	meilisearchgo "github.com/meilisearch/meilisearch-go"

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
		case "/indexes/books/settings/ranking-rules":
			writeJSON(t, w, http.StatusOK, defaultRankingRules)
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
				case "/indexes/books/settings/ranking-rules":
					writeJSON(t, w, http.StatusOK, defaultRankingRules)
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
			assert.Equal(t, []any{"sort", "words", "typo", "proximity", "attributeRank", "wordPosition", "exactness"}, settings["rankingRules"],
				"an explicit sort orders the results before relevance does")
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

func TestCreateIndexRollsBackACreationThatOutlivesTheRequest(t *testing.T) {
	t.Parallel()

	var deleted, configured atomic.Int32
	// The request ends on the first poll of the creation task, which only
	// completes once it has.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes":
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexCreation"})
		case "/tasks/1":
			taskStatus := "succeeded"
			if ctx.Err() == nil {
				cancel()
				taskStatus = "processing"
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": 1, "status": taskStatus, "type": "indexCreation", "indexUid": "books"})
		case "/indexes/books":
			if r.Method != http.MethodDelete {
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				return
			}
			deleted.Add(1)
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 3, "status": "enqueued", "type": "indexDeletion"})
		case "/tasks/3":
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": 3, "status": "succeeded", "type": "indexDeletion"})
		default:
			configured.Add(1)
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: "books"})

	require.Error(t, err)
	assert.Equal(t, codes.Canceled, status.Code(err))
	assert.Equal(t, int32(1), deleted.Load(), "an unconfigured index must not be left behind")
	assert.Zero(t, configured.Load())
}

func TestCreateIndexRollsBackWhenTheSettingsCannotBeApplied(t *testing.T) {
	t.Parallel()

	enqueued := func(taskUID int) func(*testing.T, http.ResponseWriter) {
		return func(t *testing.T, w http.ResponseWriter) {
			t.Helper()
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": taskUID, "status": "enqueued", "type": "settingsUpdate"})
		}
	}
	tests := []struct {
		name string
		// settings answers the settings update of the new index.
		settings func(*testing.T, http.ResponseWriter)
		// settingsTask is the settings task as the task status API reports it.
		settingsTask map[string]any
		// timeout, when set, ends the request while the settings are applied.
		timeout  time.Duration
		wantCode codes.Code
	}{
		{
			name: "the settings are rejected",
			settings: func(t *testing.T, w http.ResponseWriter) {
				t.Helper()
				writeJSON(t, w, http.StatusBadRequest, map[string]any{"message": "invalid settings", "code": "invalid_settings_sortable_attributes", "type": "invalid_request"})
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "the settings task fails",
			settings: enqueued(2),
			settingsTask: map[string]any{
				"uid": 2, "status": "failed", "type": "settingsUpdate", "indexUid": "books",
				"error": map[string]any{"message": "boom", "code": "internal", "type": "internal"},
			},
			wantCode: codes.Internal,
		},
		{
			name:         "the request ends while the settings are applied",
			settings:     enqueued(2),
			settingsTask: map[string]any{"uid": 2, "status": "processing", "type": "settingsUpdate", "indexUid": "books"},
			timeout:      150 * time.Millisecond,
			wantCode:     codes.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var deleted atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/indexes":
					writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexCreation"})
				case "/tasks/1":
					writeJSON(t, w, http.StatusOK, map[string]any{"uid": 1, "status": "succeeded", "type": "indexCreation"})
				case "/indexes/books/settings/ranking-rules":
					writeJSON(t, w, http.StatusOK, defaultRankingRules)
				case "/indexes/books/settings":
					tt.settings(t, w)
				case "/tasks/2":
					writeJSON(t, w, http.StatusOK, tt.settingsTask)
				case "/indexes/books":
					if r.Method != http.MethodDelete {
						t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
						return
					}
					deleted.Add(1)
					writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 3, "status": "enqueued", "type": "indexDeletion"})
				case "/tasks/3":
					writeJSON(t, w, http.StatusOK, map[string]any{"uid": 3, "status": "succeeded", "type": "indexDeletion"})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				}
			}))
			defer server.Close()

			ctx := t.Context()
			if tt.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}
			component := initializedSearch(t, server.URL)
			err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: "books", Metadata: map[string]string{"sortableAttributes": "year"}})

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err), "the failure that caused the rollback is reported")
			assert.Equal(t, int32(1), deleted.Load(), "an index that could not be configured must not be left behind")
		})
	}
}

func TestIndexDocumentsReturnsQueuedWithoutWaiting(t *testing.T) {
	t.Parallel()

	var posted []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes/books":
			serveIndexLookup(t, w, r)
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

func TestIndexDocumentsReportsItemFailuresBeforeEnqueue(t *testing.T) {
	t.Parallel()

	var posted []map[string]any
	var enqueued atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/indexes/books" {
			serveIndexLookup(t, w, r)
			return
		}
		require.Equal(t, "/indexes/books/documents", r.URL.Path)
		enqueued.Add(1)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
		writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 4, "status": "enqueued", "type": "documentAdditionOrUpdate"})
	}))
	defer server.Close()

	contentMessage := status.Convert(search.ValidateDocumentContent(nil)).Message()
	tests := []struct {
		doc          search.Document
		wantContains string
	}{
		// Content that is not a JSON object has the shape of the shared
		// validator wherever it is identified.
		{doc: search.Document{ID: "array", Content: []byte(`["not an object"]`)}, wantContains: contentMessage},
		{doc: search.Document{ID: "truncated", Content: []byte(`{`)}, wantContains: contentMessage},
		{doc: search.Document{ID: "empty"}, wantContains: contentMessage},
		{doc: search.Document{ID: "bad-utf8", Content: []byte("{\"a\":\"\x80\"}")}, wantContains: contentMessage},
		// Meilisearch rejects the whole batch for one identifier it cannot
		// store.
		{doc: search.Document{ID: "user:42", Content: []byte(`{"title":"a"}`)}, wantContains: "not a valid Meilisearch document identifier"},
		{doc: search.Document{ID: strings.Repeat("x", 512), Content: []byte(`{"title":"a"}`)}, wantContains: "not a valid Meilisearch document identifier"},
		// Attributes the component or Meilisearch own would be overwritten on
		// write or dropped on read.
		{doc: search.Document{ID: "own-id", Content: []byte(`{"title":"a","id":"other"}`)}, wantContains: `must not use the attribute "id"`},
		{doc: search.Document{ID: "own-metadata", Content: []byte(`{"daprMetadata":{"tenant":"x"}}`)}, wantContains: `must not use the attribute "daprMetadata"`},
		{doc: search.Document{ID: "geo", Content: []byte(`{"title":"a","_geo":{"lat":1,"lng":2}}`)}, wantContains: `must not use the attribute "_geo"`},
		{doc: search.Document{ID: "underscore", Content: []byte(`{"_foo":"x"}`)}, wantContains: `must not use the attribute "_foo"`},
	}
	docs := make([]search.Document, 0, 1+len(tests))
	docs = append(docs, search.Document{ID: "ok-1", Content: []byte(`{"title":"a","identifier":"kept"}`)})
	for _, tt := range tests {
		docs = append(docs, tt.doc)
	}

	component := initializedSearch(t, server.URL)
	res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "books", Documents: docs})

	require.NoError(t, err)
	assert.Equal(t, search.IndexAckQueued, res.Ack)
	require.Len(t, res.FailedItems, len(tests), "failed items are reported in request order")
	for i, tt := range tests {
		assert.Equal(t, tt.doc.ID, res.FailedItems[i].ID)
		assert.Equal(t, codes.InvalidArgument, res.FailedItems[i].Error.Code())
		assert.Contains(t, res.FailedItems[i].Error.Message(), tt.wantContains)
	}

	assert.Equal(t, int32(1), enqueued.Load())
	require.Len(t, posted, 1, "only valid documents are enqueued")
	assert.Equal(t, "ok-1", posted[0]["id"])
	assert.Equal(t, "kept", posted[0]["identifier"])
}

func TestIndexDocumentsEnqueuesNothingWhenEveryItemFails(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/indexes/books" {
			serveIndexLookup(t, w, r)
			return
		}
		t.Errorf("nothing must be enqueued: %s %s", r.Method, r.URL.Path)
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

func TestIndexDocumentsReportsAMissingIndex(t *testing.T) {
	t.Parallel()

	for _, options := range []search.IndexingOptions{{}, waitForCompletion} {
		// Meilisearch would create the index on the first document addition,
		// so the component must not enqueue anything.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/indexes/books" {
				t.Errorf("nothing must be enqueued: %s %s", r.Method, r.URL.Path)
				return
			}
			writeJSON(t, w, http.StatusNotFound, map[string]any{"message": "Index `books` not found.", "code": "index_not_found", "type": "invalid_request"})
		}))

		component := initializedSearch(t, server.URL)
		res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{
			Index:     "books",
			Documents: []search.Document{{ID: "1", Content: []byte(`{"title":"a"}`)}, {ID: "2", Content: []byte(`[]`)}},
			Options:   options,
		})

		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.Nil(t, res, "a request-wide failure is not reported per item")
		server.Close()
	}
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
			case "/indexes/books":
				serveIndexLookup(t, w, r)
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
			case "/indexes/books":
				serveIndexLookup(t, w, r)
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
			switch r.URL.Path {
			case "/indexes/books":
				serveIndexLookup(t, w, r)
			case "/tasks/stream":
				writeJSON(t, w, http.StatusForbidden, map[string]any{"message": "the provided API key is invalid", "code": "invalid_api_key", "type": "auth"})
			default:
				t.Errorf("nothing must be enqueued: %s %s", r.Method, r.URL.Path)
			}
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

func TestGetDocumentsOmitsIDsThatCannotBeStored(t *testing.T) {
	t.Parallel()

	t.Run("an invalid id is not sent to the provider", func(t *testing.T) {
		t.Parallel()

		var fetch map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/indexes/books/documents/fetch", r.URL.Path)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&fetch))
			writeJSON(t, w, http.StatusOK, map[string]any{"results": []map[string]any{{"id": "1", "title": "a"}}, "total": 1})
		}))
		defer server.Close()

		component := initializedSearch(t, server.URL)
		got, err := component.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: "books", IDs: []string{"user:42", "1", ""}, IncludeContent: true})

		require.NoError(t, err, "Meilisearch fails the whole read for an identifier it cannot store")
		assert.Equal(t, []any{"1"}, fetch["ids"])
		assert.InDelta(t, 1, fetch["limit"], 0)
		require.Len(t, got.Documents, 1)
		assert.Equal(t, "1", got.Documents[0].ID)
	})

	t.Run("only invalid ids never invoke the provider", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("the provider must not be invoked: %s %s", r.Method, r.URL.Path)
		}))
		defer server.Close()

		component := initializedSearch(t, server.URL)
		got, err := component.GetDocuments(t.Context(), &search.GetDocumentsRequest{Index: "books", IDs: []string{"user:42", "a b"}})

		require.NoError(t, err)
		assert.Empty(t, got.Documents)
	})
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
	server := httptest.NewServer(withIndexSettings(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/books/search", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		writeJSON(t, w, http.StatusOK, map[string]any{
			"hits": []map[string]any{{
				"id":            "1",
				"title":         "Pride and Prejudice",
				"year":          1813,
				"daprMetadata":  map[string]any{"tenant": "acme"},
				"_rankingScore": 0.87,
				// Meilisearch formats every retrieved attribute, highlighted
				// or not, and renders numbers as strings.
				"_formatted": map[string]any{
					"id":           "1",
					"title":        "<em>Pride</em> and Prejudice",
					"year":         "1813",
					"daprMetadata": map[string]any{"tenant": "acme"},
				},
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
	assert.Equal(t, `"author" = "jane-austen"`, sent["filter"], "search filters address content fields directly")
	assert.Equal(t, []any{"year:desc", "id:asc"}, sent["sort"], "the document id is appended as a stable tie-breaker")
	assert.Equal(t, []any{"title", "title.*", "year", "year.*"}, sent["attributesToSearchOn"],
		"the text is matched against the content attributes, not the id or the metadata")

	require.Len(t, res.Hits, 1)
	assert.Equal(t, "1", res.Hits[0].Document.ID)
	assert.JSONEq(t, `{"title":"Pride and Prejudice","year":1813}`, string(res.Hits[0].Document.Content))
	assert.Equal(t, map[string]string{"tenant": "acme"}, res.Hits[0].Document.Metadata)
	assert.InDelta(t, 0.87, res.Hits[0].Score, 0.0001)
	assert.Equal(t, map[string]string{"title": "<em>Pride</em> and Prejudice"}, res.Hits[0].Highlights,
		"only the requested fields are reported as highlights")

	require.NotNil(t, res.TotalHits)
	assert.Equal(t, uint64(5), *res.TotalHits)
	assert.Equal(t, search.TotalHitsRelationEstimate, res.TotalHitsRelation)
	assert.NotEmpty(t, res.ContinuationToken)
}

func TestSearchReportsAZeroTotal(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(withIndexSettings(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, map[string]any{"hits": []map[string]any{}, "estimatedTotalHits": 0, "offset": 0, "limit": 20})
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	res, err := component.Search(t.Context(), &search.SearchRequest{Index: "books", Text: "no such book"})

	require.NoError(t, err)
	assert.Empty(t, res.Hits)
	require.NotNil(t, res.TotalHits, "an empty result still has a total")
	assert.Zero(t, *res.TotalHits)
	assert.Equal(t, search.TotalHitsRelationEstimate, res.TotalHitsRelation)
	assert.Empty(t, res.ContinuationToken)
}

func TestSearchMatchesTextAgainstContentOnly(t *testing.T) {
	t.Parallel()

	defaultSettings := map[string]any{"searchableAttributes": []string{"*"}}
	tests := []struct {
		name     string
		req      *search.SearchRequest
		settings map[string]any
		fields   map[string]any
		// want is the attributesToSearchOn sent, nil when none is.
		want []any
		// wantNoSearch is set when the component answers without searching.
		wantNoSearch bool
	}{
		{
			name:     "the default searchable attributes are narrowed to the content",
			req:      &search.SearchRequest{Text: "acme"},
			settings: defaultSettings,
			fields:   map[string]any{"id": 2, "daprMetadata": 2, "title": 2, "publisher": 1},
			want:     []any{"publisher", "publisher.*", "title", "title.*"},
		},
		{
			name:     "wildcard search fields are narrowed to the content",
			req:      &search.SearchRequest{Text: "acme", SearchFields: []string{"*"}},
			settings: defaultSettings,
			fields:   map[string]any{"id": 2, "daprMetadata": 2, "title": 2},
			want:     []any{"title", "title.*"},
		},
		{
			name:     "nested search fields of a content attribute are kept",
			req:      &search.SearchRequest{Text: "acme", SearchFields: []string{"publisher.*"}},
			settings: defaultSettings,
			want:     []any{"publisher.*"},
		},
		{
			name:     "explicit search fields are kept",
			req:      &search.SearchRequest{Text: "acme", SearchFields: []string{"title"}},
			settings: defaultSettings,
			want:     []any{"title"},
		},
		{
			name:     "explicit searchable attributes are left alone",
			req:      &search.SearchRequest{Text: "acme"},
			settings: map[string]any{"searchableAttributes": []string{"title"}},
		},
		{
			name:     "a query without text searches no attribute",
			req:      &search.SearchRequest{},
			settings: defaultSettings,
		},
		{
			name:         "no document has content to match",
			req:          &search.SearchRequest{Text: "acme"},
			settings:     defaultSettings,
			fields:       map[string]any{"id": 1, "daprMetadata": 1},
			wantNoSearch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var sent map[string]any
			var searched, statsRead atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/indexes/books/settings":
					writeJSON(t, w, http.StatusOK, tt.settings)
				case "/indexes/books/stats":
					statsRead.Add(1)
					writeJSON(t, w, http.StatusOK, map[string]any{"fieldDistribution": tt.fields})
				case "/indexes/books/search":
					searched.Add(1)
					require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
					writeJSON(t, w, http.StatusOK, map[string]any{"hits": []map[string]any{}, "estimatedTotalHits": 0})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				}
			}))
			defer server.Close()

			tt.req.Index = "books"
			component := initializedSearch(t, server.URL)
			res, err := component.Search(t.Context(), tt.req)
			require.NoError(t, err)

			if tt.wantNoSearch {
				assert.Zero(t, searched.Load())
				assert.Empty(t, res.Hits)
				require.NotNil(t, res.TotalHits)
				assert.Zero(t, *res.TotalHits)
				assert.Equal(t, search.TotalHitsRelationExact, res.TotalHitsRelation)
				return
			}
			require.Equal(t, int32(1), searched.Load())
			if tt.want == nil {
				assert.NotContains(t, sent, "attributesToSearchOn")
				assert.Zero(t, statsRead.Load(), "the field distribution is only read to narrow `*`")
				return
			}
			assert.Equal(t, tt.want, sent["attributesToSearchOn"])
		})
	}
}

func TestSearchReportsACappedTotalAsALowerBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		total        uint64
		wantRelation search.TotalHitsRelation
	}{
		{name: "below maxTotalHits", total: 999, wantRelation: search.TotalHitsRelationEstimate},
		{name: "at maxTotalHits", total: 1000, wantRelation: search.TotalHitsRelationLowerBound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The last page of what Meilisearch returns: it neither counts nor
			// returns hits past maxTotalHits.
			server := httptest.NewServer(withIndexSettings(t, func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, http.StatusOK, map[string]any{
					"hits":               []map[string]any{{"id": "1", "_rankingScore": 1}},
					"estimatedTotalHits": tt.total,
					"offset":             tt.total - 1,
					"limit":              20,
				})
			}))
			defer server.Close()

			component := initializedSearch(t, server.URL)
			res, err := component.Search(t.Context(), &search.SearchRequest{Index: "books"})

			require.NoError(t, err)
			require.NotNil(t, res.TotalHits)
			assert.Equal(t, tt.total, *res.TotalHits)
			assert.Equal(t, tt.wantRelation, res.TotalHitsRelation)
			assert.Empty(t, res.ContinuationToken)
		})
	}
}

func TestSearchNativeQueryKeepsThePortableGuarantees(t *testing.T) {
	t.Parallel()

	tenant := map[string]any{"tenant": "acme"}
	tests := []struct {
		name   string
		req    *search.SearchRequest
		assert func(t *testing.T, sent map[string]any)
	}{
		{
			name: "ranking scores cannot be disabled",
			req:  &search.SearchRequest{Native: map[string]any{"q": "emma", "showRankingScore": false}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, "emma", sent["q"])
				assert.Equal(t, true, sent["showRankingScore"])
			},
		},
		{
			name: "a native projection still returns the id and metadata",
			req:  &search.SearchRequest{Native: map[string]any{"attributesToRetrieve": []any{"title"}}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, []any{"title", "id", "daprMetadata"}, sent["attributesToRetrieve"])
			},
		},
		{
			name: "a wildcard projection is left alone",
			req:  &search.SearchRequest{Native: map[string]any{"attributesToRetrieve": []any{"*"}}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, []any{"*"}, sent["attributesToRetrieve"])
			},
		},
		{
			name: "a native filter narrows the portable filter",
			req:  &search.SearchRequest{Filter: tenant, Native: map[string]any{"filter": "author = 'brian-vaughan'"}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, []any{`"tenant" = "acme"`, "author = 'brian-vaughan'"}, sent["filter"])
			},
		},
		{
			name: "a native array filter narrows the portable filter",
			req:  &search.SearchRequest{Filter: tenant, Native: map[string]any{"filter": []any{[]any{"a = 1", "b = 2"}, "c = 3"}}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, []any{`"tenant" = "acme"`, []any{"a = 1", "b = 2"}, "c = 3"}, sent["filter"])
			},
		},
		{
			name: "an empty native filter does not drop the portable filter",
			req:  &search.SearchRequest{Filter: tenant, Native: map[string]any{"filter": nil}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, `"tenant" = "acme"`, sent["filter"])
			},
		},
		{
			name: "a native query without a filter keeps the portable filter as is",
			req:  &search.SearchRequest{Filter: tenant, Native: map[string]any{"q": "emma"}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, `"tenant" = "acme"`, sent["filter"])
			},
		},
		{
			name: "a native filter in another case still narrows the portable filter",
			req:  &search.SearchRequest{Filter: tenant, Native: map[string]any{"Filter": "tenant = 'other'"}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, []any{`"tenant" = "acme"`, "tenant = 'other'"}, sent["filter"])
			},
		},
		{
			name: "a native filter alone is passed through",
			req:  &search.SearchRequest{Native: map[string]any{"filter": "year > 1800"}},
			assert: func(t *testing.T, sent map[string]any) {
				t.Helper()
				assert.Equal(t, "year > 1800", sent["filter"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var sent map[string]any
			server := httptest.NewServer(withIndexSettings(t, func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
				writeJSON(t, w, http.StatusOK, map[string]any{
					"hits":               []map[string]any{{"id": "1", "title": "Emma", "_rankingScore": 0.5}},
					"estimatedTotalHits": 1,
				})
			}))
			defer server.Close()

			tt.req.Index = "books"
			component := initializedSearch(t, server.URL)
			res, err := component.Search(t.Context(), tt.req)

			require.NoError(t, err)
			tt.assert(t, sent)
			require.Len(t, res.Hits, 1)
			assert.Equal(t, "1", res.Hits[0].Document.ID)
			assert.InDelta(t, 0.5, res.Hits[0].Score, 0.0001)
		})
	}
}

func TestHighlightsFromFormatted(t *testing.T) {
	t.Parallel()

	// The shape Meilisearch returns: every retrieved attribute, highlighted or
	// not, numbers as strings and nested values kept nested.
	formatted := map[string]any{
		"id":           "1",
		"title":        "<em>hello</em> world",
		"n":            "7",
		"meta":         map[string]any{"name": "<em>hello</em> nested", "n": "5"},
		"tags":         []any{"<em>hello</em>", "x"},
		"authors":      []any{map[string]any{"name": "<em>hello</em> a"}, map[string]any{"name": "b"}},
		"a.b":          "dotted <em>hello</em>",
		"daprMetadata": map[string]any{"tenant": "acme"},
	}

	tests := []struct {
		name   string
		fields []string
		want   map[string]string
	}{
		{name: "nothing requested", fields: nil, want: nil},
		{name: "a top-level field", fields: []string{"title"}, want: map[string]string{"title": "<em>hello</em> world"}},
		{name: "a nested field is reported under its path", fields: []string{"meta.name"}, want: map[string]string{"meta.name": "<em>hello</em> nested"}},
		{name: "an array keeps its structure and its tags", fields: []string{"tags"}, want: map[string]string{"tags": `["<em>hello</em>","x"]`}},
		{name: "a path through an array", fields: []string{"authors.name"}, want: map[string]string{"authors.name": `["<em>hello</em> a","b"]`}},
		{name: "an attribute named with a dot", fields: []string{"a.b"}, want: map[string]string{"a.b": "dotted <em>hello</em>"}},
		{name: "fields the hit does not have", fields: []string{"missing", "meta.missing", "title.deeper"}, want: nil},
		{name: "reserved attributes are never reported", fields: []string{"id", "daprMetadata", "title"}, want: map[string]string{"title": "<em>hello</em> world"}},
		{
			name:   "every attribute",
			fields: []string{"*"},
			want: map[string]string{
				"title":   "<em>hello</em> world",
				"n":       "7",
				"meta":    `{"n":"5","name":"<em>hello</em> nested"}`,
				"tags":    `["<em>hello</em>","x"]`,
				"authors": `[{"name":"<em>hello</em> a"},{"name":"b"}]`,
				"a.b":     "dotted <em>hello</em>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, highlightsFromFormatted(formatted, tt.fields))
		})
	}

	assert.Nil(t, highlightsFromFormatted("not an object", []string{"title"}))
}

func TestFormattedFields(t *testing.T) {
	t.Parallel()

	assert.Empty(t, formattedFields(&meilisearchgo.SearchRequest{}))
	assert.Equal(t, []string{"title"}, formattedFields(&meilisearchgo.SearchRequest{AttributesToHighlight: []string{"title"}}))
	assert.Equal(t, []string{"title", "body"}, formattedFields(&meilisearchgo.SearchRequest{
		AttributesToHighlight: []string{"title"},
		AttributesToCrop:      []string{"body:20", "title"},
	}), "a native query can also ask for cropped attributes")
}

func TestSearchPagination(t *testing.T) {
	t.Parallel()

	var offsets []any
	server := httptest.NewServer(withIndexSettings(t, func(w http.ResponseWriter, r *http.Request) {
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
			name:         "native query with its own pagination in another case",
			req:          &search.SearchRequest{Index: "books", Native: map[string]any{"LIMIT": 1000}},
			wantCode:     codes.InvalidArgument,
			wantContains: "must not set",
		},
		{
			name:         "native query with its own sort in another case",
			req:          &search.SearchRequest{Index: "books", Native: map[string]any{"SORT": []any{"year:asc"}}},
			wantCode:     codes.InvalidArgument,
			wantContains: "must not set",
		},
		{
			name:         "native query with a key in two cases",
			req:          &search.SearchRequest{Index: "books", Native: map[string]any{"filter": "a = 1", "Filter": "b = 2"}},
			wantCode:     codes.InvalidArgument,
			wantContains: "must not set both",
		},
		{
			name:         "search field under the metadata",
			req:          &search.SearchRequest{Index: "books", Text: "acme", SearchFields: []string{"daprMetadata.*"}},
			wantCode:     codes.InvalidArgument,
			wantContains: "cannot search on",
		},
		{
			name:         "search field naming a metadata key",
			req:          &search.SearchRequest{Index: "books", Text: "acme", SearchFields: []string{"title", "daprMetadata.tenant"}},
			wantCode:     codes.InvalidArgument,
			wantContains: "cannot search on",
		},
		{
			name:         "search field with a top-level prefix pattern",
			req:          &search.SearchRequest{Index: "books", Text: "acme", SearchFields: []string{"dapr*"}},
			wantCode:     codes.InvalidArgument,
			wantContains: "cannot search on",
		},
		{
			name:         "search field with a top-level wildcard path",
			req:          &search.SearchRequest{Index: "books", Text: "acme", SearchFields: []string{"*.tenant"}},
			wantCode:     codes.InvalidArgument,
			wantContains: "cannot search on",
		},
		{
			name:         "native query searching the metadata",
			req:          &search.SearchRequest{Index: "books", Native: map[string]any{"q": "acme", "AttributesToSearchOn": []any{"daprMetadata.*"}}},
			wantCode:     codes.InvalidArgument,
			wantContains: "cannot search on",
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

// defaultRankingRules are the ranking rules of a new Meilisearch v1.43 index.
var defaultRankingRules = []string{"words", "typo", "proximity", "attributeRank", "sort", "wordPosition", "exactness"}

// withIndexSettings answers the settings and stats lookups of a search of the
// books index as Meilisearch does for an index with the default settings, and
// passes every other request on to next.
func withIndexSettings(t *testing.T, next http.HandlerFunc) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes/books/settings":
			writeJSON(t, w, http.StatusOK, map[string]any{"searchableAttributes": []string{"*"}, "pagination": map[string]any{"maxTotalHits": 1000}})
		case "/indexes/books/stats":
			writeJSON(t, w, http.StatusOK, map[string]any{"fieldDistribution": map[string]any{"id": 1, "daprMetadata": 1, "title": 1, "year": 1}})
		default:
			next(w, r)
		}
	}
}

// serveIndexLookup answers the lookup IndexDocuments makes before it enqueues
// anything, for an index that exists.
func serveIndexLookup(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	if r.Method != http.MethodGet {
		t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		return
	}
	writeJSON(t, w, http.StatusOK, map[string]any{"uid": "books", "primaryKey": "id"})
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(body))
}
