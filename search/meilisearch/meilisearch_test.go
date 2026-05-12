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
	"testing"

	"github.com/stretchr/testify/require"

	contribmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	kitlogger "github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	t.Parallel()

	t.Run("happy path with api key", func(t *testing.T) {
		t.Parallel()
		seenAuth := ""
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seenAuth = r.Header.Get("Authorization")
			require.Equal(t, "/indexes", r.URL.Path)
			writeJSON(t, w, http.StatusOK, map[string]any{"results": []any{}, "limit": 20})
		}))
		defer server.Close()

		component := NewMeilisearch(kitlogger.NewLogger("test"))
		err := component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": server.URL, "apiKey": "secret"}}})
		require.NoError(t, err)
		_, err = component.ListIndexes(t.Context(), &search.ListIndexesRequest{})
		require.NoError(t, err)
		require.Equal(t, "Bearer secret", seenAuth)
	})

	t.Run("missing host", func(t *testing.T) {
		t.Parallel()
		component := NewMeilisearch(kitlogger.NewLogger("test"))
		err := component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{}}})
		require.ErrorContains(t, err, "host is required")
	})
}

func TestIndexDocuments(t *testing.T) {
	t.Parallel()

	var waited bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes/cars/documents":
			require.Equal(t, http.MethodPost, r.Method)
			var docs []map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&docs))
			require.Equal(t, "1", docs[0]["id"])
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 9, "status": "enqueued", "type": "documentAdditionOrUpdate"})
		case "/tasks/9":
			waited = true
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": 9, "status": "succeeded", "type": "documentAdditionOrUpdate"})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	res, err := component.IndexDocuments(t.Context(), &search.IndexDocumentsRequest{Index: "cars", Ack: search.IndexAckDurable, Documents: []search.Document{{ID: "1", Content: map[string]any{"title": "Hyundai Sonata"}}}})
	require.NoError(t, err)
	require.True(t, waited)
	require.Equal(t, search.IndexAckDurable, res.Ack)
	require.Equal(t, []search.OperationResult{{ID: "1", Success: true}}, res.Results)
}

func TestSearch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/indexes/cars/search", r.URL.Path)
		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "hyundai", req["q"])
		require.Equal(t, []any{"title"}, req["attributesToHighlight"])
		writeJSON(t, w, http.StatusOK, map[string]any{
			"hits":               []map[string]any{{"id": "1", "title": "Hyundai Sonata", "_rankingScore": 0.87, "_formatted": map[string]any{"title": "<em>Hyundai</em> Sonata"}}},
			"estimatedTotalHits": 1,
			"offset":             0,
			"limit":              10,
		})
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	res, err := component.Search(t.Context(), &search.SearchRequest{Index: "cars", Text: "hyundai", IncludeContent: true, HighlightFields: []string{"title"}, TopK: 10})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1)
	require.Equal(t, "1", res.Hits[0].Document.ID)
	require.Equal(t, "Hyundai Sonata", res.Hits[0].Document.Content["title"])
	require.Equal(t, 0.87, res.Hits[0].Score)
	require.Equal(t, "<em>Hyundai</em> Sonata", res.Hits[0].Highlights["title"].Snippets[0].Text)
}

func TestIndexLifecycleAndDelete(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/indexes":
			if r.Method == http.MethodPost {
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 1, "status": "enqueued", "type": "indexCreation"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"results": []map[string]any{{"uid": "cars", "primaryKey": "id"}}, "limit": 20, "total": 1})
		case "/indexes/cars/settings":
			if r.Method == http.MethodPatch {
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 2, "status": "enqueued", "type": "settingsUpdate"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"filterableAttributes": []string{"make"}, "sortableAttributes": []string{"year"}, "searchableAttributes": []string{"title"}})
		case "/indexes/cars":
			if r.Method == http.MethodDelete {
				writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 3, "status": "enqueued", "type": "indexDeletion"})
				return
			}
			writeJSON(t, w, http.StatusOK, map[string]any{"uid": "cars", "primaryKey": "id"})
		case "/indexes/cars/stats":
			writeJSON(t, w, http.StatusOK, map[string]any{"numberOfDocuments": 2})
		case "/indexes/cars/documents/delete-batch":
			writeJSON(t, w, http.StatusAccepted, map[string]any{"taskUid": 4, "status": "enqueued", "type": "documentDeletion"})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	component := initializedSearch(t, server.URL)
	_, err := component.CreateIndex(t.Context(), &search.CreateIndexRequest{Index: "cars", Fields: []search.IndexFieldSchema{{Name: "title", Searchable: true}, {Name: "make", Filterable: true}, {Name: "year", Sortable: true}}})
	require.NoError(t, err)
	desc, err := component.DescribeIndex(t.Context(), &search.DescribeIndexRequest{Index: "cars"})
	require.NoError(t, err)
	require.Equal(t, uint64(2), desc.DocumentCount)
	indexes, err := component.ListIndexes(t.Context(), &search.ListIndexesRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"cars"}, indexes.Indexes)
	del, err := component.DeleteDocuments(t.Context(), &search.DeleteDocumentsRequest{Index: "cars", IDs: []string{"1"}})
	require.NoError(t, err)
	require.True(t, del.Results[0].Success)
	require.NoError(t, component.DropIndex(t.Context(), &search.DropIndexRequest{Index: "cars"}))
}

func initializedSearch(t *testing.T, host string) search.Search {
	t.Helper()
	component := NewMeilisearch(kitlogger.NewLogger("test"))
	err := component.Init(t.Context(), search.Metadata{Base: contribmetadata.Base{Properties: map[string]string{"host": host}}})
	require.NoError(t, err)
	return component
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(body))
}
