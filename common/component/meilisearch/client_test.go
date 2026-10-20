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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListIndexUIDsReadsEveryPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		total     int
		wantPages int32
	}{
		{name: "no indexes", total: 0, wantPages: 1},
		{name: "a single page", total: 3, wantPages: 1},
		{name: "exactly one full page", total: indexPageSize, wantPages: 1},
		{name: "several pages", total: 2*indexPageSize + 5, wantPages: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var pages atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/indexes" {
					http.NotFound(w, r)
					return
				}
				pages.Add(1)
				// Meilisearch serves 20 indexes per page unless asked otherwise.
				limit, offset := 20, 0
				if raw := r.URL.Query().Get("limit"); raw != "" {
					limit, _ = strconv.Atoi(raw)
				}
				if raw := r.URL.Query().Get("offset"); raw != "" {
					offset, _ = strconv.Atoi(raw)
				}
				results := []map[string]any{}
				for i := offset; i < min(offset+limit, tt.total); i++ {
					results = append(results, map[string]any{"uid": fmt.Sprintf("index-%03d", i), "primaryKey": "id"})
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"results": results, "offset": offset, "limit": limit, "total": tt.total})
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(MeilisearchMetadata{Host: server.URL})
			require.NoError(t, err)

			uids, err := ListIndexUIDs(t.Context(), client)

			require.NoError(t, err)
			require.Len(t, uids, tt.total)
			for i, uid := range uids {
				assert.Equal(t, fmt.Sprintf("index-%03d", i), uid)
			}
			assert.Equal(t, tt.wantPages, pages.Load())
		})
	}
}

func TestReconcileIndexCreationRollsBackOnlyWhatTheTaskCreated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		task        map[string]any
		wantDeleted int32
	}{
		{
			name:        "the task created the index",
			task:        map[string]any{"uid": 1, "status": "succeeded", "type": "indexCreation", "indexUid": "books"},
			wantDeleted: 1,
		},
		{
			name: "the index already existed",
			task: map[string]any{
				"uid": 1, "status": "failed", "type": "indexCreation", "indexUid": "books",
				"error": map[string]any{"message": "Index `books` already exists.", "code": "index_already_exists", "type": "invalid_request"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var deleted atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/tasks/1":
					_ = json.NewEncoder(w).Encode(tt.task)
				case r.Method == http.MethodDelete && r.URL.Path == "/indexes/books":
					deleted.Add(1)
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(map[string]any{"taskUid": 2, "status": "enqueued", "type": "indexDeletion"})
				case r.URL.Path == "/tasks/2":
					_ = json.NewEncoder(w).Encode(map[string]any{"uid": 2, "status": "succeeded", "type": "indexDeletion"})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				}
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(MeilisearchMetadata{Host: server.URL})
			require.NoError(t, err)

			// The request has already ended: the reconciliation must not depend on it.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			ReconcileIndexCreation(ctx, client, 1, "books", nil)

			assert.Equal(t, tt.wantDeleted, deleted.Load())
		})
	}
}
