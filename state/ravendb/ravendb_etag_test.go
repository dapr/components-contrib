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

package ravendb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	ravendb "github.com/ravendb/ravendb-go-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type fakeRavenServer struct {
	lock     sync.Mutex
	commands [][]map[string]any
	conflict bool
}

func (f *fakeRavenServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/bulk_docs") {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var body struct {
		Commands []map[string]any `json:"Commands"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	f.lock.Lock()
	f.commands = append(f.commands, body.Commands)
	conflict := f.conflict
	f.lock.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if conflict {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"Type":    "Raven.Client.Exceptions.ConcurrencyException",
			"Message": "Optimistic concurrency violation, transaction will be aborted.",
		})
		return
	}

	results := make([]map[string]any, len(body.Commands))
	for i, c := range body.Commands {
		results[i] = map[string]any{"Type": c["Type"], "@id": c["Id"]}
		if c["Type"] == "PUT" {
			results[i]["@change-vector"] = "A:" + c["Id"].(string)
		}
	}
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"Results": results})
}

func (f *fakeRavenServer) commandsByID(t *testing.T) map[string]map[string]any {
	t.Helper()
	f.lock.Lock()
	defer f.lock.Unlock()
	require.Len(t, f.commands, 1)
	byID := map[string]map[string]any{}
	for _, c := range f.commands[0] {
		byID[c["Id"].(string)] = c
	}
	return byID
}

func newFakeRaven(t *testing.T) (*RavenDB, *fakeRavenServer) {
	t.Helper()
	fake := &fakeRavenServer{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	store := ravendb.NewDocumentStore([]string{srv.URL}, "testdb")
	store.GetConventions().SetDisableTopologyUpdates(true)
	require.NoError(t, store.Initialize())
	t.Cleanup(store.Close)

	r := NewRavenDB(logger.NewLogger("test")).(*RavenDB)
	r.documentStore = store
	r.metadata.DatabaseName = "testdb"
	return r, fake
}

func TestSetETag(t *testing.T) {
	t.Run("first-write etag on a missing document is sent as a change vector", func(t *testing.T) {
		r, fake := newFakeRaven(t)
		err := r.Set(t.Context(), &state.SetRequest{
			Key: "a", Value: "v", ETag: ptr.Of("A:1-x"),
			Options: state.SetStateOption{Concurrency: state.FirstWrite},
		})
		require.NoError(t, err)

		cmd := fake.commandsByID(t)["a"]
		assert.Equal(t, "PUT", cmd["Type"])
		assert.Equal(t, "A:1-x", cmd["ChangeVector"])
	})
}

func TestMultiETags(t *testing.T) {
	t.Run("etags are sent as change vectors", func(t *testing.T) {
		r, fake := newFakeRaven(t)
		err := r.Multi(t.Context(), &state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("A:1-x"), Options: state.SetStateOption{Concurrency: state.FirstWrite}},
				state.SetRequest{Key: "b", Value: "v", ETag: ptr.Of("A:2-x")},
				state.DeleteRequest{Key: "c", ETag: ptr.Of("A:3-x")},
			},
		})
		require.NoError(t, err)

		byID := fake.commandsByID(t)
		require.Len(t, byID, 3)
		assert.Equal(t, "PUT", byID["a"]["Type"])
		assert.Equal(t, "A:1-x", byID["a"]["ChangeVector"])
		assert.Equal(t, "PUT", byID["b"]["Type"])
		assert.Equal(t, "A:2-x", byID["b"]["ChangeVector"])
		assert.Equal(t, "DELETE", byID["c"]["Type"])
		assert.Equal(t, "A:3-x", byID["c"]["ChangeVector"])
	})

	t.Run("delete etag conflict is an etag mismatch", func(t *testing.T) {
		r, fake := newFakeRaven(t)
		fake.conflict = true
		err := r.Multi(t.Context(), &state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				state.DeleteRequest{Key: "b", ETag: ptr.Of("A:3-x")},
			},
		})
		var etagErr *state.ETagError
		require.ErrorAs(t, err, &etagErr)
		assert.Equal(t, state.ETagMismatch, etagErr.Kind())
	})

	t.Run("etag on a key already used in the transaction is a mismatch", func(t *testing.T) {
		for name, ops := range map[string][]state.TransactionalStateOperation{
			"set then set": {
				state.SetRequest{Key: "a", Value: "v"},
				state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("A:1-x")},
			},
			"set then first-write set": {
				state.SetRequest{Key: "a", Value: "v"},
				state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("A:1-x"), Options: state.SetStateOption{Concurrency: state.FirstWrite}},
			},
			"set then delete": {
				state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("A:1-x")},
				state.DeleteRequest{Key: "a", ETag: ptr.Of("A:1-x")},
			},
			"delete then set": {
				state.DeleteRequest{Key: "a"},
				state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("A:1-x")},
			},
		} {
			t.Run(name, func(t *testing.T) {
				r, fake := newFakeRaven(t)
				err := r.Multi(t.Context(), &state.TransactionalStateRequest{Operations: ops})
				var etagErr *state.ETagError
				require.ErrorAs(t, err, &etagErr)
				assert.Equal(t, state.ETagMismatch, etagErr.Kind())
				assert.Empty(t, fake.commands)
			})
		}
	})
}
