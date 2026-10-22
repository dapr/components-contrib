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

package cosmosdb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const fakeEndpoint = "https://fake.documents.azure.com:443/"

// An upsert with If-Match on a missing item creates it, assumed to match the server.
type fakeCosmos struct {
	lock    sync.Mutex
	items   map[string]string
	nextTag int
	batches int
}

type fakeBatchOp struct {
	OperationType string          `json:"operationType"`
	ID            string          `json:"id"`
	IfMatch       string          `json:"ifMatch"`
	ResourceBody  json.RawMessage `json:"resourceBody"`
}

type fakeBatchResult struct {
	StatusCode int32  `json:"statusCode"`
	ETag       string `json:"eTag,omitempty"`
}

func (f *fakeCosmos) Do(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet {
		return fakeResponse(req, http.StatusOK, map[string]any{
			"writableLocations": []map[string]string{{"name": "local", "databaseAccountEndpoint": fakeEndpoint}},
			"readableLocations": []map[string]string{{"name": "local", "databaseAccountEndpoint": fakeEndpoint}},
		})
	}

	var ops []fakeBatchOp
	if err := json.NewDecoder(req.Body).Decode(&ops); err != nil {
		return nil, err
	}

	f.lock.Lock()
	defer f.lock.Unlock()
	f.batches++

	items := make(map[string]string, len(f.items))
	for k, v := range f.items {
		items[k] = v
	}
	results := make([]fakeBatchResult, len(ops))
	for i, op := range ops {
		id := op.ID
		if id == "" {
			var doc CosmosItem
			if err := json.Unmarshal(op.ResourceBody, &doc); err != nil {
				return nil, err
			}
			id = doc.ID
		}
		cur, exists := items[id]
		code := int32(http.StatusOK)
		switch {
		case op.OperationType != "Upsert" && !exists:
			code = http.StatusNotFound
		case op.IfMatch != "" && exists && op.IfMatch != cur:
			code = http.StatusPreconditionFailed
		}
		if code != http.StatusOK {
			for j := range results {
				results[j] = fakeBatchResult{StatusCode: http.StatusFailedDependency}
			}
			results[i].StatusCode = code
			return fakeResponse(req, http.StatusMultiStatus, results)
		}
		if op.OperationType == "Delete" {
			delete(items, id)
			results[i] = fakeBatchResult{StatusCode: http.StatusNoContent}
			continue
		}
		f.nextTag++
		items[id] = "etag-" + strconv.Itoa(f.nextTag)
		results[i] = fakeBatchResult{StatusCode: code, ETag: items[id]}
	}
	f.items = items
	return fakeResponse(req, http.StatusOK, results)
}

func fakeResponse(req *http.Request, code int, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(b)),
		Request:    req,
	}, nil
}

func newFakeStore(t *testing.T, items map[string]string) (*StateStore, *fakeCosmos) {
	t.Helper()
	fake := &fakeCosmos{items: items, nextTag: 100}
	cred, err := azcosmos.NewKeyCredential(base64.StdEncoding.EncodeToString([]byte("key")))
	require.NoError(t, err)
	client, err := azcosmos.NewClientWithKey(fakeEndpoint, cred, &azcosmos.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: fake,
			Retry:     policy.RetryOptions{MaxRetries: -1},
		},
	})
	require.NoError(t, err)
	container, err := client.NewContainer("db", "coll")
	require.NoError(t, err)
	return &StateStore{client: container, logger: logger.NewLogger("test")}, fake
}

func TestMulti(t *testing.T) {
	t.Run("stale etag is an etag mismatch", func(t *testing.T) {
		s, fake := newFakeStore(t, map[string]string{"a": "etag-1", "b": "etag-2"})
		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "v"},
			state.SetRequest{Key: "b", Value: "v", ETag: ptr.Of("stale")},
		}})
		var etagErr *state.ETagError
		require.ErrorAs(t, err, &etagErr)
		assert.Equal(t, state.ETagMismatch, etagErr.Kind())
		assert.Equal(t, map[string]string{"a": "etag-1", "b": "etag-2"}, fake.items)
	})

	t.Run("etag delete of a missing item is an etag mismatch", func(t *testing.T) {
		s, fake := newFakeStore(t, map[string]string{})
		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "v"},
			state.DeleteRequest{Key: "b", ETag: ptr.Of("etag-1")},
		}})
		var etagErr *state.ETagError
		require.ErrorAs(t, err, &etagErr)
		assert.Equal(t, state.ETagMismatch, etagErr.Kind())
		assert.Empty(t, fake.items)
	})

	t.Run("delete of a missing item keeps the other operations", func(t *testing.T) {
		s, fake := newFakeStore(t, map[string]string{"c": "etag-1"})
		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "v"},
			state.DeleteRequest{Key: "missing"},
			state.DeleteRequest{Key: "c"},
		}})
		require.NoError(t, err)
		assert.Contains(t, fake.items, "a")
		assert.NotContains(t, fake.items, "c")
		assert.Equal(t, 2, fake.batches)
	})

	t.Run("only deletes of missing items is a no-op", func(t *testing.T) {
		s, fake := newFakeStore(t, map[string]string{})
		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{Key: "x"},
			state.DeleteRequest{Key: "y"},
		}})
		require.NoError(t, err)
		assert.Empty(t, fake.items)
		assert.Equal(t, 2, fake.batches)
	})

	t.Run("repeated keys keep order across retries", func(t *testing.T) {
		s, fake := newFakeStore(t, map[string]string{})
		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{Key: "a"},
			state.SetRequest{Key: "a", Value: "v"},
			state.DeleteRequest{Key: "a"},
			state.DeleteRequest{Key: "a"},
			state.SetRequest{Key: "a", Value: "v"},
		}})
		require.NoError(t, err)
		assert.Contains(t, fake.items, "a")
		assert.Equal(t, 3, fake.batches)
	})

	t.Run("etag upsert of a missing item does not create it", func(t *testing.T) {
		s, fake := newFakeStore(t, map[string]string{})
		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("etag-1")},
		}})
		var etagErr *state.ETagError
		require.ErrorAs(t, err, &etagErr)
		assert.Equal(t, state.ETagMismatch, etagErr.Kind())
		assert.Empty(t, fake.items)
	})

	t.Run("matching etags apply", func(t *testing.T) {
		s, fake := newFakeStore(t, map[string]string{"a": "etag-1", "b": "etag-2"})
		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("etag-1")},
			state.DeleteRequest{Key: "b", ETag: ptr.Of("etag-2")},
		}})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"a": "etag-101"}, fake.items)
	})
}

func TestBatchResultError(t *testing.T) {
	ops := []batchOp{
		{kind: batchUpsert, id: "a"},
		{kind: batchReplace, id: "b", guarded: true},
		{kind: batchDelete, id: "c"},
		{kind: batchDelete, id: "d", guarded: true},
	}
	results := func(failed int, code int32) []azcosmos.TransactionalBatchResult {
		res := make([]azcosmos.TransactionalBatchResult, len(ops))
		for i := range res {
			res[i].StatusCode = http.StatusFailedDependency
		}
		res[failed].StatusCode = code
		return res
	}

	for name, tc := range map[string]struct {
		failed int
		code   int32
	}{
		"stale etag on replace":   {1, http.StatusPreconditionFailed},
		"etag on missing item":    {1, http.StatusNotFound},
		"stale etag on delete":    {3, http.StatusPreconditionFailed},
		"etag delete of missing":  {3, http.StatusNotFound},
		"first write on existing": {0, http.StatusPreconditionFailed},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := batchResultError(ops, results(tc.failed, tc.code))
			var etagErr *state.ETagError
			require.ErrorAs(t, err, &etagErr)
			assert.Equal(t, state.ETagMismatch, etagErr.Kind())
		})
	}

	t.Run("unguarded delete of a missing item is dropped", func(t *testing.T) {
		drop, err := batchResultError(ops, results(2, http.StatusNotFound))
		require.NoError(t, err)
		assert.Equal(t, 2, drop)
	})

	t.Run("other failures stay untyped", func(t *testing.T) {
		_, err := batchResultError(ops, results(0, http.StatusTooManyRequests))
		require.Error(t, err)
		var etagErr *state.ETagError
		require.NotErrorAs(t, err, &etagErr)
	})

	t.Run("no failed operation is an error", func(t *testing.T) {
		_, err := batchResultError(ops, results(0, http.StatusFailedDependency))
		require.Error(t, err)
	})
}
