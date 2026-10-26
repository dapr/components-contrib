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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// newBulkGetStore returns a store that uses a database file, so the store
// has more than one connection and uses WAL.
func newBulkGetStore(t *testing.T) *SQLiteStore {
	t.Helper()

	s := NewSQLiteStateStore(logger.NewLogger("test")).(*SQLiteStore)
	require.NoError(t, s.Init(t.Context(), state.Metadata{
		Base: metadata.Base{Properties: map[string]string{
			"connectionString": filepath.Join(t.TempDir(), "bulkget.db"),
			"tableName":        "test_state",
		}},
	}))
	t.Cleanup(func() { s.Close() })
	return s
}

// setBulkGetValues sets the keys with the same value in one transaction.
func setBulkGetValues(ctx context.Context, s state.TransactionalStore, keys []string, value string) error {
	ops := make([]state.TransactionalStateOperation, len(keys))
	for i, k := range keys {
		ops[i] = state.SetRequest{Key: k, Value: []byte(value)}
	}
	return s.Multi(ctx, &state.TransactionalStateRequest{Operations: ops})
}

func bulkGetRequest(keys []string) []state.GetRequest {
	req := make([]state.GetRequest, len(keys))
	for i, k := range keys {
		req[i] = state.GetRequest{Key: k}
	}
	return req
}

// TestBulkGetChunkBoundaries reads requests with a size around each multiple
// of the chunk size. Each key must have one response with its own value.
func TestBulkGetChunkBoundaries(t *testing.T) {
	s := newBulkGetStore(t)

	const max = bulkGetMaxKeysPerQuery
	keys := make([]string, 3*max)
	ops := make([]state.TransactionalStateOperation, len(keys))
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		ops[i] = state.SetRequest{Key: keys[i], Value: []byte(strconv.Itoa(i))}
	}
	require.NoError(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: ops}))

	for _, n := range []int{1, max - 1, max, max + 1, 2*max - 1, 2 * max, 2*max + 1, 3 * max} {
		t.Run(strconv.Itoa(n)+" keys", func(t *testing.T) {
			res, err := s.BulkGet(t.Context(), bulkGetRequest(keys[:n]), state.BulkGetOpts{})
			require.NoError(t, err)
			require.Len(t, res, n)

			got := make(map[string]string, n)
			for _, r := range res {
				assert.Empty(t, r.Error, r.Key)
				require.NotNil(t, r.ETag, r.Key)
				_, dup := got[r.Key]
				assert.False(t, dup, "key %s has more than one response", r.Key)
				got[r.Key] = string(r.Data)
			}
			for i, k := range keys[:n] {
				assert.Equal(t, strconv.Itoa(i), got[k], k)
			}
		})
	}
}

// failingQuerier fails the query with the number failAt, and counts queries.
type failingQuerier struct {
	querier
	failAt  int
	queries int
}

func (f *failingQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	f.queries++
	if f.queries == f.failAt {
		return nil, errors.New("query failed")
	}
	return f.querier.QueryContext(ctx, query, args...)
}

// TestBulkGetChunksError checks that an error in the second chunk fails the
// request. It must not return the responses of the first chunk, and it must
// not run more queries.
func TestBulkGetChunksError(t *testing.T) {
	s := newBulkGetStore(t)
	dba := s.GetDBAccess()

	keys := make([]string, 3*bulkGetMaxKeysPerQuery)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	require.NoError(t, setBulkGetValues(t.Context(), s, keys, "v"))

	fq := &failingQuerier{querier: dba.db, failAt: 2}
	res, err := dba.bulkGetChunks(t.Context(), fq, keys)
	require.ErrorContains(t, err, "query failed")
	assert.Nil(t, res)
	assert.Equal(t, 2, fq.queries)

	// Without a failure, the same keys have one response each.
	fq = &failingQuerier{querier: dba.db}
	res, err = dba.bulkGetChunks(t.Context(), fq, keys)
	require.NoError(t, err)
	assert.Len(t, res, len(keys))
	assert.Equal(t, 3, fq.queries)
}

// TestBulkGetCanceledContext checks that a request in chunks with a canceled
// context fails, and that it does not keep a connection or a transaction.
func TestBulkGetCanceledContext(t *testing.T) {
	s := newBulkGetStore(t)
	dba := s.GetDBAccess()

	keys := make([]string, 2*bulkGetMaxKeysPerQuery+1)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	require.NoError(t, setBulkGetValues(t.Context(), s, keys, "v"))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := s.BulkGet(ctx, bulkGetRequest(keys), state.BulkGetOpts{})
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, dba.db.Stats().InUse, "connections in use after a failed BulkGet")

	res, err := s.BulkGet(t.Context(), bulkGetRequest(keys), state.BulkGetOpts{})
	require.NoError(t, err)
	assert.Len(t, res, len(keys))
	assert.Zero(t, dba.db.Stats().InUse, "connections in use after a BulkGet")
}

// TestBulkGetSnapshot checks that all chunks of a request read the same
// snapshot. A writer sets all keys to a new value in one transaction, again
// and again. Each response of a reader must have one value for all keys.
func TestBulkGetSnapshot(t *testing.T) {
	s := newBulkGetStore(t)

	keys := make([]string, 3*bulkGetMaxKeysPerQuery+7)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	require.NoError(t, setBulkGetValues(t.Context(), s, keys, "0"))

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	var (
		wg        sync.WaitGroup
		writerErr atomic.Value
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; ctx.Err() == nil; i++ {
			// The writer uses its own context, so a write is not canceled
			// while it runs.
			if err := setBulkGetValues(context.Background(), s, keys, strconv.Itoa(i)); err != nil {
				writerErr.Store(err)
				return
			}
		}
	}()

	req := bulkGetRequest(keys)
	values := make(map[string]struct{})
	for ctx.Err() == nil {
		res, err := s.BulkGet(context.Background(), req, state.BulkGetOpts{})
		require.NoError(t, err)
		require.Len(t, res, len(keys))

		first := string(res[0].Data)
		for _, r := range res {
			require.Equal(t, first, string(r.Data), "key %s has another value than key %s", r.Key, res[0].Key)
		}
		values[first] = struct{}{}
	}
	wg.Wait()

	if err := writerErr.Load(); err != nil {
		require.NoError(t, err.(error))
	}
	assert.Greater(t, len(values), 1, "the writer did not change the keys while the reader ran")
}
