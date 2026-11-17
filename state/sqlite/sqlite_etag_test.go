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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestETagSkipsExpiredRows(t *testing.T) {
	s := NewSQLiteStateStore(logger.NewLogger("test")).(*SQLiteStore)
	require.NoError(t, s.Init(t.Context(), state.Metadata{Base: metadata.Base{Properties: map[string]string{
		"connectionString": ":memory:",
		"tableName":        "etag_expired",
	}}}))
	t.Cleanup(func() { s.Close() })

	setExpired := func(t *testing.T, key string) *string {
		t.Helper()
		require.NoError(t, s.Set(t.Context(), &state.SetRequest{Key: key, Value: "v"}))
		res, err := s.Get(t.Context(), &state.GetRequest{Key: key})
		require.NoError(t, err)
		_, err = s.GetDBAccess().GetConnection().ExecContext(t.Context(),
			"UPDATE etag_expired SET expiration_time = DATETIME(CURRENT_TIMESTAMP, '-10 seconds') WHERE key = ?", key)
		require.NoError(t, err)
		return res.ETag
	}

	requireMismatch := func(t *testing.T, err error) {
		t.Helper()
		var etagErr *state.ETagError
		require.ErrorAs(t, err, &etagErr)
		assert.Equal(t, state.ETagMismatch, etagErr.Kind())
	}

	requireRowExists := func(t *testing.T, key string) {
		t.Helper()
		var n int
		require.NoError(t, s.GetDBAccess().GetConnection().QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM etag_expired WHERE key = ?", key).Scan(&n))
		assert.Equal(t, 1, n)
	}

	t.Run("set", func(t *testing.T) {
		etag := setExpired(t, "s1")
		requireMismatch(t, s.Set(t.Context(), &state.SetRequest{Key: "s1", Value: "x", ETag: etag}))
		requireMismatch(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "other", Value: "x"},
			state.SetRequest{Key: "s1", Value: "x", ETag: etag},
		}}))
		requireRowExists(t, "s1")
	})

	t.Run("delete", func(t *testing.T) {
		etag := setExpired(t, "d1")
		requireMismatch(t, s.Delete(t.Context(), &state.DeleteRequest{Key: "d1", ETag: etag}))
		requireMismatch(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "other", Value: "x"},
			state.DeleteRequest{Key: "d1", ETag: etag},
		}}))
		requireRowExists(t, "d1")
	})
}
