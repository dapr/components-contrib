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

package postgresql

import (
	"testing"
	"time"

	pgxmock "github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const testETag = "e9b9e142-74b1-4a2e-8e90-3f4ffeea2e70"

func newETagMock(t *testing.T) (*PostgreSQL, pgxmock.PgxPoolIface) {
	t.Helper()
	db, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return &PostgreSQL{
		metadata: pgMetadata{Timeout: 30 * time.Second},
		logger:   logger.NewLogger("test"),
		db:       db,
	}, db
}

func requireETagMismatch(t *testing.T, err error) {
	t.Helper()
	var etagErr *state.ETagError
	require.ErrorAs(t, err, &etagErr)
	assert.Equal(t, state.ETagMismatch, etagErr.Kind())
}

func TestETagSkipsExpiredRows(t *testing.T) {
	const updateSQL = `UPDATE state\s+SET .+WHERE\s+key = \$1\s+AND etag = \$3\s+AND \(expires_at IS NULL OR expires_at >= now\(\)\)`
	const deleteSQL = `DELETE FROM state WHERE key = \$1 AND etag = \$2 AND \(expires_at IS NULL OR expires_at >= now\(\)\)`

	t.Run("set", func(t *testing.T) {
		p, db := newETagMock(t)
		db.ExpectExec(updateSQL).
			WithArgs("a", []byte(`"a"`), testETag).
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))

		err := p.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of(testETag)})
		requireETagMismatch(t, err)
		require.NoError(t, db.ExpectationsWereMet())
	})

	t.Run("delete", func(t *testing.T) {
		p, db := newETagMock(t)
		db.ExpectExec(deleteSQL).
			WithArgs("a", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("DELETE", 0))

		err := p.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of(testETag)})
		requireETagMismatch(t, err)
		require.NoError(t, db.ExpectationsWereMet())
	})

	t.Run("multi", func(t *testing.T) {
		p, db := newETagMock(t)
		db.ExpectBegin()
		db.ExpectExec(updateSQL).
			WithArgs("a", []byte(`"a"`), testETag).
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		db.ExpectExec(deleteSQL).
			WithArgs("b", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("DELETE", 0))
		db.ExpectRollback()

		err := p.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of(testETag)},
			state.DeleteRequest{Key: "b", ETag: ptr.Of(testETag)},
		}})
		requireETagMismatch(t, err)
		require.NoError(t, db.ExpectationsWereMet())
	})
}
