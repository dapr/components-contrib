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

package oracledatabase

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func newMockStore(t *testing.T) (*OracleDatabase, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	dba := &oracleDatabaseAccess{
		logger:   logger.NewLogger("test"),
		db:       db,
		metadata: oracleDatabaseMetadata{TableName: "state", BulkGetChunkSize: defaultBulkGetChunkSize},
	}
	return newOracleDatabaseStateStore(logger.NewLogger("test"), dba), mock
}

func requireETagMismatch(t *testing.T, err error) {
	t.Helper()
	var etagErr *state.ETagError
	require.ErrorAs(t, err, &etagErr)
	assert.Equal(t, state.ETagMismatch, etagErr.Kind())
}

func TestETagSkipsExpiredRows(t *testing.T) {
	const updateSQL = `UPDATE state SET .+WHERE key = :key\s+AND etag = :etag\s+AND \(expiration_time IS NULL OR expiration_time > systimestamp\)`
	const deleteSQL = `DELETE FROM state WHERE key = :key AND etag = :etag AND \(expiration_time IS NULL OR expiration_time > systimestamp\)`

	t.Run("set", func(t *testing.T) {
		s, mock := newMockStore(t)
		mock.ExpectExec(updateSQL).WillReturnResult(sqlmock.NewResult(0, 0))

		err := s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("e")})
		requireETagMismatch(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("delete", func(t *testing.T) {
		s, mock := newMockStore(t)
		mock.ExpectExec(deleteSQL).WithArgs("a", "e").WillReturnResult(sqlmock.NewResult(0, 0))

		err := s.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of("e")})
		requireETagMismatch(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("multi", func(t *testing.T) {
		s, mock := newMockStore(t)
		mock.ExpectBegin()
		mock.ExpectExec(updateSQL).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(deleteSQL).WithArgs("b", "e").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("e")},
			state.DeleteRequest{Key: "b", ETag: ptr.Of("e")},
		}})
		requireETagMismatch(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
