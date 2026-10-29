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

package sqlserver

import (
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqlserverAuth "github.com/dapr/components-contrib/common/authentication/sqlserver"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func newETagTestStore(t *testing.T) (*SQLServer, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	s := &SQLServer{
		logger: logger.NewLogger("test"),
		db:     db,
	}
	s.metadata = sqlServerMetadata{
		SQLServerAuthMetadata: sqlserverAuth.SQLServerAuthMetadata{SchemaName: "dbo"},
		TableName:             "state",
	}
	mr := (&migration{metadata: &s.metadata}).newMigrationResult()
	s.upsertCommand = mr.upsertProcFullName
	s.deleteWithETagCommand = mr.deleteWithETagCommand
	s.deleteWithoutETagCommand = mr.deleteWithoutETagCommand
	return s, mock
}

func requireETagErr(t *testing.T, err error, kind state.ETagErrorKind) {
	t.Helper()
	var etagErr *state.ETagError
	require.ErrorAs(t, err, &etagErr)
	assert.Equal(t, kind, etagErr.Kind())
}

const etagUpdateQuery = `UPDATE \[dbo\]\.\[state\].*CAST\(@TTL AS INT\).*WHERE \[Key\]=@Key AND \[RowVersion\]=@RowVersion\s+AND \(\[ExpireDate\] IS NULL OR \[ExpireDate\] > GETDATE\(\)\)`

const firstWriteQuery = `SET NOCOUNT ON\s+DELETE FROM \[dbo\]\.\[state\] WHERE \[Key\]=@Key AND \[ExpireDate\] <= GETDATE\(\)\s+SET NOCOUNT OFF\s+INSERT INTO \[dbo\]\.\[state\].*CAST\(@TTL AS INT\).*WHERE NOT EXISTS \(SELECT 1 FROM \[dbo\]\.\[state\] WHERE \[Key\]=@Key\)`

func TestFirstWriteConflict(t *testing.T) {
	firstWrite := state.SetStateOption{Concurrency: state.FirstWrite}

	t.Run("set inserts over an expired row", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectExec(firstWriteQuery).WillReturnResult(sqlmock.NewResult(0, 1))

		err := s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", Options: firstWrite, Metadata: map[string]string{"ttlInSeconds": "10"}})
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("existing key", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectExec(firstWriteQuery).WillReturnResult(sqlmock.NewResult(0, 0))

		err := s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", Options: firstWrite})
		requireETagErr(t, err, state.ETagMismatch)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("multi rolls back", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectBegin()
		mock.ExpectExec(`sp_Upsert_v5_state`).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(firstWriteQuery).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "a"},
			state.SetRequest{Key: "b", Value: "b", Options: firstWrite},
		}})
		requireETagErr(t, err, state.ETagMismatch)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	for _, num := range []int32{2627, 2601} {
		t.Run(fmt.Sprintf("concurrent insert %d", num), func(t *testing.T) {
			s, mock := newETagTestStore(t)
			mock.ExpectExec(firstWriteQuery).WillReturnError(mssql.Error{Number: num})

			err := s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", Options: firstWrite})
			requireETagErr(t, err, state.ETagMismatch)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("other errors are not mapped", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectExec(firstWriteQuery).WillReturnError(mssql.Error{Number: 1205})

		err := s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", Options: firstWrite})
		require.Error(t, err)
		var etagErr *state.ETagError
		require.NotErrorAs(t, err, &etagErr)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestETagWriteSkipsExpiredRows(t *testing.T) {
	for name, opts := range map[string]state.SetStateOption{
		"last-write":  {},
		"first-write": {Concurrency: state.FirstWrite},
	} {
		t.Run(name, func(t *testing.T) {
			s, mock := newETagTestStore(t)
			mock.ExpectExec(etagUpdateQuery).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(etagUpdateQuery).WillReturnResult(sqlmock.NewResult(0, 0))

			req := &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("00000000000007d1"), Options: opts, Metadata: map[string]string{"ttlInSeconds": "10"}}
			require.NoError(t, s.Set(t.Context(), req))
			requireETagErr(t, s.Set(t.Context(), req), state.ETagMismatch)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("multi rolls back", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectBegin()
		mock.ExpectExec(`sp_Upsert_v5_state`).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(etagUpdateQuery).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "a"},
			state.SetRequest{Key: "b", Value: "b", ETag: ptr.Of("00000000000007d1")},
		}})
		requireETagErr(t, err, state.ETagMismatch)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestETagDeleteSkipsExpiredRows(t *testing.T) {
	s, mock := newETagTestStore(t)
	mock.ExpectExec(`DELETE \[dbo\]\.\[state\] WHERE \[Key\]=@Key AND \[RowVersion\]=@RowVersion AND \(\[ExpireDate\] IS NULL OR \[ExpireDate\] > GETDATE\(\)\)`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := s.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of("00000000000007d1")})
	requireETagErr(t, err, state.ETagMismatch)
	require.NoError(t, mock.ExpectationsWereMet())
}

type nilVarBinaryArg struct{}

// go-mssqldb declares a typed nil []byte as varbinary, an untyped nil as nvarchar(1).
func (nilVarBinaryArg) Match(v driver.Value) bool {
	b, ok := v.([]byte)
	return ok && b == nil
}

func TestBinaryDataParamType(t *testing.T) {
	anyArg := sqlmock.AnyArg()

	t.Run("etag update", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectExec(etagUpdateQuery).
			WithArgs(anyArg, anyArg, nilVarBinaryArg{}, anyArg, anyArg, anyArg).
			WillReturnResult(sqlmock.NewResult(0, 1))
		require.NoError(t, s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("00000000000007d1")}))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("upsert procedure", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectExec(`sp_Upsert_v5_state`).
			WithArgs(anyArg, anyArg, anyArg, nilVarBinaryArg{}, anyArg, anyArg, anyArg).
			WillReturnResult(sqlmock.NewResult(0, 1))
		require.NoError(t, s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a"}))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("binary value", func(t *testing.T) {
		s, mock := newETagTestStore(t)
		mock.ExpectExec(etagUpdateQuery).
			WithArgs(anyArg, nil, []byte("a"), true, anyArg, anyArg).
			WillReturnResult(sqlmock.NewResult(0, 1))
		require.NoError(t, s.Set(t.Context(), &state.SetRequest{Key: "a", Value: []byte("a"), ETag: ptr.Of("00000000000007d1")}))
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
