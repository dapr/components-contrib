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

package mysql

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/ptr"
)

func requireETagMismatch(t *testing.T, err error) {
	t.Helper()
	var etagErr *state.ETagError
	require.ErrorAs(t, err, &etagErr)
	assert.Equal(t, state.ETagMismatch, etagErr.Kind())
}

func TestETagSkipsExpiredRows(t *testing.T) {
	const updateSQL = `UPDATE state\s+SET .+WHERE id = \?\s+AND eTag = \?\s+AND \(expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP\)`
	const deleteSQL = `DELETE FROM state WHERE id = \? AND eTag = \?\s+AND \(expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP\)`

	t.Run("set", func(t *testing.T) {
		m, _ := mockDatabase(t)
		defer m.mySQL.Close()

		m.mock1.ExpectExec(updateSQL).WillReturnResult(sqlmock.NewResult(0, 0))

		err := m.mySQL.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("e")})
		requireETagMismatch(t, err)
		require.NoError(t, m.mock1.ExpectationsWereMet())
	})

	t.Run("delete", func(t *testing.T) {
		m, _ := mockDatabase(t)
		defer m.mySQL.Close()

		m.mock1.ExpectExec(deleteSQL).WithArgs("a", "e").WillReturnResult(sqlmock.NewResult(0, 0))

		err := m.mySQL.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of("e")})
		requireETagMismatch(t, err)
		require.NoError(t, m.mock1.ExpectationsWereMet())
	})

	t.Run("multi", func(t *testing.T) {
		m, _ := mockDatabase(t)
		defer m.mySQL.Close()

		m.mock1.ExpectBegin()
		m.mock1.ExpectExec(updateSQL).WillReturnResult(sqlmock.NewResult(0, 1))
		m.mock1.ExpectExec(deleteSQL).WithArgs("b", "e").WillReturnResult(sqlmock.NewResult(0, 0))
		m.mock1.ExpectRollback()

		err := m.mySQL.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("e")},
			state.DeleteRequest{Key: "b", ETag: ptr.Of("e")},
		}})
		requireETagMismatch(t, err)
		require.NoError(t, m.mock1.ExpectationsWereMet())
	})
}
