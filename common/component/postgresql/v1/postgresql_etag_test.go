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

	pgxmock "github.com/pashagolub/pgxmock/v2"
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

func TestETagDeleteSkipsExpiredRows(t *testing.T) {
	const deleteSQL = `DELETE FROM state WHERE key = \$1 AND \$2 = xmin AND \(expiredate IS NULL OR expiredate >= CURRENT_TIMESTAMP\)`

	t.Run("delete", func(t *testing.T) {
		m, _ := mockDatabase(t)
		m.pg.etagColumn = "xmin"
		defer m.db.Close()

		m.db.ExpectExec(deleteSQL).WithArgs("a", uint32(5)).WillReturnResult(pgxmock.NewResult("DELETE", 0))

		err := m.pg.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of("5")})
		requireETagMismatch(t, err)
		require.NoError(t, m.db.ExpectationsWereMet())
	})

	t.Run("multi", func(t *testing.T) {
		m, _ := mockDatabase(t)
		m.pg.etagColumn = "xmin"
		defer m.db.Close()

		m.db.ExpectBegin()
		m.db.ExpectExec("INSERT INTO state").WithArgs("a", `"a"`, false).WillReturnResult(pgxmock.NewResult("INSERT", 1))
		m.db.ExpectExec(deleteSQL).WithArgs("b", uint32(5)).WillReturnResult(pgxmock.NewResult("DELETE", 0))
		m.db.ExpectRollback()

		err := m.pg.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "a"},
			state.DeleteRequest{Key: "b", ETag: ptr.Of("5")},
		}})
		requireETagMismatch(t, err)
		require.NoError(t, m.db.ExpectationsWereMet())
	})
}
