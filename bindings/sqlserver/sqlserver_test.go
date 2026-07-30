/*
Copyright 2024 The Dapr Authors
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
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	t.Run("missing connection string", func(t *testing.T) {
		b := NewSQLServer(logger.NewLogger("test"))
		md := bindings.Metadata{}
		md.Properties = map[string]string{}
		err := b.Init(t.Context(), md)
		require.Error(t, err)
	})

	t.Run("cannot re-init a closed component", func(t *testing.T) {
		m, _, _ := mockDatabase(t)
		require.NoError(t, m.Close())

		md := bindings.Metadata{}
		md.Properties = map[string]string{
			"connectionString": "sqlserver://sa:password@localhost:1433?database=master",
		}
		err := m.Init(t.Context(), md)
		require.Error(t, err)
	})
}

func TestQuery(t *testing.T) {
	m, mock, _ := mockDatabase(t)
	defer m.Close()

	t.Run("query returns rows as JSON", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "value", "timestamp"}).
			AddRow(1, "value-1", time.Now()).
			AddRow(2, "value-2", time.Now().Add(1000)).
			AddRow(3, "value-3", time.Now().Add(2000))

		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < 4").WillReturnRows(rows)
		ret, err := m.query(t.Context(), `SELECT * FROM foo WHERE id < 4`)
		require.NoError(t, err)
		assert.Contains(t, string(ret), "\"id\":1")

		var result []any
		err = json.Unmarshal(ret, &result)
		require.NoError(t, err)
		assert.Len(t, result, 3)
	})

	t.Run("query with typed columns", func(t *testing.T) {
		col1 := sqlmock.NewColumn("id").OfType("BIGINT", 1)
		col2 := sqlmock.NewColumn("value").OfType("FLOAT", 1.0)
		rows := sqlmock.NewRowsWithColumnDefinition(col1, col2).
			AddRow(1, 1.1).
			AddRow(2, 2.2)
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < 4").WillReturnRows(rows)
		ret, err := m.query(t.Context(), "SELECT * FROM foo WHERE id < 4")
		require.NoError(t, err)
		assert.Contains(t, string(ret), "\"value\":2.2")
	})

	t.Run("query fails", func(t *testing.T) {
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < 4").WillReturnError(errors.New("query failed"))
		_, err := m.query(t.Context(), `SELECT * FROM foo WHERE id < 4`)
		require.Error(t, err)
	})
}

func TestExec(t *testing.T) {
	m, mock, _ := mockDatabase(t)
	defer m.Close()

	mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnResult(sqlmock.NewResult(1, 1))
	i, err := m.exec(t.Context(), "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')")
	assert.Equal(t, int64(1), i)
	require.NoError(t, err)
}

func TestInvoke(t *testing.T) {
	m, mock, _ := mockDatabase(t)
	defer m.Close()

	t.Run("exec operation succeeds", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnResult(sqlmock.NewResult(1, 1))
		metadata := map[string]string{commandSQLKey: "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')"}
		req := &bindings.InvokeRequest{
			Metadata:  metadata,
			Operation: execOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		require.NoError(t, err)
		assert.Equal(t, "1", resp.Metadata[respRowsAffectedKey])
	})

	t.Run("exec operation fails", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnError(errors.New("insert failed"))
		metadata := map[string]string{commandSQLKey: "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')"}
		req := &bindings.InvokeRequest{
			Metadata:  metadata,
			Operation: execOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("query operation succeeds", func(t *testing.T) {
		col1 := sqlmock.NewColumn("id").OfType("BIGINT", 1)
		rows := sqlmock.NewRowsWithColumnDefinition(col1).AddRow(1)
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < \\d+").WillReturnRows(rows)

		metadata := map[string]string{commandSQLKey: "SELECT * FROM foo WHERE id < 2"}
		req := &bindings.InvokeRequest{
			Metadata:  metadata,
			Operation: queryOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		require.NoError(t, err)
		var data []any
		err = json.Unmarshal(resp.Data, &data)
		require.NoError(t, err)
		assert.Len(t, data, 1)
	})

	t.Run("query operation fails", func(t *testing.T) {
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < \\d+").WillReturnError(errors.New("query failed"))
		metadata := map[string]string{commandSQLKey: "SELECT * FROM foo WHERE id < 2"}
		req := &bindings.InvokeRequest{
			Metadata:  metadata,
			Operation: queryOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("invalid params metadata", func(t *testing.T) {
		metadata := map[string]string{
			commandSQLKey:    "SELECT * FROM foo WHERE id < @p1",
			commandParamsKey: "not-json",
		}
		req := &bindings.InvokeRequest{
			Metadata:  metadata,
			Operation: queryOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("missing sql metadata", func(t *testing.T) {
		req := &bindings.InvokeRequest{
			Metadata:  map[string]string{},
			Operation: execOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("unsupported operation", func(t *testing.T) {
		req := &bindings.InvokeRequest{
			Metadata:  map[string]string{commandSQLKey: "SELECT 1"},
			Operation: "unsupported",
		}
		resp, err := m.Invoke(t.Context(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("close operation", func(t *testing.T) {
		mock.ExpectClose()
		req := &bindings.InvokeRequest{
			Operation: closeOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		require.NoError(t, err)
		assert.Nil(t, resp)
	})

	t.Run("invoke after close fails", func(t *testing.T) {
		req := &bindings.InvokeRequest{
			Metadata:  map[string]string{commandSQLKey: "SELECT 1"},
			Operation: queryOperation,
		}
		resp, err := m.Invoke(t.Context(), req)
		assert.Nil(t, resp)
		require.Error(t, err)
	})
}

func TestOperations(t *testing.T) {
	b := NewSQLServer(logger.NewLogger("test"))
	require.NotNil(t, b)
	ops := b.Operations()
	assert.Len(t, ops, 3)
	assert.Contains(t, ops, execOperation)
	assert.Contains(t, ops, queryOperation)
	assert.Contains(t, ops, closeOperation)
}

func mockDatabase(t *testing.T) (*SQLServer, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	m := NewSQLServer(logger.NewLogger("test")).(*SQLServer)
	m.db = db

	return m, mock, err
}
