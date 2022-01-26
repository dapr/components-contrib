/*
Copyright 2021 The Dapr Authors
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

package clickhouse

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	ck, mock, _ := mockDatabase(t)
	defer ck.Close()

	t.Run("no dbType provided", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "value", "timestamp"}).
			AddRow(1, "value-1", time.Now()).
			AddRow(2, "value-2", time.Now().Add(1000)).
			AddRow(3, "value-3", time.Now().Add(2000))

		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < 4").WillReturnRows(rows)
		ret, err := ck.query(`SELECT * FROM foo WHERE id < 4`)
		assert.Nil(t, err)
		t.Logf("query result: %s", ret)
		assert.Contains(t, string(ret), "\"id\":1")
		var result []interface{}
		err = json.Unmarshal(ret, &result)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(result))
	})

	t.Run("dbType provided", func(t *testing.T) {
		col1 := sqlmock.NewColumn("id").OfType("BIGINT", 1)
		col2 := sqlmock.NewColumn("value").OfType("FLOAT", 1.0)
		col3 := sqlmock.NewColumn("timestamp").OfType("TIME", time.Now())
		rows := sqlmock.NewRowsWithColumnDefinition(col1, col2, col3).
			AddRow(1, 1.1, time.Now()).
			AddRow(2, 2.2, time.Now().Add(1000)).
			AddRow(3, 3.3, time.Now().Add(2000))
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < 4").WillReturnRows(rows)
		ret, err := ck.query("SELECT * FROM foo WHERE id < 4")
		assert.Nil(t, err)
		t.Logf("query result: %s", ret)

		// verify number
		assert.Contains(t, string(ret), "\"id\":1")
		assert.Contains(t, string(ret), "\"value\":2.2")

		var result []interface{}
		err = json.Unmarshal(ret, &result)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(result))

		// verify timestamp
		ts, ok := result[0].(map[string]interface{})["timestamp"].(string)
		assert.True(t, ok)
		var tt time.Time
		tt, err = time.Parse(time.RFC3339, ts)
		assert.Nil(t, err)
		t.Logf("time stamp is: %v", tt)
	})
}

func TestExec(t *testing.T) {
	ck, mock, _ := mockDatabase(t)
	defer ck.Close()
	mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnResult(sqlmock.NewResult(1, 1))
	err := ck.exec("INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')")
	assert.Nil(t, err)
}

func TestInvoke(t *testing.T) {
	ck, mock, _ := mockDatabase(t)
	defer ck.Close()

	t.Run("exec operation succeeds", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnResult(sqlmock.NewResult(1, 1))
		metadata := map[string]string{commandSQLKey: "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  metadata,
			Operation: execOperation,
		}
		_, err := ck.Invoke(req)
		assert.Nil(t, err)
	})

	t.Run("exec operation fails", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO foo \\(id, v1, ts\\) VALUES \\(.*\\)").WillReturnError(errors.New("insert failed"))
		metadata := map[string]string{commandSQLKey: "INSERT INTO foo (id, v1, ts) VALUES (1, 'test-1', '2021-01-22')"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  metadata,
			Operation: execOperation,
		}
		resp, err := ck.Invoke(req)
		assert.Nil(t, resp)
		assert.NotNil(t, err)
	})

	t.Run("query operation succeeds", func(t *testing.T) {
		col1 := sqlmock.NewColumn("id").OfType("BIGINT", 1)
		col2 := sqlmock.NewColumn("value").OfType("FLOAT", 1.0)
		col3 := sqlmock.NewColumn("timestamp").OfType("TIME", time.Now())
		rows := sqlmock.NewRowsWithColumnDefinition(col1, col2, col3).AddRow(1, 1.1, time.Now())
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < \\d+").WillReturnRows(rows)

		metadata := map[string]string{commandSQLKey: "SELECT * FROM foo WHERE id < 2"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  metadata,
			Operation: queryOperation,
		}
		resp, err := ck.Invoke(req)
		assert.Nil(t, err)
		var data []interface{}
		err = json.Unmarshal(resp.Data, &data)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(data))
	})

	t.Run("query operation fails", func(t *testing.T) {
		mock.ExpectQuery("SELECT \\* FROM foo WHERE id < \\d+").WillReturnError(errors.New("query failed"))
		metadata := map[string]string{commandSQLKey: "SELECT * FROM foo WHERE id < 2"}
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  metadata,
			Operation: queryOperation,
		}
		resp, err := ck.Invoke(req)
		assert.Nil(t, resp)
		assert.NotNil(t, err)
	})

	t.Run("close operation", func(t *testing.T) {
		mock.ExpectClose()
		req := &bindings.InvokeRequest{
			Operation: closeOperation,
		}
		resp, _ := ck.Invoke(req)
		assert.Nil(t, resp)
	})

	t.Run("unsupported operation", func(t *testing.T) {
		req := &bindings.InvokeRequest{
			Data:      nil,
			Metadata:  map[string]string{},
			Operation: "unsupported",
		}
		resp, err := ck.Invoke(req)
		assert.Nil(t, resp)
		assert.NotNil(t, err)
	})
}

func mockDatabase(t *testing.T) (*Clickhouse, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	ck := NewClickhouse(logger.NewLogger("test"))
	ck.db = db

	return ck, mock, err
}
