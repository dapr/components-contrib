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

package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// MySQL doesn't accept RFC3339 formatted time, rejects trailing 'Z' for UTC indicator.
const mySQLDateTimeFormat = "2006-01-02 15:04:05"

func TestOperations(t *testing.T) {
	t.Run("Get operation list", func(t *testing.T) {
		b := NewMysql(logger.NewLogger("test"))
		require.NotNil(t, b)
		l := b.Operations()
		assert.Len(t, l, 3)
		assert.Contains(t, l, execOperation)
		assert.Contains(t, l, closeOperation)
		assert.Contains(t, l, queryOperation)
	})
}

// SETUP TESTS
// 1. `CREATE DATABASE daprtest;`
// 2. `CREATE USER daprtest;`
// 3. `GRANT ALL PRIVILEGES ON daprtest.* to daprtest;`
// 4. `export MYSQL_TEST_CONN_URL=daprtest@tcp(localhost:3306)/daprtest`
// 5. `go test -v -count=1 ./bindings/mysql -run ^TestMysqlIntegrationWithURL`

func TestMysqlIntegration(t *testing.T) {
	url := os.Getenv("MYSQL_TEST_CONN_URL")
	if url == "" {
		t.Skip("Skipping because env var MYSQL_TEST_CONN_URL is empty")
	}

	b := NewMysql(logger.NewLogger("test")).(*Mysql)
	m := bindings.Metadata{Base: metadata.Base{Properties: map[string]string{connectionURLKey: url}}}

	err := b.Init(context.Background(), m)
	require.NoError(t, err)

	defer b.Close()

	t.Run("Invoke create table", func(t *testing.T) {
		res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
			Operation: execOperation,
			Metadata: map[string]string{
				commandSQLKey: `CREATE TABLE IF NOT EXISTS foo (
					id bigint NOT NULL,
					v1 character varying(50) NOT NULL,
					b  BOOLEAN,
					ts TIMESTAMP,
					data LONGTEXT)`,
			},
		})
		assertResponse(t, res, err)
	})

	t.Run("Invoke delete", func(t *testing.T) {
		res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
			Operation: execOperation,
			Metadata: map[string]string{
				commandSQLKey: "DELETE FROM foo",
			},
		})
		assertResponse(t, res, err)
	})

	t.Run("Invoke insert", func(t *testing.T) {
		for i := range 10 {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: execOperation,
				Metadata: map[string]string{
					commandSQLKey: fmt.Sprintf(
						"INSERT INTO foo (id, v1, b, ts, data) VALUES (%d, 'test-%d', %t, '%v', '%s')",
						i, i, true, time.Now().Format(mySQLDateTimeFormat), `{"key":"val"}`),
				},
			})
			assertResponse(t, res, err)
		}
	})

	t.Run("Invoke update", func(t *testing.T) {
		date := time.Now().Add(time.Hour)
		for i := range 10 {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: execOperation,
				Metadata: map[string]string{
					commandSQLKey: fmt.Sprintf(
						"UPDATE foo SET ts = '%v' WHERE id = %d",
						date.Add(10*time.Duration(i)*time.Second).Format(mySQLDateTimeFormat), i),
				},
			})
			assertResponse(t, res, err)
			assert.Equal(t, "1", res.Metadata[respRowsAffectedKey])
		}
	})

	t.Run("Invoke update with parameters", func(t *testing.T) {
		date := time.Now().Add(2 * time.Hour)
		for i := range 10 {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: execOperation,
				Metadata: map[string]string{
					commandSQLKey:    "UPDATE foo SET ts = ? WHERE id = ?",
					commandParamsKey: fmt.Sprintf(`[%q,%d]`, date.Add(10*time.Duration(i)*time.Second).Format(mySQLDateTimeFormat), i),
				},
			})
			assertResponse(t, res, err)
			assert.Equal(t, "1", res.Metadata[respRowsAffectedKey])
		}
	})

	t.Run("Invoke select", func(t *testing.T) {
		res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
			Operation: queryOperation,
			Metadata: map[string]string{
				commandSQLKey: "SELECT * FROM foo WHERE id < 3",
			},
		})
		assertResponse(t, res, err)
		t.Logf("received result: %s", res.Data)

		// verify number, boolean and string
		assert.Contains(t, string(res.Data), `"id":1`)
		assert.Contains(t, string(res.Data), `"b":1`)
		assert.Contains(t, string(res.Data), `"v1":"test-1"`)
		assert.Contains(t, string(res.Data), `"data":"{\"key\":\"val\"}"`)

		result := make([]any, 0)
		err = json.Unmarshal(res.Data, &result)
		require.NoError(t, err)
		assert.Len(t, len(result), 3)

		// verify timestamp
		ts, ok := result[0].(map[string]any)["ts"].(string)
		assert.True(t, ok)
		// have to use custom layout to parse timestamp, see this: https://github.com/dapr/components-contrib/pull/615
		var tt time.Time
		tt, err = time.Parse("2006-01-02T15:04:05Z", ts)
		require.NoError(t, err)
		t.Logf("time stamp is: %v", tt)
	})

	t.Run("Invoke select with parameters", func(t *testing.T) {
		res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
			Operation: queryOperation,
			Metadata: map[string]string{
				commandSQLKey:    "SELECT * FROM foo WHERE id = ?",
				commandParamsKey: `[1]`,
			},
		})
		assertResponse(t, res, err)
		t.Logf("received result: %s", res.Data)

		// verify number, boolean and string
		assert.Contains(t, string(res.Data), `"id":1`)
		assert.Contains(t, string(res.Data), `"b":1`)
		assert.Contains(t, string(res.Data), `"v1":"test-1"`)
		assert.Contains(t, string(res.Data), `"data":"{\"key\":\"val\"}"`)

		result := make([]any, 0)
		err = json.Unmarshal(res.Data, &result)
		require.NoError(t, err)
		assert.Len(t, len(result), 1)
	})

	t.Run("Invoke drop", func(t *testing.T) {
		res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
			Operation: execOperation,
			Metadata: map[string]string{
				commandSQLKey: "DROP TABLE foo",
			},
		})
		assertResponse(t, res, err)
	})

	t.Run("Invoke close", func(t *testing.T) {
		_, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
			Operation: closeOperation,
		})
		require.NoError(t, err)
	})
}

func assertResponse(t *testing.T, res *bindings.InvokeResponse, err error) {
	t.Helper()

	require.NoError(t, err)
	assert.NotNil(t, res)
	if res != nil {
		assert.NotEmpty(t, res.Metadata)
	}
}
