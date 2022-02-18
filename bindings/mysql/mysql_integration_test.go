/*
Copyright 2022 The Dapr Authors
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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	// MySQL doesn't accept RFC3339 formatted time, rejects trailing 'Z' for UTC indicator.
	mySQLDateTimeFormat = "2006-01-02 15:04:05"

	testCreateTable = `CREATE TABLE IF NOT EXISTS foo (
		id bigint NOT NULL,
		v1 character varying(50) NOT NULL,
		b  BOOLEAN,
		ts TIMESTAMP,
		data LONGTEXT)`
	testDropTable         = `DROP TABLE foo`
	testInsert            = "INSERT INTO foo (id, v1, b, ts, data) VALUES (%d, 'test-%d', %t, '%v', '%s')"
	testDelete            = "DELETE FROM foo"
	testUpdate            = "UPDATE foo SET ts = '%v' WHERE id = %d"
	testSelect            = "SELECT * FROM foo WHERE id < 3"
	testSelectJSONExtract = "SELECT JSON_EXTRACT(data, '$.key') AS `key` FROM foo WHERE id < 3"
)

func TestOperations(t *testing.T) {
	t.Parallel()
	t.Run("Get operation list", func(t *testing.T) {
		t.Parallel()
		b := NewMysql(nil)
		assert.NotNil(t, b)
		l := b.Operations()
		assert.Equal(t, 3, len(l))
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
		t.SkipNow()
	}

	b := NewMysql(logger.NewLogger("test"))
	m := bindings.Metadata{Properties: map[string]string{connectionURLKey: url}}
	if err := b.Init(m); err != nil {
		t.Fatal(err)
	}

	defer b.Close()

	req := &bindings.InvokeRequest{Metadata: map[string]string{}}

	t.Run("Invoke create table", func(t *testing.T) {
		req.Operation = execOperation
		req.Metadata[commandSQLKey] = testCreateTable
		res, err := b.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke delete", func(t *testing.T) {
		req.Operation = execOperation
		req.Metadata[commandSQLKey] = testDelete
		res, err := b.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke insert", func(t *testing.T) {
		req.Operation = execOperation
		for i := 0; i < 10; i++ {
			req.Metadata[commandSQLKey] = fmt.Sprintf(testInsert, i, i, true, time.Now().Format(mySQLDateTimeFormat), "{\"key\":\"val\"}")
			res, err := b.Invoke(req)
			assertResponse(t, res, err)
		}
	})

	t.Run("Invoke update", func(t *testing.T) {
		req.Operation = execOperation
		for i := 0; i < 10; i++ {
			req.Metadata[commandSQLKey] = fmt.Sprintf(testUpdate, time.Now().Format(mySQLDateTimeFormat), i)
			res, err := b.Invoke(req)
			assertResponse(t, res, err)
		}
	})

	t.Run("Invoke select", func(t *testing.T) {
		req.Operation = queryOperation
		req.Metadata[commandSQLKey] = testSelect
		res, err := b.Invoke(req)
		assertResponse(t, res, err)
		t.Logf("received result: %s", res.Data)

		// verify number, boolean and string
		assert.Contains(t, string(res.Data), "\"id\":1")
		assert.Contains(t, string(res.Data), "\"b\":1")
		assert.Contains(t, string(res.Data), "\"v1\":\"test-1\"")
		assert.Contains(t, string(res.Data), "\"data\":\"{\\\"key\\\":\\\"val\\\"}\"")

		result := make([]interface{}, 0)
		err = json.Unmarshal(res.Data, &result)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(result))

		// verify timestamp
		ts, ok := result[0].(map[string]interface{})["ts"].(string)
		assert.True(t, ok)
		// have to use custom layout to parse timestamp, see this: https://github.com/dapr/components-contrib/pull/615
		var tt time.Time
		tt, err = time.Parse("2006-01-02T15:04:05Z", ts)
		assert.Nil(t, err)
		t.Logf("time stamp is: %v", tt)
	})

	t.Run("Invoke select JSON_EXTRACT", func(t *testing.T) {
		req.Operation = queryOperation
		req.Metadata[commandSQLKey] = testSelectJSONExtract
		res, err := b.Invoke(req)
		assertResponse(t, res, err)
		t.Logf("received result: %s", res.Data)

		// verify json extract number
		assert.Contains(t, string(res.Data), "{\"key\":\"\\\"val\\\"\"}")

		result := make([]interface{}, 0)
		err = json.Unmarshal(res.Data, &result)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(result))
	})

	t.Run("Invoke delete", func(t *testing.T) {
		req.Operation = execOperation
		req.Metadata[commandSQLKey] = testDelete
		req.Data = nil
		res, err := b.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke drop", func(t *testing.T) {
		req.Operation = execOperation
		req.Metadata[commandSQLKey] = testDropTable
		res, err := b.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke close", func(t *testing.T) {
		req.Operation = closeOperation
		req.Metadata = nil
		req.Data = nil
		_, err := b.Invoke(req)
		assert.NoError(t, err)
	})
}

func assertResponse(t *testing.T, res *bindings.InvokeResponse, err error) {
	assert.NoError(t, err)
	assert.NotNil(t, res)
	if res != nil {
		assert.NotNil(t, res.Metadata)
	}
}
