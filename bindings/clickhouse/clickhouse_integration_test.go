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
	"os"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	testCreateTable = `CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory`
	testDropTable = `DROP TABLE example`
	testInsert    = "INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)"
	testDelete    = "ALTER TABLE example DELETE WHERE os_id = ?"
	testUpdate    = "ALTER TABLE example UPDATE country_code = ? WHERE os_id = ?"
	testSelect    = "SELECT * FROM example WHERE os_id < 15"
)

func TestOperations(t *testing.T) {
	t.Parallel()
	t.Run("Get operation list", func(t *testing.T) {
		t.Parallel()
		b := NewClickhouse(nil)
		assert.NotNil(t, b)
		l := b.Operations()
		assert.Equal(t, 6, len(l))
		assert.Contains(t, l, execOperation)
		assert.Contains(t, l, closeOperation)
		assert.Contains(t, l, queryOperation)
		assert.Contains(t, l, insertOperation)
		assert.Contains(t, l, updateOperation)
		assert.Contains(t, l, deleteOperation)
	})
}

// SETUP TESTS
// 1. clickhouse client --query='CREATE DATABASE IF NOT EXISTS daprtest'
// 2. `CREATE USER daprtest`
// 3. `export CLICKHOUSE_TEST_CONN_URL="tcp://localhost:9000?database=daprtest"`
// 4. `go test -v -count=1 ./bindings/clickhouse -run ^TestClickHouseIntegration`.
func TestClickHouseIntegration(t *testing.T) {
	url := os.Getenv("CLICKHOUSE_TEST_CONN_URL")
	if url == "" {
		t.SkipNow()
	}

	ck := NewClickhouse(logger.NewLogger("test"))
	m := bindings.Metadata{Properties: map[string]string{connectionURLKey: url}}
	if err := ck.Init(m); err != nil {
		t.Fatal(err)
	}

	defer ck.Close()

	req := &bindings.InvokeRequest{Metadata: map[string]string{}}

	t.Run("Invoke create table", func(t *testing.T) {
		req.Operation = execOperation
		req.Metadata[commandSQLKey] = testCreateTable
		res, err := ck.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke delete", func(t *testing.T) {
		req.Operation = deleteOperation
		req.Metadata[commandSQLKey] = testDelete
		data := []interface{}{uint8(18)}
		b, err := msgpack.Marshal(&data)
		if err != nil {
			panic(err)
		}
		req.Data = b
		res, err := ck.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke insert", func(t *testing.T) {
		req.Operation = insertOperation
		for i := 0; i < 10; i++ {
			req.Metadata[commandSQLKey] = testInsert
			data := []interface{}{"RU", uint8(10 + i), uint8(100 + i), []int16{1, 2, 3}, time.Now(), time.Now()}
			b, err := msgpack.Marshal(data)
			if err != nil {
				panic(err)
			}
			req.Data = b
			res, err := ck.Invoke(req)
			assertResponse(t, res, err)
		}
	})

	t.Run("Invoke update", func(t *testing.T) {
		req.Operation = updateOperation
		for i := 0; i < 10; i++ {
			req.Metadata[commandSQLKey] = testUpdate
			data := []interface{}{"CN", uint8(10 + i)}
			b, err := msgpack.Marshal(data)
			if err != nil {
				panic(err)
			}
			req.Data = b
			res, err := ck.Invoke(req)
			assertResponse(t, res, err)
		}
	})

	t.Run("Invoke select", func(t *testing.T) {
		req.Operation = queryOperation
		req.Metadata[commandSQLKey] = testSelect
		res, err := ck.Invoke(req)
		assertResponse(t, res, err)
		t.Logf("received result: %s", res.Data)

		// verify result.
		assert.Contains(t, string(res.Data), "\"country_code\":\"CN\"")
		assert.Contains(t, string(res.Data), "\"os_id\":11")
		assert.Contains(t, string(res.Data), "\"categories\":[1,2,3]")

		result := make([]interface{}, 0)
		err = json.Unmarshal(res.Data, &result)
		assert.Nil(t, err)
		assert.Equal(t, 5, len(result))

		cc, ok := result[0].(map[string]interface{})["country_code"].(string)
		assert.True(t, ok)
		assert.Equal(t, cc, "CN")
	})

	t.Run("Invoke delete", func(t *testing.T) {
		req.Operation = deleteOperation
		req.Metadata[commandSQLKey] = testDelete
		data := []interface{}{uint8(10)}
		b, err := msgpack.Marshal(data)
		if err != nil {
			panic(err)
		}
		req.Data = b
		res, err := ck.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke drop", func(t *testing.T) {
		req.Operation = execOperation
		req.Metadata[commandSQLKey] = testDropTable
		res, err := ck.Invoke(req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke close", func(t *testing.T) {
		req.Operation = closeOperation
		req.Metadata = nil
		req.Data = nil
		_, err := ck.Invoke(req)
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
