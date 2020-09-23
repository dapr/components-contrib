// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgres

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

// SETUP TESTS

// 1. createdb daprtest
// 2. createuser daprtest
// 3. psql=# grant all privileges on database daprtest to daprtest;
// 2. export POSTGRES_TEST_CONN_URL="host=localhost user=daprtest dbname=daprtest sslmode=disable"
// 3. go test -v -count=1 ./bindings/postgres -run ^TestCRUD
func TestCRUD(t *testing.T) {
	if os.Getenv("POSTGRES_RUN_LIVE_TEST") != "true" {
		t.Log("POSTGRES_RUN_LIVE_TEST not set, skipping test")
		t.SkipNow() // skip this test unless the PG DB is available
	}
	t.Log("POSTGRES_RUN_LIVE_TEST set, running live DB test")

	// live DB test
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		ConnectionURLKey: os.Getenv("POSTGRES_TEST_CONN_URL"),
	}

	logger := logger.NewLogger("test")
	logger.SetOutputLevel("debug")
	b := NewBinding(logger)

	err := b.Init(m)
	assert.NoError(t, err)

	// create table
	ddl := `CREATE TABLE IF NOT EXISTS foo (
		id bigint NOT NULL,
		v1 character varying(50) NOT NULL,
		ts TIMESTAMP
	)`

	req := &bindings.InvokeRequest{
		Operation: ExecOperation,
		Metadata:  map[string]string{CommandSQLKey: ddl},
	}
	res, err := b.Invoke(req)
	assertResponse(t, res, err)

	// delete all previous records if any
	req.Metadata[CommandSQLKey] = "DELETE FROM foo"
	res, err = b.Invoke(req)
	assertResponse(t, res, err)

	// insert recrods
	for i := 0; i < 10; i++ {
		req.Metadata[CommandSQLKey] = fmt.Sprintf(
			"INSERT INTO foo (id, v1, ts) VALUES (%d, 'test-%d', '%v')",
			i, i, time.Now().Format(time.RFC3339),
		)
		res, err = b.Invoke(req)
		assertResponse(t, res, err)
	}

	// update recrods
	for i := 0; i < 10; i++ {
		req.Metadata[CommandSQLKey] = fmt.Sprintf(
			"UPDATE foo SET ts = '%v' WHERE id = %d",
			time.Now().Format(time.RFC3339), i,
		)
		res, err = b.Invoke(req)
		assertResponse(t, res, err)
	}

	// select records
	req.Operation = QueryOperation
	req.Metadata[CommandSQLKey] = "SELECT * FROM foo WHERE id < 3"
	res, err = b.Invoke(req)
	assertResponse(t, res, err)
	t.Logf("result data: %v", string(res.Data))

	// delete records
	req.Operation = ExecOperation
	req.Metadata[CommandSQLKey] = "DELETE FROM foo"
	res, err = b.Invoke(req)
	assertResponse(t, res, err)

	// close connection
	req.Operation = CloseOperation
	_, err = b.Invoke(req)
	assert.NoError(t, err)

}

func assertResponse(t *testing.T, res *bindings.InvokeResponse, err error) {
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Metadata)
	t.Logf("result meta: %v", res.Metadata)
}
