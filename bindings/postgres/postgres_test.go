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

const (
	testTableDDL = `CREATE TABLE IF NOT EXISTS foo (
		id bigint NOT NULL,
		v1 character varying(50) NOT NULL,
		ts TIMESTAMP)`
	testInsert = "INSERT INTO foo (id, v1, ts) VALUES (%d, 'test-%d', '%v')"
	testDelete = "DELETE FROM foo"
	testUpdate = "UPDATE foo SET ts = '%v' WHERE id = %d"
	testSelect = "SELECT * FROM foo WHERE id < 3"
)

// SETUP TESTS
// 1. createdb daprtest
// 2. createuser daprtest
// 3. psql=# grant all privileges on database daprtest to daprtest;
// 4. export POSTGRES_TEST_CONN_URL="postgres://daprtest@localhost:5432/daprtest?application_name=test&connect_timeout=5"

// TO STEST
// go test -v -count=1 ./bindings/postgres -run ^TestCRUD

func TestCRUD(t *testing.T) {
	// skip this test unless the PG DB is available
	if os.Getenv("POSTGRES_RUN_LIVE_TEST") != "true" {
		t.Log("POSTGRES_RUN_LIVE_TEST not set, skipping test")
		t.SkipNow()
	}
	t.Log("POSTGRES_RUN_LIVE_TEST set, running live DB test")

	// live DB test
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		ConnectionURLKey: os.Getenv("POSTGRES_TEST_CONN_URL"),
	}

	logger := logger.NewLogger("test")
	// logger.SetOutputLevel("debug")
	b := NewBinding(logger)

	err := b.Init(m)
	assert.NoError(t, err)

	// create table
	req := &bindings.InvokeRequest{
		Operation: ExecOperation,
		Metadata:  map[string]string{CommandSQLKey: testTableDDL},
	}
	res, err := b.Invoke(req)
	assertResponse(t, res, err)

	// delete all previous records if any
	req.Metadata[CommandSQLKey] = testDelete
	res, err = b.Invoke(req)
	assertResponse(t, res, err)

	// insert recrods
	for i := 0; i < 10; i++ {
		req.Metadata[CommandSQLKey] = fmt.Sprintf(testInsert, i, i, time.Now().Format(time.RFC3339))
		res, err = b.Invoke(req)
		assertResponse(t, res, err)
	}

	// update recrods
	for i := 0; i < 10; i++ {
		req.Metadata[CommandSQLKey] = fmt.Sprintf(testUpdate, time.Now().Format(time.RFC3339), i)
		res, err = b.Invoke(req)
		assertResponse(t, res, err)
	}

	// select records
	req.Operation = QueryOperation
	req.Metadata[CommandSQLKey] = testSelect
	res, err = b.Invoke(req)
	assertResponse(t, res, err)
	t.Logf("result data: %v", string(res.Data))

	// delete records
	req.Operation = ExecOperation
	req.Metadata[CommandSQLKey] = testDelete
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
