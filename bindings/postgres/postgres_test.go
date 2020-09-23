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

func TestOperations(t *testing.T) {
	t.Parallel()
	b := NewPostgres(logger.NewLogger("test"))
	assert.NotNil(t, b)
	l := b.Operations()
	assert.Equal(t, 3, len(l))
}

// SETUP TESTS
// 1. `createdb daprtest`
// 2. `createuser daprtest`
// 3. `psql=# grant all privileges on database daprtest to daprtest;``
// 4. `export POSTGRES_TEST_CONN_URL="postgres://daprtest@localhost:5432/daprtest?application_name=test&connect_timeout=5"``
// 5. `go test -v -count=1 ./bindings/postgres -run ^TestPostgresIntegration`

func TestPostgresIntegration(t *testing.T) {
	url := os.Getenv("POSTGRES_TEST_CONN_URL")
	if url == "" {
		t.SkipNow()
	}

	// live DB test
	b := NewPostgres(logger.NewLogger("test"))
	err := b.Init(bindings.Metadata{Properties: map[string]string{connectionURLKey: url}})
	assert.NoError(t, err)

	// create table
	req := &bindings.InvokeRequest{
		Operation: execOperation,
		Metadata:  map[string]string{commandSQLKey: testTableDDL},
	}
	res, err := b.Invoke(req)
	assertResponse(t, res, err)

	// delete all previous records if any
	req.Metadata[commandSQLKey] = testDelete
	res, err = b.Invoke(req)
	assertResponse(t, res, err)

	// insert recrods
	for i := 0; i < 10; i++ {
		req.Metadata[commandSQLKey] = fmt.Sprintf(testInsert, i, i, time.Now().Format(time.RFC3339))
		res, err = b.Invoke(req)
		assertResponse(t, res, err)
	}

	// update recrods
	for i := 0; i < 10; i++ {
		req.Metadata[commandSQLKey] = fmt.Sprintf(testUpdate, time.Now().Format(time.RFC3339), i)
		res, err = b.Invoke(req)
		assertResponse(t, res, err)
	}

	// select records
	req.Operation = queryOperation
	req.Metadata[commandSQLKey] = testSelect
	res, err = b.Invoke(req)
	assertResponse(t, res, err)
	t.Logf("result data: %v", string(res.Data))

	// delete records
	req.Operation = execOperation
	req.Metadata[commandSQLKey] = testDelete
	res, err = b.Invoke(req)
	assertResponse(t, res, err)

	// close connection
	req.Operation = closeOperation
	_, err = b.Invoke(req)
	assert.NoError(t, err)
}

func assertResponse(t *testing.T, res *bindings.InvokeResponse, err error) {
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Metadata)
	t.Logf("result meta: %v", res.Metadata)
}
