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
// 2. POSTGRES_TEST_CONNECT_URL="postgres://daprtest@localhost/daprtest?sslmode=disable"
// 3. go test -v -count=1 ./bindings/postgres -run ^TestInit
func TestInit(t *testing.T) {
	logger := logger.NewLogger("test")
	logger.SetOutputLevel("debug")
	b := NewBinding(logger)

	if os.Getenv("POSTGRES_RUN_LIVE_TEST") != "true" {
		t.Log("POSTGRES_RUN_LIVE_TEST not set, skipping test")
		t.SkipNow() // skip this test unless the PG DB is available
	}
	t.Log("POSTGRES_RUN_LIVE_TEST set, running live DB test")

	m := bindings.Metadata{}
	m.Properties = map[string]string{
		ConnectionURLKey: os.Getenv("POSTGRES_TEST_CONNECT_URL"),
	}

	err := b.Init(m)
	assert.NoError(t, err)

	ddl := `CREATE TABLE IF NOT EXISTS foo (
		id bigint NOT NULL,
		v1 character varying(50) NOT NULL,
		ts TIMESTAMP
	);`

	req := &bindings.InvokeRequest{
		Operation: ExecOperation,
		Metadata:  map[string]string{CommandSQLKey: ddl},
	}

	res, err := b.Invoke(req)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Metadata)
	t.Logf("result: %v", res.Metadata)

	for i := 0; i < 10; i++ {
		req.Metadata[CommandSQLKey] = fmt.Sprintf(`INSERT INTO foo 
			(id, v1, ts) VALUES (%d, 'test-%d', '%v');
		`, i, i, time.Now())
		res, err = b.Invoke(req)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.NotNil(t, res.Metadata)
		t.Logf("result: %v", res.Metadata)
	}

	req.Operation = CloseOperation
	_, err = b.Invoke(req)
	assert.NoError(t, err)

}
