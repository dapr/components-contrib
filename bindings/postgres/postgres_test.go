/*
Copyright 2023 The Dapr Authors
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

package postgres

import (
	"context"
	"errors"
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

const (
	testCustomersTableDDL = `CREATE TABLE IF NOT EXISTS customers (
								customer_id SERIAL NOT NULL PRIMARY KEY,
								customer_name VARCHAR(255),
								contact_name VARCHAR(255),
								address VARCHAR(255),
								city VARCHAR(255),
								postal_code VARCHAR(255),
								country VARCHAR(255)
								)`
)

func TestOperations(t *testing.T) {
	t.Parallel()
	t.Run("Get operation list", func(t *testing.T) {
		b := NewPostgres(nil)
		assert.NotNil(t, b)
		l := b.Operations()
		assert.Len(t, l, 7)
	})
}

// SETUP TESTS
// 1. `createdb daprtest`
// 2. `createuser daprtest`
// 3. `psql=# grant all privileges on database daprtest to daprtest;``
// 4. `export POSTGRES_TEST_CONN_URL="postgres://daprtest@localhost:5432/daprtest"``
// 5. `go test -v -count=1 ./bindings/postgres -run ^TestPostgresIntegration`

func TestPostgresIntegration(t *testing.T) {
	url := os.Getenv("POSTGRES_TEST_CONN_URL")
	if url == "" {
		t.SkipNow()
	}

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t, url)
	})

	// live DB test
	b := NewPostgres(logger.NewLogger("test")).(*Postgres)
	m := bindings.Metadata{Base: metadata.Base{Properties: map[string]string{"connectionString": url}}}
	if err := b.Init(context.Background(), m); err != nil {
		t.Fatal(err)
	}

	// create table
	req := &bindings.InvokeRequest{
		Operation: execOperation,
		Metadata:  map[string]string{commandSQLKey: testTableDDL},
	}
	ctx := context.TODO()
	t.Run("Invoke create table", func(t *testing.T) {
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke delete", func(t *testing.T) {
		req.Metadata[commandSQLKey] = testDelete
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke insert", func(t *testing.T) {
		for i := range 10 {
			req.Metadata[commandSQLKey] = fmt.Sprintf(testInsert, i, i, time.Now().Format(time.RFC3339))
			res, err := b.Invoke(ctx, req)
			assertResponse(t, res, err)
		}
	})

	t.Run("Invoke update", func(t *testing.T) {
		for i := range 10 {
			req.Metadata[commandSQLKey] = fmt.Sprintf(testUpdate, time.Now().Format(time.RFC3339), i)
			res, err := b.Invoke(ctx, req)
			assertResponse(t, res, err)
		}
	})

	t.Run("Invoke select", func(t *testing.T) {
		req.Operation = queryOperation
		req.Metadata[commandSQLKey] = testSelect
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke delete", func(t *testing.T) {
		req.Operation = execOperation
		req.Metadata[commandSQLKey] = testDelete
		req.Data = nil
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
	})

	t.Run("Invoke close", func(t *testing.T) {
		req.Operation = closeOperation
		req.Metadata = nil
		req.Data = nil
		_, err := b.Invoke(ctx, req)
		require.NoError(t, err)
	})

	t.Run("Close", func(t *testing.T) {
		err := b.Close()
		require.NoError(t, err, "expected no error closing output binding")
	})
}

// testInitConfiguration tests valid and invalid config settings.
func testInitConfiguration(t *testing.T, connectionString string) {
	logger := logger.NewLogger("test")
	tests := []struct {
		name        string
		props       map[string]string
		expectedErr error
	}{
		{
			name:        "Empty",
			props:       map[string]string{},
			expectedErr: errors.New("missing connection string"),
		},
		{
			name:        "Valid connection string",
			props:       map[string]string{"connectionString": connectionString},
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPostgres(logger).(*Postgres)
			defer p.Close()

			metadata := bindings.Metadata{
				Base: metadata.Base{Properties: tt.props},
			}

			err := p.Init(context.Background(), metadata)
			if tt.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.expectedErr, err)
			}
		})
	}
}

// `go test -v -count=1 ./bindings/postgres -run ^TestPostgresEntityIntegration`
func TestPostgresEntityIntegration(t *testing.T) {
	url := os.Getenv("POSTGRES_TEST_CONN_URL")
	if url == "" {
		t.SkipNow()
	}

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t, url)
	})

	// live DB test
	b := NewPostgres(logger.NewLogger("test")).(*Postgres)
	m := bindings.Metadata{Base: metadata.Base{Properties: map[string]string{"connectionString": url}}}
	if err := b.Init(context.Background(), m); err != nil {
		t.Fatal(err)
	}

	// create table
	req := &bindings.InvokeRequest{
		Operation: execOperation,
		Metadata:  map[string]string{commandSQLKey: testCustomersTableDDL},
	}
	ctx := context.TODO()
	t.Run("Invoke create table", func(t *testing.T) {
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
	})

	req = &bindings.InvokeRequest{
		Operation: registerOperation,
		Metadata: map[string]string{operationType: "entity",
			commandEntityName: "customers"},
	}
	t.Run("Invoke register", func(t *testing.T) {
		req.Metadata[commandEntityId] = "customer_id"
		req.Metadata[commandEntityProps] = "[\"customer_id\",\"customer_name\",\"contact_name\",\"address\", \"city\", \"postal_code\",\"country\"]"
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
	})

	req = &bindings.InvokeRequest{
		Operation: findAllOperation,
		Metadata: map[string]string{operationType: "entity",
			commandEntityName: "customers"},
	}

	t.Run("Invoke findAll", func(t *testing.T) {
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
		assert.Equal(t, string(res.Data), "[]")
	})

	req = &bindings.InvokeRequest{
		Operation: saveOperation,
		Metadata: map[string]string{operationType: "entity",
			commandEntityName: "customers"},
	}
	t.Run("Invoke save", func(t *testing.T) {
		// @TODO: we should marshal a customer struct to JSON, but it will not be an array
		req.Metadata[commandEntityProps] = "[\"'salaboy'\",\"'salaboy'\",\"'chiswick'\", \"'london'\", \"'w4'\",\"'uk'\"]"
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
	})

	req = &bindings.InvokeRequest{
		Operation: findAllOperation,
		Metadata: map[string]string{operationType: "entity",
			commandEntityName: "customers"},
	}
	t.Run("Invoke findAll", func(t *testing.T) {
		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
		assert.NotNil(t, res.Data)
		//@TODO: we can marshal this into a customer struct and validate props
		assert.Contains(t, string(res.Data), "\"salaboy\",\"salaboy\",\"chiswick\",\"london\",\"w4\",\"uk\"")
	})

	req = &bindings.InvokeRequest{
		Operation: deleteAllOperation,
		Metadata: map[string]string{operationType: "entity",
			commandEntityName: "customers"},
	}
	t.Run("Invoke delete all", func(t *testing.T) {

		res, err := b.Invoke(ctx, req)
		assertResponse(t, res, err)
		assert.Equal(t, res.Metadata["rows-affected"], "1")
	})

	t.Run("Close", func(t *testing.T) {
		err := b.Close()
		require.NoError(t, err, "expected no error closing output binding")
	})
}

func assertResponse(t *testing.T, res *bindings.InvokeResponse, err error) {
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Metadata)
}
