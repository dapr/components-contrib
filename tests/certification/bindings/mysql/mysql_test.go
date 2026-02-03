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

package mysql_test

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	// MySQL driver for database/sql
	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	binding_mysql "github.com/dapr/components-contrib/bindings/mysql"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	dockerComposeYAML      = "docker-compose.yml"
	numOfMessages          = 10
	dockerConnectionString = "mysql:example@tcp(localhost:3306)/dapr_test?allowNativePasswords=true"
)

// MySQL doesn't accept RFC3339 formatted time, rejects trailing 'Z' for UTC indicator.
const mySQLDateTimeFormat = "2006-01-02 15:04:05"

func TestMysql(t *testing.T) {
	const tableName = "dapr_test_table"

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]

	testExec := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client")

		ctx.Log("Invoking output binding for exec operation")
		err = client.InvokeOutputBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql": fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (1, 'demo', '%s');", tableName, time.Now().Format(mySQLDateTimeFormat)),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec")

		ctx.Log("Invoking output binding for exec operation with parameters")
		err = client.InvokeOutputBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql":    fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (?, ?, ?);", tableName),
				"params": fmt.Sprintf(`[2, "demo2", "%s"]`, time.Now().Add(time.Hour).Format(mySQLDateTimeFormat)),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec")

		return nil
	}

	testQuery := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client")

		ctx.Log("Invoking output binding for query operation")
		resp, err := client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "query",
			Metadata: map[string]string{
				"sql": "SELECT * FROM " + tableName + " WHERE id = 1;",
			},
		})
		require.NoError(ctx, err, "error in output binding - query")
		assert.Contains(t, string(resp.Data), `"id":1`)
		assert.Contains(t, string(resp.Data), `"c1":"demo"`)

		ctx.Log("Invoking output binding for query operation with parameters")
		resp, err = client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "query",
			Metadata: map[string]string{
				"sql":    "SELECT * FROM " + tableName + " WHERE id IN (?, ?);",
				"params": `[1, 2]`,
			},
		})
		require.NoError(ctx, err, "error in output binding - query")
		assert.Contains(t, string(resp.Data), `"id":1`)
		assert.Contains(t, string(resp.Data), `"id":2`)

		return nil
	}

	testClose := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(ctx, err, "Could not initialize dapr client.")

		metadata := make(map[string]string)

		ctx.Log("Invoking output binding for close operation!")
		req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "close", Metadata: metadata}
		errBinding := client.InvokeOutputBinding(ctx, req)
		require.NoError(ctx, errBinding, "error in output binding - close")

		ctx.Log("Invoking output binding for query operation!")
		req = &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "query", Metadata: metadata}
		req.Metadata["sql"] = "SELECT * FROM " + tableName + " WHERE id = 1;"
		errBinding = client.InvokeOutputBinding(ctx, req)
		require.Error(ctx, errBinding, "error in output binding - query")

		return nil
	}

	createTable := func(ctx flow.Context) error {
		db, err := sql.Open("mysql", dockerConnectionString)
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE " + tableName + " (id INT, c1 TEXT, ts TIMESTAMP);")
		require.NoError(t, err)
		db.Close()
		return nil
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Creating table", createTable).
		Step(sidecar.Run("standardSidecar",
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/standard"),
			)...,
		)).
		Step("Run exec test", testExec).
		Step("Run query test", testQuery).
		Step("Run close test", testClose).
		Step("stop mysql", dockercompose.Stop("db", dockerComposeYAML, "db")).
		Run()
}

func TestMysqlNetworkError(t *testing.T) {
	const tableName = "dapr_test_table_network"

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]

	testExec := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client")

		ctx.Log("Invoking output binding for exec operation!")
		errBinding := client.InvokeOutputBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql": fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (1, 'demo', '%s');", tableName, time.Now().Format(mySQLDateTimeFormat)),
			},
		})
		require.NoError(ctx, errBinding, "error in output binding - exec")

		return nil
	}

	testQuery := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client")

		ctx.Log("Invoking output binding for query operation!")
		_, errBinding := client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "query",
			Metadata: map[string]string{
				"sql": "SELECT * FROM " + tableName + " WHERE id = 1;",
			},
		})
		require.NoError(ctx, errBinding, "error in output binding - query")

		return nil
	}

	createTable := func(ctx flow.Context) error {
		db, err := sql.Open("mysql", dockerConnectionString)
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE " + tableName + " (id INT, c1 TEXT, ts TIMESTAMP);")
		require.NoError(t, err)
		db.Close()
		return nil
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Creating table", createTable).
		Step(sidecar.Run("standardSidecar",
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/standard"),
			)...,
		)).
		Step("Run exec test", testExec).
		Step("Run query test", testQuery).
		Step("wait for DB operations to complete", flow.Sleep(5*time.Second)).
		Step("interrupt network", network.InterruptNetwork(20*time.Second, nil, nil, "3306")).
		Step("wait for component to recover", flow.Sleep(10*time.Second)).
		Step("Run query test", testQuery).
		Run()
}

func TestMysqlExecEncoding(t *testing.T) {
	const tableName = "dapr_test_table_encoding"

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]

	testExecEncoding := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client")

		ctx.Log("Testing exec binding response encoding for INSERT operation")
		resp, err := client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql": fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (1, 'test1', '%s');", tableName, time.Now().Format(mySQLDateTimeFormat)),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec")
		require.NotNil(ctx, resp, "response should not be nil")
		require.NotNil(ctx, resp.Metadata, "response metadata should not be nil")

		// Verify rows-affected metadata exists and is a string
		rowsAffected, exists := resp.Metadata["rows-affected"]
		require.True(ctx, exists, "rows-affected metadata should exist")
		assert.Equal(t, "1", rowsAffected, "rows-affected should be '1' for single INSERT")
		
		// Verify the encoding is correct (string, not number)
		// Parse to verify it's a valid integer string
		rowsCount, err := strconv.ParseInt(rowsAffected, 10, 64)
		require.NoError(ctx, err, "rows-affected should be parseable as int64")
		assert.Equal(t, int64(1), rowsCount, "rows-affected should be 1")

		ctx.Log("Testing exec binding response encoding for UPDATE operation")
		resp, err = client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql": fmt.Sprintf("UPDATE %s SET c1 = 'updated' WHERE id = 1;", tableName),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec UPDATE")
		require.NotNil(ctx, resp, "response should not be nil")
		
		rowsAffected, exists = resp.Metadata["rows-affected"]
		require.True(ctx, exists, "rows-affected metadata should exist for UPDATE")
		assert.Equal(t, "1", rowsAffected, "rows-affected should be '1' for single UPDATE")
		
		// Verify encoding again
		rowsCount, err = strconv.ParseInt(rowsAffected, 10, 64)
		require.NoError(ctx, err, "rows-affected should be parseable as int64")
		assert.Equal(t, int64(1), rowsCount, "rows-affected should be 1")

		ctx.Log("Testing exec binding response encoding for INSERT with multiple rows")
		resp, err = client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql": fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (2, 'test2', '%s'), (3, 'test3', '%s');", 
					tableName, time.Now().Format(mySQLDateTimeFormat), time.Now().Format(mySQLDateTimeFormat)),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec multi-row INSERT")
		require.NotNil(ctx, resp, "response should not be nil")
		
		rowsAffected, exists = resp.Metadata["rows-affected"]
		require.True(ctx, exists, "rows-affected metadata should exist for multi-row INSERT")
		assert.Equal(t, "2", rowsAffected, "rows-affected should be '2' for two-row INSERT")
		
		// Verify encoding for multiple rows
		rowsCount, err = strconv.ParseInt(rowsAffected, 10, 64)
		require.NoError(ctx, err, "rows-affected should be parseable as int64")
		assert.Equal(t, int64(2), rowsCount, "rows-affected should be 2")

		ctx.Log("Testing exec binding response encoding for DELETE operation")
		resp, err = client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql": fmt.Sprintf("DELETE FROM %s WHERE id IN (2, 3);", tableName),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec DELETE")
		require.NotNil(ctx, resp, "response should not be nil")
		
		rowsAffected, exists = resp.Metadata["rows-affected"]
		require.True(ctx, exists, "rows-affected metadata should exist for DELETE")
		assert.Equal(t, "2", rowsAffected, "rows-affected should be '2' for two-row DELETE")
		
		// Verify encoding for DELETE
		rowsCount, err = strconv.ParseInt(rowsAffected, 10, 64)
		require.NoError(ctx, err, "rows-affected should be parseable as int64")
		assert.Equal(t, int64(2), rowsCount, "rows-affected should be 2")

		ctx.Log("Testing exec binding response encoding for UPDATE with no matching rows")
		resp, err = client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql": fmt.Sprintf("UPDATE %s SET c1 = 'no-match' WHERE id = 999;", tableName),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec UPDATE with no match")
		require.NotNil(ctx, resp, "response should not be nil")
		
		rowsAffected, exists = resp.Metadata["rows-affected"]
		require.True(ctx, exists, "rows-affected metadata should exist even for zero rows")
		assert.Equal(t, "0", rowsAffected, "rows-affected should be '0' when no rows match")
		
		// Verify encoding for zero rows
		rowsCount, err = strconv.ParseInt(rowsAffected, 10, 64)
		require.NoError(ctx, err, "rows-affected should be parseable as int64")
		assert.Equal(t, int64(0), rowsCount, "rows-affected should be 0")

		// Verify other metadata fields are present and correctly encoded
		operation, exists := resp.Metadata["operation"]
		require.True(ctx, exists, "operation metadata should exist")
		assert.Equal(t, "exec", operation, "operation should be 'exec'")

		sql, exists := resp.Metadata["sql"]
		require.True(ctx, exists, "sql metadata should exist")
		assert.Contains(t, sql, "UPDATE", "sql metadata should contain the SQL statement")

		_, exists = resp.Metadata["start-time"]
		require.True(ctx, exists, "start-time metadata should exist")

		_, exists = resp.Metadata["end-time"]
		require.True(ctx, exists, "end-time metadata should exist")

		_, exists = resp.Metadata["duration"]
		require.True(ctx, exists, "duration metadata should exist")

		return nil
	}

	createTable := func(ctx flow.Context) error {
		db, err := sql.Open("mysql", dockerConnectionString)
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE " + tableName + " (id INT, c1 TEXT, ts TIMESTAMP);")
		require.NoError(t, err)
		db.Close()
		return nil
	}

	flow.New(t, "Run exec encoding tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Creating table", createTable).
		Step(sidecar.Run("standardSidecar",
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithComponentsPath("./components/standard"),
			)...,
		)).
		Step("Run exec encoding test", testExecEncoding).
		Step("stop mysql", dockercompose.Stop("db", dockerComposeYAML, "db")).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return binding_mysql.NewMysql(l)
	}, "mysql")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
