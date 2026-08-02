/*
Copyright 2024 The Dapr Authors
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

package sqlserver_test

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	// MSSQL driver for database/sql
	_ "github.com/microsoft/go-mssqldb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	binding_sqlserver "github.com/dapr/components-contrib/bindings/sqlserver"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	dockerComposeYAML = "docker-compose.yml"

	// Connection string without a database: used to create the test database itself.
	// Connection Timeout is generous because CREATE DATABASE can be slow right after the
	// container has *just* become available to accept logins.
	dockerConnectionStringNoDB = "server=localhost;user id=sa;password=Pass@Word1;port=1433;Connection Timeout=30;"
	// Connection string used by the component under test, and by test setup code once the DB exists.
	dockerConnectionString = "server=localhost;user id=sa;password=Pass@Word1;port=1433;database=dapr_test;Connection Timeout=30;"
)

func setGodebugX509Workaround(t *testing.T) {
	// The default certificate created by the SQL Server docker container sometimes contains a negative serial number.
	// A TLS certificate with a negative serial number is invalid, although it was tolerated until Go 1.22.
	// Since Go 1.23 the default behavior has changed and the certificate is rejected.
	// This environment variable reverts to the old behavior.
	// Ref: https://github.com/microsoft/mssql-docker/issues/895
	oldDebugValue := os.Getenv("GODEBUG")
	suffix := ""
	if oldDebugValue != "" {
		suffix = "," + oldDebugValue
	}
	t.Setenv("GODEBUG", "x509negativeserial=1"+suffix)
	t.Cleanup(func() {
		t.Setenv("GODEBUG", oldDebugValue)
	})
}

// checkSQLServerAvailability polls the container directly (no database selected yet) so that
// dockercompose's own healthcheck - which reports "healthy" well before the TDS listener is
// actually ready to accept connections - doesn't cause the next steps to race against startup.
func checkSQLServerAvailability(ctx flow.Context) error {
	db, err := sql.Open("mssql", dockerConnectionStringNoDB)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("SELECT 1;")
	return err
}

func createDatabaseAndTable(t *testing.T, tableName string) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		db, err := sql.Open("mssql", dockerConnectionStringNoDB)
		require.NoError(t, err)
		_, err = db.Exec("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'dapr_test') CREATE DATABASE [dapr_test];")
		require.NoError(t, err)
		db.Close()

		db, err = sql.Open("mssql", dockerConnectionString)
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE " + tableName + " (id INT, c1 NVARCHAR(100), ts DATETIME2);")
		require.NoError(t, err)
		db.Close()
		return nil
	}
}

func TestSqlServer(t *testing.T) {
	setGodebugX509Workaround(t)

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
				"sql": fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (1, 'demo', '%s');", tableName, time.Now().Format(time.RFC3339)),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec")

		ctx.Log("Invoking output binding for exec operation with parameters")
		err = client.InvokeOutputBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql":    fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (@p1, @p2, @p3);", tableName),
				"params": fmt.Sprintf(`[2, "demo2", "%s"]`, time.Now().Add(time.Hour).Format(time.RFC3339)),
			},
		})
		require.NoError(ctx, err, "error in output binding - exec with parameters")

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
				"sql":    "SELECT * FROM " + tableName + " WHERE id IN (@p1, @p2);",
				"params": `[1, 2]`,
			},
		})
		require.NoError(ctx, err, "error in output binding - query with parameters")
		assert.Contains(t, string(resp.Data), `"id":1`)
		assert.Contains(t, string(resp.Data), `"id":2`)

		return nil
	}

	testClose := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(ctx, err, "Could not initialize dapr client.")

		ctx.Log("Invoking output binding for close operation")
		err = client.InvokeOutputBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "close",
			Metadata:  map[string]string{},
		})
		require.NoError(ctx, err, "error in output binding - close")

		ctx.Log("Invoking output binding for query operation after close")
		_, err = client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "query",
			Metadata: map[string]string{
				"sql": "SELECT * FROM " + tableName + " WHERE id = 1;",
			},
		})
		require.Error(ctx, err, "expected error invoking a closed binding")

		return nil
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
		Step("wait for SQL Server readiness", retry.Do(3*time.Second, 30, checkSQLServerAvailability)).
		Step("settle before DDL", flow.Sleep(5*time.Second)).
		Step("Creating database and table", createDatabaseAndTable(t, tableName)).
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
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()
}

func TestSqlServerNetworkError(t *testing.T) {
	setGodebugX509Workaround(t)

	const tableName = "dapr_test_table_network"

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
				"sql": fmt.Sprintf("INSERT INTO %s (id, c1, ts) VALUES (1, 'demo', '%s');", tableName, time.Now().Format(time.RFC3339)),
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

		return nil
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
		Step("wait for SQL Server readiness", retry.Do(3*time.Second, 30, checkSQLServerAvailability)).
		Step("settle before DDL", flow.Sleep(5*time.Second)).
		Step("Creating database and table", createDatabaseAndTable(t, tableName)).
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
		Step("interrupt network", network.InterruptNetwork(20*time.Second, nil, nil, "1433")).
		Step("wait for component to recover", flow.Sleep(10*time.Second)).
		Step("Run query test", testQuery).
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return binding_sqlserver.NewSQLServer(l)
	}, "sqlserver")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
