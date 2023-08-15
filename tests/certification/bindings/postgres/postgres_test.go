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

package postgres_test

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	// PGX driver for database/sql
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	binding_postgres "github.com/dapr/components-contrib/bindings/postgres"
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
	dockerConnectionString = "host=localhost user=postgres password=example port=5432 connect_timeout=10 database=dapr_test sslmode=disable"
)

func TestPostgres(t *testing.T) {
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
				"sql": "INSERT INTO " + tableName + " (id, c1, ts) VALUES (1, 'demo', '2020-09-24T11:45:05+07:00');",
			},
		})
		require.NoError(ctx, err, "error in output binding - exec")

		ctx.Log("Invoking output binding for exec operation with parameters")
		err = client.InvokeOutputBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "exec",
			Metadata: map[string]string{
				"sql":    "INSERT INTO " + tableName + " (id, c1, ts) VALUES ($1, $2, $3);",
				"params": `[2, "demo2", "2021-03-19T11:45:05+07:00"]`,
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
		assert.Equal(t, `[[1,"demo","2020-09-24T11:45:05Z"]]`, string(resp.Data))
		require.NoError(ctx, err, "error in output binding - query")

		ctx.Log("Invoking output binding for query operation with parameters")
		resp, err = client.InvokeBinding(ctx, &daprClient.InvokeBindingRequest{
			Name:      "standard-binding",
			Operation: "query",
			Metadata: map[string]string{
				"sql":    "SELECT * FROM " + tableName + " WHERE id = ANY($1);",
				"params": `[[1, 2]]`,
			},
		})
		assert.Equal(t, `[[1,"demo","2020-09-24T11:45:05Z"],[2,"demo2","2021-03-19T11:45:05Z"]]`, string(resp.Data))
		require.NoError(ctx, err, "error in output binding - query")

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
		db, err := sql.Open("pgx", dockerConnectionString)
		assert.NoError(t, err)
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
		Step("stop postgresql", dockercompose.Stop("db", dockerComposeYAML, "db")).
		Run()
}

func TestPostgresNetworkError(t *testing.T) {
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
				"sql": "INSERT INTO " + tableName + " (id, c1, ts) VALUES (1, 'demo', '2020-09-24T11:45:05+07:00');",
			},
		})
		require.NoError(ctx, errBinding, "error in output binding - exec")

		return nil
	}

	testQuery := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client")

		ctx.Log("Invoking output binding for query operation!")
		errBinding := client.InvokeOutputBinding(ctx, &daprClient.InvokeBindingRequest{
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
		db, err := sql.Open("pgx", dockerConnectionString)
		assert.NoError(t, err)
		_, err = db.Exec("CREATE TABLE " + tableName + " (id INT, c1 TEXT, ts TIMESTAMP);")
		assert.NoError(t, err)
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
		Step("interrupt network", network.InterruptNetwork(20*time.Second, nil, nil, "5432")).
		Step("wait for component to recover", flow.Sleep(10*time.Second)).
		Step("Run query test", testQuery).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return binding_postgres.NewPostgres(l)
	}, "postgresql")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
