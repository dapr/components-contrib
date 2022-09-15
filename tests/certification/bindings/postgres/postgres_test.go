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
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	binding_postgres "github.com/dapr/components-contrib/bindings/postgres"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
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

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]

	testExec := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		metadata := make(map[string]string)

		ctx.Log("Invoking output binding for exec operation!")
		req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "exec", Metadata: metadata}
		req.Metadata["sql"] = "INSERT INTO dapr_test_table (id, c1, ts) VALUES (1, 'demo', '2020-09-24T11:45:05Z07:00');"
		errBinding := client.InvokeOutputBinding(ctx, req)
		require.NoError(ctx, errBinding, "error in output binding - exec")

		return nil
	}

	testQuery := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		metadata := make(map[string]string)

		ctx.Log("Invoking output binding for query operation!")
		req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "query", Metadata: metadata}
		req.Metadata["sql"] = "SELECT * FROM dapr_test_table WHERE id = 1;"
		resp, errBinding := client.InvokeBinding(ctx, req)
		assert.Contains(t, string(resp.Data), "1,\"demo\",\"2020-09-24T11:45:05Z07:00\"")
		require.NoError(ctx, errBinding, "error in output binding - query")

		return nil
	}

	testClose := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		metadata := make(map[string]string)

		ctx.Log("Invoking output binding for query operation!")
		req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "close", Metadata: metadata}
		errBinding := client.InvokeOutputBinding(ctx, req)
		require.NoError(ctx, errBinding, "error in output binding - close")

		req = &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "query", Metadata: metadata}
		req.Metadata["sql"] = "SELECT * FROM dapr_test_table WHERE id = 1;"
		errBinding = client.InvokeOutputBinding(ctx, req)
		require.Error(ctx, errBinding, "error in output binding - query")

		return nil
	}

	createTable := func(ctx flow.Context) error {
		db, err := sql.Open("postgres", dockerConnectionString)
		assert.NoError(t, err)
		_, err = db.Exec("CREATE TABLE dapr_test_table(id INT, c1 TEXT, ts TEXT);")
		assert.NoError(t, err)
		db.Close()
		return nil
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Creating table", createTable).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step(sidecar.Run("standardSidecar",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/standard"),
			componentRuntimeOptions(),
		)).
		Step("Run exec test", testExec).
		Step("Run query test", testQuery).
		Step("wait for DB operations to complete", flow.Sleep(10*time.Second)).
		Step("Run close test", testClose).
		Step("stop postgresql", dockercompose.Stop("db", dockerComposeYAML, "db")).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Run()
}

func TestPostgresNetworkError(t *testing.T) {

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]

	testExec := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		metadata := make(map[string]string)

		ctx.Log("Invoking output binding for exec operation!")
		req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "exec", Metadata: metadata}
		req.Metadata["sql"] = "INSERT INTO dapr_test_table (id, c1, ts) VALUES (1, 'demo', '2020-09-24T11:45:05Z07:00');"
		errBinding := client.InvokeOutputBinding(ctx, req)
		require.NoError(ctx, errBinding, "error in output binding - exec")

		return nil
	}

	testQuery := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		metadata := make(map[string]string)

		ctx.Log("Invoking output binding for query operation!")
		req := &daprClient.InvokeBindingRequest{Name: "standard-binding", Operation: "query", Metadata: metadata}
		req.Metadata["sql"] = "SELECT * FROM dapr_test_table WHERE id = 1;"
		errBinding := client.InvokeOutputBinding(ctx, req)
		require.NoError(ctx, errBinding, "error in output binding - query")

		return nil
	}

	createTable := func(ctx flow.Context) error {
		db, err := sql.Open("postgres", dockerConnectionString)
		assert.NoError(t, err)
		_, err = db.Exec("CREATE TABLE dapr_test_table(id INT, c1 TEXT, ts TEXT);")
		assert.NoError(t, err)
		db.Close()
		return nil
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Creating table", createTable).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step(sidecar.Run("standardSidecar",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/standard"),
			componentRuntimeOptions(),
		)).
		Step("Run exec test", testExec).
		Step("Run query test", testQuery).
		Step("wait for DB operations to complete", flow.Sleep(10*time.Second)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "5432")).
		Step("wait for component to recover", flow.Sleep(10*time.Second)).
		Step("Run query test", testQuery).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return binding_postgres.NewPostgres(l)
	}, "postgres")

	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
	}
}
