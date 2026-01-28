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

package postgres

import (
	"fmt"
	"net"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	pgauth "github.com/dapr/components-contrib/common/authentication/postgresql"
	"github.com/dapr/components-contrib/configuration"
	metadatapkg "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestSelectAllQuery(t *testing.T) {
	g := &configuration.GetRequest{}
	expected := "SELECT * FROM cfgtbl"
	query, _, err := buildQuery(g, "cfgtbl")
	if err != nil {
		t.Errorf("Error building query: %v ", err)
	}
	assert.Equal(t, expected, query, "did not get expected result. Got: '%v' , Expected: '%v'", query, expected)

	g = &configuration.GetRequest{
		Keys: []string{},
		Metadata: map[string]string{
			"Version": "1.0",
		},
	}
	query, _, err = buildQuery(g, "cfgtbl")
	if err != nil {
		t.Errorf("Error building query: %v ", err)
	}
	require.NoError(t, err, "Error building query: %v ", err)
	assert.Equal(t, expected, query, "did not get expected result. Got: '%v' , Expected: '%v'", query, expected)
}

func TestPostgresbuildQuery(t *testing.T) {
	g := &configuration.GetRequest{
		Keys: []string{"someKey"},
		Metadata: map[string]string{
			"Version": "1.0",
		},
	}

	query, params, err := buildQuery(g, "cfgtbl")
	_ = params
	require.NoError(t, err, "Error building query: %v ", err)
	expected := "SELECT * FROM cfgtbl WHERE KEY IN ($1) AND $2 = $3"
	assert.Equal(t, expected, query, "did not get expected result. Got: '%v' , Expected: '%v'", query, expected)
	i := 0
	for _, v := range params {
		got := v.(string)
		switch i {
		case 0:
			expected := "someKey"
			assert.Equal(t, expected, got, "Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
		case 1:
			expected := "Version"
			assert.Equal(t, expected, got, "Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
		case 2:
			expected := "1.0"
			assert.Equal(t, expected, got, "Did not get expected result. Got: '%v' , Expected: '%v'", got, expected)
		}
		i++
	}
}

func TestConnectAndQuery(t *testing.T) {
	m := metadata{
		PostgresAuthMetadata: pgauth.PostgresAuthMetadata{
			ConnectionString: "mockConnectionString",
		},
		ConfigTable: "mockConfigTable",
	}

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	query := "SELECT EXISTS (SELECT FROM pg_tables where tablename = '" + m.ConfigTable + "'"
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WillReturnRows(pgxmock.NewRows(
			[]string{"exists"}).
			AddRow(string("t")),
		)
	rows := mock.QueryRow(t.Context(), query)
	var id string
	err = rows.Scan(&id)
	require.NoError(t, err, "error in scan")
	err = mock.ExpectationsWereMet()
	require.NoError(t, err, "pgxmock error in expectations were met")
}

func TestValidateInput(t *testing.T) {
	keys := []string{"testKey1", "testKey2"}
	require.NoError(t, validateInput(keys), "incorrect input provided: %v", keys)

	var keys2 []string
	require.NoError(t, validateInput(keys), "incorrect input provided: %v", keys2)

	keys3 := []string{"Name 1=1"}
	require.Error(t, validateInput(keys3), "invalid key : 'Name 1=1'")
}

func TestPostgresConfigurationWithIAM(t *testing.T) {
	ctx := t.Context()

	// Testing use of moto to mock AWS services for IAM authentication
	motoContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "motoserver/moto:5.1.19",
			ExposedPorts: []string{"5000/tcp"},
			WaitingFor:   wait.ForLog("Running on http://127.0.0.1:5000"),
		},
		Started: true,
	})
	require.NoError(t, err)
	defer func() { _ = motoContainer.Terminate(ctx) }()

	motoHost, err := motoContainer.Host(ctx)
	require.NoError(t, err)
	motoPort, err := motoContainer.MappedPort(ctx, "5000")
	require.NoError(t, err)
	motoURL := "http://" + net.JoinHostPort(motoHost, motoPort.Port())

	t.Setenv("AWS_ENDPOINT_URL", motoURL)
	t.Setenv("AWS_ACCESS_KEY_ID", "testing")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testing")
	t.Setenv("AWS_REGION", "us-east-1")

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_PASSWORD": "password",
				"POSTGRES_DB":       "testdb",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("5432/tcp"),
				wait.ForLog("database system is ready to accept connections"),
			),
		},
		Started: true,
	})
	require.NoError(t, err)
	defer func() { _ = pgContainer.Terminate(ctx) }()

	pgHost, err := pgContainer.Host(ctx)
	require.NoError(t, err)
	pgPort, err := pgContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)

	pgConnString := "postgres://postgres:password@" + net.JoinHostPort(pgHost, pgPort.Port()) + "/testdb?sslmode=disable"

	pgConn, err := pgx.Connect(ctx, pgConnString)
	require.NoError(t, err)
	defer func() { _ = pgConn.Close(ctx) }()

	cfg, err := config.LoadDefaultConfig(ctx)
	require.NoError(t, err)

	token, err := auth.BuildAuthToken(ctx, net.JoinHostPort(pgHost, pgPort.Port()), "us-east-1", "testuser", cfg.Credentials)
	require.NoError(t, err)

	_, err = pgConn.Exec(ctx, fmt.Sprintf("CREATE USER testuser WITH PASSWORD '%s'", token))
	require.NoError(t, err)

	_, err = pgConn.Exec(ctx, "CREATE TABLE config_table (key TEXT PRIMARY KEY, value TEXT, version TEXT, metadata JSONB)")
	require.NoError(t, err)

	_, err = pgConn.Exec(ctx, "GRANT ALL ON config_table TO testuser")
	require.NoError(t, err)

	t.Run("Valid IAM Authentication", func(t *testing.T) {
		metadata := map[string]string{
			"connectionString": "postgres://testuser@" + net.JoinHostPort(pgHost, pgPort.Port()) + "/testdb?sslmode=disable",
			"table":            "config_table",
			"useAWSIAM":        "true",
			"awsRegion":        "us-east-1",
			"awsAccessKey":     "testing",
			"awsSecretKey":     "testing",
		}

		store := NewPostgresConfigurationStore(logger.NewLogger("test"))
		err := store.Init(ctx, configuration.Metadata{Base: metadatapkg.Base{Properties: metadata}})
		require.NoError(t, err)

		t.Run("Get Request", func(t *testing.T) {
			resp, err2 := store.Get(ctx, &configuration.GetRequest{})
			require.NoError(t, err2)
			assert.Empty(t, resp.Items)
		})

		err = store.Close()
		require.NoError(t, err)
	})

	t.Run("Invalid IAM Authentication", func(t *testing.T) {
		metadata := map[string]string{
			"connectionString": "postgres://testuser@" + net.JoinHostPort(pgHost, pgPort.Port()) + "/testdb?sslmode=disable",
			"table":            "config_table",
			"useAWSIAM":        "true",
			"awsRegion":        "us-east-1",
			"awsAccessKey":     "testingincorrect",
			"awsSecretKey":     "testingincorrect",
		}

		store := NewPostgresConfigurationStore(logger.NewLogger("test"))
		err := store.Init(ctx, configuration.Metadata{Base: metadatapkg.Base{Properties: metadata}})
		require.Error(t, err)

		err = store.Close()
		require.NoError(t, err)
	})
}
