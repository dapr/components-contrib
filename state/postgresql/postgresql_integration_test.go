// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package postgresql

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	connectionStringEnvKey = "DAPR_TEST_POSTGRES_CONNSTRING"
	databaseName = "dapr_test"
)

func TestIntegrationCases(t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("SQLServer state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"server=localhost;user id=sa;password=Pass@Word1;port=1433;\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	ensureDBIsValid(t)
	//t.Run("Single operations", testSingleOperations)
}

func TestInitConfiguration(t *testing.T) {
	
	logger := logger.NewLogger("test")
	tests := []struct {
		name        string
		props       map[string]string
		expectedErr string
	}{
		{
			name:        "Empty",
			props:       map[string]string{},
			expectedErr: errMissingConnectionString,
		},
		{
			name:        "Valid connection string",
			props:       map[string]string{connectionStringKey: getConnectionString(),},
			expectedErr: "",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			
			metadata := state.Metadata{
				Properties: tt.props,
			}

			p := NewPostgreSQLStateStore(logger, NewPostgresDBAccess(logger, metadata))

			err := p.Init(metadata)
			if tt.expectedErr == "" {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func getMasterConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func getConnectionString() string {
	if connString := getMasterConnectionString(); connString != "" {
		if strings.Contains(connString, "database=") {
			return connString
		}

		return connString + " database=" + databaseName
	}

	return ""
}

func ensureDBIsValid(t *testing.T) {
	cstr := getMasterConnectionString()
	db, err := sql.Open("pgx", cstr)
	assert.Nil(t, err)
	
	defer db.Close()
	err = db.Ping()
	assert.Nil(t, err)

	ensureDBExists(t, db)
}

// Creates the test database if it does not exist
func ensureDBExists(t *testing.T, db *sql.DB){
	var exists int = -1
	err := db.QueryRow("SELECT Count(*) as Exists FROM pg_database WHERE datname = $1", databaseName).Scan(&exists)
	assert.Nil(t, err)
	assert.NotEqual(t, exists, -1)
	if(exists == 0){
		_, err = db.Exec("CREATE DATABASE $1", databaseName)
		assert.Nil(t, err)
	}
}
