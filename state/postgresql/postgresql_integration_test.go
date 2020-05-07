// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package postgresql

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"os"
	"database/sql"
	"strings"
	//_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	connectionStringEnvKey = "DAPR_TEST_POSTGRES_CONNSTRING"
	databaseName = "dapr_test"
)

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

func TestIntegrationCases(t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("SQLServer state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"server=localhost;user id=sa;password=Pass@Word1;port=1433;\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	ensureDBIsValid(t)
	//t.Run("Single operations", testSingleOperations)
}
