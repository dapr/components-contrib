// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package postgresql

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
)

const (
	connectionStringEnvKey = "DAPR_TEST_POSTGRES_CONNSTRING" // Environment variable containing the connection string
	databaseName = "dapr_test"
)

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
			
			metadata := &state.Metadata{
				Properties: tt.props,
			}

			p := NewPostgreSQLStateStore(NewPostgresDBAccess(logger))

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

func TestDBIsValid(t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("SQLServer state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"server=localhost;user id=sa;password=Pass@Word1;port=1433;\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	db, err := sql.Open("pgx", connectionString)
	assert.Nil(t, err)
	
	defer db.Close()
	err = db.Ping()
	assert.Nil(t, err)
}

func TestSetGetDelete(t *testing.T) {
	
	key := uuid.New().String()
	
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: getConnectionString(),},
	}

	pgs := getNewPostgreSQLStore()
	pgs.Init(metadata)

	setReq := &state.SetRequest{
		Key: key,
		Metadata: metadata.Properties,
		Value: `{"something": "somevalue"}`,
	}

	// Set
	err := pgs.Set(setReq)
	assert.Nil(t, err)
	assert.True(t, storeItemExists(t, key))

	// Get
	getReq := &state.GetRequest{
		Key: key,
		Metadata: metadata.Properties,
		Options: state.GetStateOption{},
	}
	
	getResponse, getErr := pgs.Get(getReq)
	assert.NotNil(t, getResponse)
	assert.Nil(t, getErr)

	// Delete
	deleteReq := &state.DeleteRequest{
		Key: key,
		Metadata: metadata.Properties,
		Options: state.DeleteStateOption{},
	}

	deleteErr := pgs.Delete(deleteReq)
	assert.Nil(t, deleteErr)
	assert.False(t, storeItemExists(t, key))

}

func getConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func getDbConnection(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", getConnectionString())
	assert.Nil(t, err)
	return db
}

func getNewPostgreSQLStore() *PostgreSQL {
	logger := logger.NewLogger("test")
	return NewPostgreSQLStateStore(NewPostgresDBAccess(logger))
}

func storeItemExists(t *testing.T, key string) bool {
	var exists bool = false
	statement := fmt.Sprintf(`SELECT EXISTS (SELECT FROM %s WHERE key = $1)`, tableName)
	err := getDbConnection(t).QueryRow(statement, key).Scan(&exists)
	assert.Nil(t, err)
	return exists
}