// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package postgresql

import (
	"database/sql"
	"os"
	"testing"
	"github.com/google/uuid"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	_ "github.com/jackc/pgx/v4/stdlib"
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

func TestSet(t *testing.T) {
	
	key := uuid.New().String()
	
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: getConnectionString(),},
	}

	pgs := getNewPostgreSQLStore()
	pgs.Init(metadata)

	setReq := &state.SetRequest{
		Key: key,
		Value: `{"something": "somevalue"}`,
	}

	pgs.Set(setReq)
}

func getConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func getNewPostgreSQLStore() *PostgreSQL {
	
	logger := logger.NewLogger("test")
	return NewPostgreSQLStateStore(NewPostgresDBAccess(logger))
}
