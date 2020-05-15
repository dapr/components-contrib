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

func TestPostgreSQLIntegration (t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("SQLServer state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"server=localhost;user id=sa;password=Pass@Word1;port=1433;\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: connectionString,},
	}
	
	pgs := NewPostgreSQLStateStore(NewPostgresDBAccess(logger.NewLogger("test")))
	defer pgs.Close()

	error := pgs.Init(*metadata)
	if error != nil {
		t.Fatal(error)
	}

	// Can set and get an item.
	t.Run("Get Set Delete one item", func(t *testing.T) {
		key := uuid.New().String()
		value := `{"something": "somevalue"}`
		setItem(t, pgs, key, value)
		getResponse := getItem(t, pgs, key)
		assert.Equal(t, value, string(getResponse.Data))
		deleteItem(t, pgs, key)
	})

	// When an item does not exist, get should return a response with Data set to nil.
	t.Run("Get item that does not exist", func(t *testing.T) {
		key := uuid.New().String()
		response := getItem(t, pgs, key)
		assert.Nil(t, response.Data)
	})

	// Insert date and update date are correctly set and updated in the database
	t.Run("Set updates the updatedate field", func(t *testing.T) {
		key := uuid.New().String()
		value := `{"something": "somevalue"}`
		setItem(t, pgs, key, value)
		
		// insertdate should have a value and updatedate should be nil
		retrievedValue, insertdate, updatedate := getRowData(t, key)
		assert.Equal(t, value, retrievedValue)
		assert.NotNil(t, insertdate)
		assert.Equal(t, "", updatedate.String)

		// insertdate should not change, updatedate should have a value
		value = `{"newthing": "newvalue"}`
		setItem(t, pgs, key, value)
		retrievedValue, newinsertdate, updatedate := getRowData(t, key)
		assert.Equal(t, value, retrievedValue)
		assert.Equal(t, insertdate, newinsertdate) // The insertdate should not change.
		assert.NotEqual(t, "", updatedate.String)
		
		deleteItem(t, pgs, key)
	})
}

func TestInitConfiguration(t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("SQLServer state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"server=localhost;user id=sa;password=Pass@Word1;port=1433;\")", connectionStringEnvKey, connectionStringEnvKey)
	}

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
			defer p.Close()

			err := p.Init(*metadata)
			if tt.expectedErr == "" {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func getConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func setItem(t *testing.T, pgs *PostgreSQL, key string, value string) {
	setReq := &state.SetRequest{
		Key: key,
		Metadata: pgs.metadata.Properties,
		Value: value,
	}

	err := pgs.Set(setReq)
	assert.Nil(t, err)
	assert.True(t, storeItemExists(t, key))
}

func getItem (t *testing.T, pgs *PostgreSQL, key string) *state.GetResponse {
	getReq := &state.GetRequest{
		Key: key,
		Metadata: pgs.metadata.Properties,
		Options: state.GetStateOption{},
	}
	
	response, getErr := pgs.Get(getReq)
	assert.Nil(t, getErr)
	assert.NotNil(t, response)
	return response
}

func deleteItem (t *testing.T, pgs *PostgreSQL, key string) {
	deleteReq := &state.DeleteRequest{
		Key: key,
		Metadata: pgs.metadata.Properties,
		Options: state.DeleteStateOption{},
	}

	deleteErr := pgs.Delete(deleteReq)
	assert.Nil(t, deleteErr)
	assert.False(t, storeItemExists(t, key))
}

func storeItemExists(t *testing.T, key string) bool {
	db, err := sql.Open("pgx", getConnectionString())
	assert.Nil(t, err)
	defer db.Close()
	
	var exists bool = false
	statement := fmt.Sprintf(`SELECT EXISTS (SELECT FROM %s WHERE key = $1)`, tableName)
	err = db.QueryRow(statement, key).Scan(&exists)
	assert.Nil(t, err)
	return exists
}

func getRowData(t *testing.T, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString) {
	db, err := sql.Open("pgx", getConnectionString())
	assert.Nil(t, err)
	defer db.Close()

	err = db.QueryRow(fmt.Sprintf("SELECT value, insertdate, updatedate FROM %s WHERE key = $1", tableName), key).Scan(&returnValue, &insertdate, &updatedate)
	return returnValue, insertdate, updatedate
}
