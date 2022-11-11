/*
Copyright 2021 The Dapr Authors
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
package sqlserver

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	sampleConnectionString = "server=localhost;user id=sa;password=Pass@Word1;port=1433;database=sample;"
	sampleUserTableName    = "Users"
)

type mockMigrator struct{}

func (m *mockMigrator) executeMigrations() (migrationResult, error) {
	r := migrationResult{}

	return r, nil
}

type mockFailingMigrator struct{}

func (m *mockFailingMigrator) executeMigrations() (migrationResult, error) {
	r := migrationResult{}

	return r, errors.New("migration failed")
}

func TestValidConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		props    map[string]string
		expected SQLServer
	}{
		{
			name:  "No schema",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				tableName:        sampleUserTableName,
				schema:           defaultSchema,
				keyType:          StringKeyType,
				keyLength:        defaultKeyLength,
				databaseName:     defaultDatabase,
			},
		},
		{
			name:  "Custom schema",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, schemaKey: "mytest"},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				tableName:        sampleUserTableName,
				schema:           "mytest",
				keyType:          StringKeyType,
				keyLength:        defaultKeyLength,
				databaseName:     defaultDatabase,
			},
		},
		{
			name:  "String key type",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, keyTypeKey: "string"},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				schema:           defaultSchema,
				tableName:        sampleUserTableName,
				keyType:          StringKeyType,
				keyLength:        defaultKeyLength,
				databaseName:     defaultDatabase,
			},
		},
		{
			name:  "Unique identifier key type",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, keyTypeKey: "uuid"},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				schema:           defaultSchema,
				tableName:        sampleUserTableName,
				keyType:          UUIDKeyType,
				keyLength:        0,
				databaseName:     defaultDatabase,
			},
		},
		{
			name:  "Integer identifier key type",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, keyTypeKey: "integer"},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				schema:           defaultSchema,
				tableName:        sampleUserTableName,
				keyType:          IntegerKeyType,
				keyLength:        0,
				databaseName:     defaultDatabase,
			},
		},
		{
			name:  "Custom key length",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, keyLengthKey: "100"},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				schema:           defaultSchema,
				tableName:        sampleUserTableName,
				keyType:          StringKeyType,
				keyLength:        100,
				databaseName:     defaultDatabase,
			},
		},
		{
			name:  "Single indexed property",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, indexedPropertiesKey: `[{"column": "Age","property":"age", "type":"int"}]`},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				schema:           defaultSchema,
				tableName:        sampleUserTableName,
				keyType:          StringKeyType,
				keyLength:        defaultKeyLength,
				indexedProperties: []IndexedProperty{
					{ColumnName: "Age", Property: "age", Type: "int"},
				},
				databaseName: defaultDatabase,
			},
		},
		{
			name:  "Multiple indexed properties",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, indexedPropertiesKey: `[{"column": "Age","property":"age", "type":"int"}, {"column": "Name","property":"name", "type":"nvarchar(100)"}]`},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				schema:           defaultSchema,
				tableName:        sampleUserTableName,
				keyType:          StringKeyType,
				keyLength:        defaultKeyLength,
				indexedProperties: []IndexedProperty{
					{ColumnName: "Age", Property: "age", Type: "int"},
					{ColumnName: "Name", Property: "name", Type: "nvarchar(100)"},
				},
				databaseName: defaultDatabase,
			},
		},
		{
			name:  "Custom database",
			props: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, databaseNameKey: "dapr_test_table"},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				schema:           defaultSchema,
				tableName:        sampleUserTableName,
				keyType:          StringKeyType,
				keyLength:        defaultKeyLength,
				databaseName:     "dapr_test_table",
			},
		},
		{
			name:  "No table",
			props: map[string]string{connectionStringKey: sampleConnectionString},
			expected: SQLServer{
				connectionString: sampleConnectionString,
				tableName:        defaultTable,
				schema:           defaultSchema,
				keyType:          StringKeyType,
				keyLength:        defaultKeyLength,
				databaseName:     defaultDatabase,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlStore := NewSQLServerStateStore(logger.NewLogger("test")).(*SQLServer)
			sqlStore.migratorFactory = func(s *SQLServer) migrator {
				return &mockMigrator{}
			}

			metadata := state.Metadata{
				Base: metadata.Base{Properties: tt.props},
			}

			err := sqlStore.Init(metadata)
			assert.Nil(t, err)
			assert.Equal(t, tt.expected.connectionString, sqlStore.connectionString)
			assert.Equal(t, tt.expected.tableName, sqlStore.tableName)
			assert.Equal(t, tt.expected.schema, sqlStore.schema)
			assert.Equal(t, tt.expected.keyType, sqlStore.keyType)
			assert.Equal(t, tt.expected.keyLength, sqlStore.keyLength)
			assert.Equal(t, tt.expected.databaseName, sqlStore.databaseName)

			assert.Equal(t, len(tt.expected.indexedProperties), len(sqlStore.indexedProperties))
			if len(tt.expected.indexedProperties) > 0 && len(tt.expected.indexedProperties) == len(sqlStore.indexedProperties) {
				for i, e := range tt.expected.indexedProperties {
					assert.Equal(t, e.ColumnName, sqlStore.indexedProperties[i].ColumnName)
					assert.Equal(t, e.Property, sqlStore.indexedProperties[i].Property)
					assert.Equal(t, e.Type, sqlStore.indexedProperties[i].Type)
				}
			}
		})
	}
}

func TestInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		props       map[string]string
		expectedErr string
	}{
		{
			name:        "Empty",
			props:       map[string]string{},
			expectedErr: "missing connection string",
		},
		{
			name:        "Empty connection string",
			props:       map[string]string{connectionStringKey: ""},
			expectedErr: "missing connection string",
		},
		{
			name:        "Negative maxKeyLength value",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", keyLengthKey: "-1"},
			expectedErr: "invalid key length value of -1",
		},
		{
			name:        "Indexes properties are not valid json",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: "no_json"},
			expectedErr: "invalid character",
		},
		{
			name:        "Invalid table name with ;",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test;"},
			expectedErr: "invalid table name",
		},
		{
			name:        "Invalid table name with space",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test GO DROP DATABASE dapr_test"},
			expectedErr: "invalid table name",
		},
		{
			name:        "Invalid schema name with ;",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", schemaKey: "test;"},
			expectedErr: "invalid schema name",
		},
		{
			name:        "Invalid schema name with space",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", schemaKey: "test GO DROP DATABASE dapr_test"},
			expectedErr: "invalid schema name",
		},
		{
			name:        "Invalid index property column name with ;",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"test;", "property": "age", "type": "INT"}]`},
			expectedErr: "invalid indexed property column name",
		},
		{
			name:        "Invalid index property column name with space",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"test GO DROP DATABASE dapr_test", "property": "age", "type": "INT"}]`},
			expectedErr: "invalid indexed property column name",
		},
		{
			name:        "Invalid index property name with ;",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"age", "property": "test;", "type": "INT"}]`},
			expectedErr: "invalid indexed property name",
		},
		{
			name:        "Invalid index property name with space",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"age", "property": "test GO DROP DATABASE dapr_test", "type": "INT"}]`},
			expectedErr: "invalid indexed property name",
		},
		{
			name:        "Invalid index property type with ;",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"age", "property": "age", "type": "INT;"}]`},
			expectedErr: "invalid indexed property type",
		},
		{
			name:        "Invalid index property type with space",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"age", "property": "age", "type": "INT GO DROP DATABASE dapr_test"}]`},
			expectedErr: "invalid indexed property type",
		},
		{
			name:        "Index property column cannot be empty",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"", "property": "age", "type": "INT"}]`},
			expectedErr: "indexed property column cannot be empty",
		},
		{
			name:        "Invalid property name cannot be empty",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"age", "property": "", "type": "INT"}]`},
			expectedErr: "indexed property name cannot be empty",
		},
		{
			name:        "Invalid property type cannot be empty",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", indexedPropertiesKey: `[{"column":"age", "property": "age", "type": ""}]`},
			expectedErr: "indexed property type cannot be empty",
		},
		{
			name:        "Invalid database name with ;",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", databaseNameKey: "test;"},
			expectedErr: "invalid database name",
		},
		{
			name:        "Invalid database name with space",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", databaseNameKey: "test GO DROP DATABASE dapr_test"},
			expectedErr: "invalid database name",
		},
		{
			name:        "Invalid key type invalid",
			props:       map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: "test", keyTypeKey: "invalid"},
			expectedErr: "invalid key type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlStore := NewSQLServerStateStore(logger.NewLogger("test")).(*SQLServer)

			metadata := state.Metadata{
				Base: metadata.Base{Properties: tt.props},
			}

			err := sqlStore.Init(metadata)
			assert.NotNil(t, err)

			if tt.expectedErr != "" {
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

// Test that if the migration fails the error is reported.
func TestExecuteMigrationFails(t *testing.T) {
	sqlStore := NewSQLServerStateStore(logger.NewLogger("test")).(*SQLServer)
	sqlStore.migratorFactory = func(s *SQLServer) migrator {
		return &mockFailingMigrator{}
	}

	metadata := state.Metadata{
		Base: metadata.Base{Properties: map[string]string{connectionStringKey: sampleConnectionString, tableNameKey: sampleUserTableName, databaseNameKey: "dapr_test_table"}},
	}

	err := sqlStore.Init(metadata)
	assert.NotNil(t, err)
}

func TestSupportedFeatures(t *testing.T) {
	sqlStore := NewSQLServerStateStore(logger.NewLogger("test")).(*SQLServer)

	actual := sqlStore.Features()
	assert.NotNil(t, actual)
	assert.Equal(t, state.FeatureETag, actual[0])
	assert.Equal(t, state.FeatureTransactional, actual[1])
}
