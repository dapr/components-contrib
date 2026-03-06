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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/authentication/sqlserver"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	sampleConnectionString = "server=localhost;user id=sa;password=Pass@Word1;port=1433;database=sample;"
	sampleUserTableName    = "Users"
	defaultDatabase        = "dapr"
	defaultSchema          = "dbo"
)

type mockMigrator struct{}

func (m *mockMigrator) executeMigrations(context.Context) (migrationResult, error) {
	r := migrationResult{}

	return r, nil
}

type mockFailingMigrator struct{}

func (m *mockFailingMigrator) executeMigrations(context.Context) (migrationResult, error) {
	r := migrationResult{}

	return r, errors.New("migration failed")
}

func TestValidConfiguration(t *testing.T) {
	tests := map[string]struct {
		props    map[string]string
		expected SQLServer
	}{
		"No schema": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         sampleUserTableName,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   defaultKeyLength,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Custom schema": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "schema": "mytest"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       "mytest",
					},
					TableName:         sampleUserTableName,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   defaultKeyLength,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"String key type": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "keyType": "string"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         sampleUserTableName,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   defaultKeyLength,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Unique identifier key type": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "keyType": "uuid"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         sampleUserTableName,
					keyTypeParsed:     UUIDKeyType,
					keyLengthParsed:   0,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Integer identifier key type": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "keyType": "integer"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         sampleUserTableName,
					keyTypeParsed:     IntegerKeyType,
					keyLengthParsed:   0,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Custom key length": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "keyLength": "100"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         sampleUserTableName,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   100,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Single indexed property": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "indexedProperties": `[{"column": "Age","property":"age", "type":"int"}]`},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:       sampleUserTableName,
					keyTypeParsed:   StringKeyType,
					keyLengthParsed: defaultKeyLength,
					indexedPropertiesParsed: []IndexedProperty{
						{ColumnName: "Age", Property: "age", Type: "int"},
					},
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Multiple indexed properties": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "indexedProperties": `[{"column": "Age","property":"age", "type":"int"}, {"column": "Name","property":"name", "type":"nvarchar(100)"}]`},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:       sampleUserTableName,
					keyTypeParsed:   StringKeyType,
					keyLengthParsed: defaultKeyLength,
					indexedPropertiesParsed: []IndexedProperty{
						{ColumnName: "Age", Property: "age", Type: "int"},
						{ColumnName: "Name", Property: "name", Type: "nvarchar(100)"},
					},
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Custom database": {
			props: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "databaseName": "dapr_test_table"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     "dapr_test_table",
						SchemaName:       defaultSchema,
					},
					TableName:         sampleUserTableName,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   defaultKeyLength,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"No table": {
			props: map[string]string{"connectionString": sampleConnectionString},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         defaultTable,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   defaultKeyLength,
					MetadataTableName: defaultMetaTable,
				},
			},
		},
		"Custom meta table": {
			props: map[string]string{"connectionString": sampleConnectionString, "metadataTableName": "dapr_test_meta_table"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         defaultTable,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   defaultKeyLength,
					MetadataTableName: "dapr_test_meta_table",
				},
			},
		},
		"Actor state store true": {
			props: map[string]string{"connectionString": sampleConnectionString, "metadataTableName": "dapr_test_meta_table", "actorStateStore": "true"},
			expected: SQLServer{
				metadata: sqlServerMetadata{
					SQLServerAuthMetadata: sqlserver.SQLServerAuthMetadata{
						ConnectionString: sampleConnectionString,
						DatabaseName:     defaultDatabase,
						SchemaName:       defaultSchema,
					},
					TableName:         defaultTable,
					keyTypeParsed:     StringKeyType,
					keyLengthParsed:   defaultKeyLength,
					MetadataTableName: "dapr_test_meta_table",
				},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			sqlStore := &SQLServer{
				logger: logger.NewLogger("test"),
				migratorFactory: func(*sqlServerMetadata) migrator {
					return &mockMigrator{}
				},
			}

			metadata := state.Metadata{
				Base: metadata.Base{Properties: tt.props},
			}

			err := sqlStore.Init(t.Context(), metadata)
			require.NoError(t, err)
			assert.Equal(t, tt.expected.metadata.ConnectionString, sqlStore.metadata.ConnectionString)
			assert.Equal(t, tt.expected.metadata.TableName, sqlStore.metadata.TableName)
			assert.Equal(t, tt.expected.metadata.SchemaName, sqlStore.metadata.SchemaName)
			assert.Equal(t, tt.expected.metadata.keyTypeParsed, sqlStore.metadata.keyTypeParsed)
			assert.Equal(t, tt.expected.metadata.keyLengthParsed, sqlStore.metadata.keyLengthParsed)
			assert.Equal(t, tt.expected.metadata.DatabaseName, sqlStore.metadata.DatabaseName)
			assert.Equal(t, tt.expected.metadata.MetadataTableName, sqlStore.metadata.MetadataTableName)

			assert.Equal(t, len(tt.expected.metadata.indexedPropertiesParsed), len(sqlStore.metadata.indexedPropertiesParsed))
			if len(tt.expected.metadata.indexedPropertiesParsed) > 0 && len(tt.expected.metadata.indexedPropertiesParsed) == len(sqlStore.metadata.indexedPropertiesParsed) {
				for i, e := range tt.expected.metadata.indexedPropertiesParsed {
					assert.Equal(t, e.ColumnName, sqlStore.metadata.indexedPropertiesParsed[i].ColumnName)
					assert.Equal(t, e.Property, sqlStore.metadata.indexedPropertiesParsed[i].Property)
					assert.Equal(t, e.Type, sqlStore.metadata.indexedPropertiesParsed[i].Type)
				}
			}
		})
	}
}

func TestInvalidConfiguration(t *testing.T) {
	tests := map[string]struct {
		props       map[string]string
		expectedErr string
	}{
		"Empty": {
			props:       map[string]string{},
			expectedErr: "missing connection string",
		},
		"Empty connection string": {
			props:       map[string]string{"connectionString": ""},
			expectedErr: "missing connection string",
		},
		"Negative maxKeyLength value": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "keyLength": "-1"},
			expectedErr: "invalid key length value of -1",
		},
		"Indexes properties are not valid json": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": "no_json"},
			expectedErr: "invalid character",
		},
		"Invalid table name with ;": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test;"},
			expectedErr: "invalid table name",
		},
		"Invalid table name with space": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test GO DROP DATABASE dapr_test"},
			expectedErr: "invalid table name",
		},
		"Invalid metadata table name with ;": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "metadataTableName": "test;"},
			expectedErr: "invalid metadata table name",
		},
		"Invalid metadata table name with space": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "metadataTableName": "test GO DROP DATABASE dapr_test"},
			expectedErr: "invalid metadata table name",
		},
		"Invalid schema name with ;": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "schema": "test;"},
			expectedErr: "invalid schema name",
		},
		"Invalid schema name with space": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "schema": "test GO DROP DATABASE dapr_test"},
			expectedErr: "invalid schema name",
		},
		"Invalid index property column name with ;": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"test;", "property": "age", "type": "INT"}]`},
			expectedErr: "invalid indexed property column name",
		},
		"Invalid index property column name with space": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"test GO DROP DATABASE dapr_test", "property": "age", "type": "INT"}]`},
			expectedErr: "invalid indexed property column name",
		},
		"Invalid index property name with ;": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"age", "property": "test;", "type": "INT"}]`},
			expectedErr: "invalid indexed property name",
		},
		"Invalid index property name with space": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"age", "property": "test GO DROP DATABASE dapr_test", "type": "INT"}]`},
			expectedErr: "invalid indexed property name",
		},
		"Invalid index property type with ;": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"age", "property": "age", "type": "INT;"}]`},
			expectedErr: "invalid indexed property type",
		},
		"Invalid index property type with space": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"age", "property": "age", "type": "INT GO DROP DATABASE dapr_test"}]`},
			expectedErr: "invalid indexed property type",
		},
		"Index property column cannot be empty": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"", "property": "age", "type": "INT"}]`},
			expectedErr: "indexed property column cannot be empty",
		},
		"Invalid property name cannot be empty": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"age", "property": "", "type": "INT"}]`},
			expectedErr: "indexed property name cannot be empty",
		},
		"Invalid property type cannot be empty": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "indexedProperties": `[{"column":"age", "property": "age", "type": ""}]`},
			expectedErr: "indexed property type cannot be empty",
		},
		"Invalid database name with ;": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "databaseName": "test;"},
			expectedErr: "invalid database name",
		},
		"Invalid database name with space": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "databaseName": "test GO DROP DATABASE dapr_test"},
			expectedErr: "invalid database name",
		},
		"Invalid key type invalid": {
			props:       map[string]string{"connectionString": sampleConnectionString, "tableName": "test", "keyType": "invalid"},
			expectedErr: "invalid key type",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			sqlStore := &SQLServer{
				logger: logger.NewLogger("test"),
			}

			metadata := state.Metadata{
				Base: metadata.Base{Properties: tt.props},
			}

			err := sqlStore.Init(t.Context(), metadata)
			require.Error(t, err)

			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func TestCleanupInterval(t *testing.T) {
	t.Run("cleanupInterval not set", func(t *testing.T) {
		properties := map[string]string{
			"url": "test",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		assert.Equal(t, "test", md.ConnectionString)
		require.NotNil(t, md.CleanupInterval)
		assert.Equal(t, defaultCleanupInterval, *md.CleanupInterval)
	})

	t.Run("cleanupInterval as Go duration", func(t *testing.T) {
		properties := map[string]string{
			"connectionString": "test",
			"cleanupInterval":  "1m",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		assert.Equal(t, "test", md.ConnectionString)
		require.NotNil(t, md.CleanupInterval)
		assert.Equal(t, time.Minute, *md.CleanupInterval)
	})

	t.Run("cleanupInterval as seconds", func(t *testing.T) {
		properties := map[string]string{
			"connectionString": "test",
			"cleanupInterval":  "10",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		assert.Equal(t, "test", md.ConnectionString)
		require.NotNil(t, md.CleanupInterval)
		assert.Equal(t, 10*time.Second, *md.CleanupInterval)
	})

	t.Run("cleanupIntervalInSeconds as Go duration", func(t *testing.T) {
		properties := map[string]string{
			"connectionString":         "test",
			"cleanupIntervalInSeconds": "1m",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		require.NotNil(t, md.CleanupInterval)
		assert.Equal(t, time.Minute, *md.CleanupInterval)
	})

	t.Run("cleanupIntervalInSeconds as seconds", func(t *testing.T) {
		properties := map[string]string{
			"connectionString":         "test",
			"cleanupIntervalInSeconds": "10",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		require.NotNil(t, md.CleanupInterval)
		assert.Equal(t, 10*time.Second, *md.CleanupInterval)
	})

	t.Run("cleanupInterval as 0", func(t *testing.T) {
		properties := map[string]string{
			"connectionString": "test",
			"cleanupInterval":  "0",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		require.Nil(t, md.CleanupInterval)
	})

	t.Run("cleanupIntervallInSeconds as 0", func(t *testing.T) {
		properties := map[string]string{
			"connectionString":         "test",
			"cleanupIntervalInSeconds": "0",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		require.Nil(t, md.CleanupInterval)
	})

	t.Run("cleanupInterval negative", func(t *testing.T) {
		properties := map[string]string{
			"connectionString": "test",
			"cleanupInterval":  "-1",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		require.Nil(t, md.CleanupInterval)
	})

	t.Run("cleanupIntervallInSeconds negative", func(t *testing.T) {
		properties := map[string]string{
			"connectionString":         "test",
			"cleanupIntervalInSeconds": "-1",
		}

		md := newMetadata()
		err := md.Parse(properties)
		require.NoError(t, err)
		require.Nil(t, md.CleanupInterval)
	})
}

// Test that if the migration fails the error is reported.
func TestExecuteMigrationFails(t *testing.T) {
	sqlStore := &SQLServer{
		logger: logger.NewLogger("test"),
		migratorFactory: func(*sqlServerMetadata) migrator {
			return &mockFailingMigrator{}
		},
	}

	metadata := state.Metadata{
		Base: metadata.Base{Properties: map[string]string{"connectionString": sampleConnectionString, "tableName": sampleUserTableName, "databaseName": "dapr_test_table"}},
	}

	err := sqlStore.Init(t.Context(), metadata)
	require.Error(t, err)
}

func TestSupportedFeatures(t *testing.T) {
	sqlStore := &SQLServer{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger.NewLogger("test"),
	}

	actual := sqlStore.Features()
	assert.NotNil(t, actual)
	assert.Equal(t, state.FeatureETag, actual[0])
	assert.Equal(t, state.FeatureTransactional, actual[1])
}
