//go:build conftests
// +build conftests

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

package conformance

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	s_awsdynamodb "github.com/dapr/components-contrib/state/aws/dynamodb"
	s_blobstorage_v1 "github.com/dapr/components-contrib/state/azure/blobstorage/v1"
	s_blobstorage_v2 "github.com/dapr/components-contrib/state/azure/blobstorage/v2"
	s_cosmosdb "github.com/dapr/components-contrib/state/azure/cosmosdb"
	s_azuretablestorage "github.com/dapr/components-contrib/state/azure/tablestorage"
	s_cassandra "github.com/dapr/components-contrib/state/cassandra"
	s_cloudflareworkerskv "github.com/dapr/components-contrib/state/cloudflare/workerskv"
	s_cockroachdb_v1 "github.com/dapr/components-contrib/state/cockroachdb"
	s_etcd "github.com/dapr/components-contrib/state/etcd"
	s_gcpfirestore "github.com/dapr/components-contrib/state/gcp/firestore"
	s_inmemory "github.com/dapr/components-contrib/state/in-memory"
	s_memcached "github.com/dapr/components-contrib/state/memcached"
	s_mongodb "github.com/dapr/components-contrib/state/mongodb"
	s_mysql "github.com/dapr/components-contrib/state/mysql"
	s_oracledatabase "github.com/dapr/components-contrib/state/oracledatabase"
	s_postgresql_v1 "github.com/dapr/components-contrib/state/postgresql/v1"
	s_postgresql_v2 "github.com/dapr/components-contrib/state/postgresql/v2"
	s_redis "github.com/dapr/components-contrib/state/redis"
	s_rethinkdb "github.com/dapr/components-contrib/state/rethinkdb"
	s_sqlite "github.com/dapr/components-contrib/state/sqlite"
	s_sqlserver "github.com/dapr/components-contrib/state/sqlserver"
	conf_state "github.com/dapr/components-contrib/tests/conformance/state"
)

func TestStateConformance(t *testing.T) {
	const configPath = "../config/state/"
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			store := loadStateStore(comp.Component)
			require.NotNilf(t, store, "error running conformance test for component %s", comp.Component)

			storeConfig, err := conf_state.NewTestConfig(comp.Component, comp.Operations, comp.Config)
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			conf_state.ConformanceTests(t, props, store, storeConfig)
		}
	}

	tc.Run(t)
}

func loadStateStore(name string) state.Store {
	switch name {
	case "redis.v6":
		return s_redis.NewRedisStateStore(testLogger)
	case "redis.v7":
		return s_redis.NewRedisStateStore(testLogger)
	case "azure.blobstorage.v1":
		return s_blobstorage_v1.NewAzureBlobStorageStore(testLogger)
	case "azure.blobstorage.v2":
		return s_blobstorage_v2.NewAzureBlobStorageStore(testLogger)
	case "azure.cosmosdb":
		return s_cosmosdb.NewCosmosDBStateStore(testLogger)
	case "mongodb":
		return s_mongodb.NewMongoDB(testLogger)
	case "azure.sql":
		return s_sqlserver.New(testLogger)
	case "sqlserver":
		return s_sqlserver.New(testLogger)
	case "postgresql.v1.docker":
		return s_postgresql_v1.NewPostgreSQLStateStore(testLogger)
	case "postgresql.v1.azure":
		return s_postgresql_v1.NewPostgreSQLStateStore(testLogger)
	case "postgresql.v2.docker":
		return s_postgresql_v2.NewPostgreSQLStateStore(testLogger)
	case "postgresql.v2.azure":
		return s_postgresql_v2.NewPostgreSQLStateStore(testLogger)
	case "sqlite":
		return s_sqlite.NewSQLiteStateStore(testLogger)
	case "mysql.mysql":
		return s_mysql.NewMySQLStateStore(testLogger)
	case "mysql.mariadb":
		return s_mysql.NewMySQLStateStore(testLogger)
	case "oracledatabase":
		return s_oracledatabase.NewOracleDatabaseStateStore(testLogger)
	case "azure.tablestorage.storage":
		return s_azuretablestorage.NewAzureTablesStateStore(testLogger)
	case "azure.tablestorage.cosmosdb":
		return s_azuretablestorage.NewAzureTablesStateStore(testLogger)
	case "cassandra":
		return s_cassandra.NewCassandraStateStore(testLogger)
	case "cloudflare.workerskv":
		return s_cloudflareworkerskv.NewCFWorkersKV(testLogger)
	case "cockroachdb.v1":
		return s_cockroachdb_v1.New(testLogger)
	case "cockroachdb.v2":
		// v2 of the component is an alias for the PostgreSQL state store
		// We still have a conformance test to validate that the component works with CockroachDB
		return s_postgresql_v2.NewPostgreSQLStateStoreWithOptions(testLogger, s_postgresql_v2.Options{NoAzureAD: true, NoAWSIAM: true})
	case "memcached":
		return s_memcached.NewMemCacheStateStore(testLogger)
	case "rethinkdb":
		return s_rethinkdb.NewRethinkDBStateStore(testLogger)
	case "in-memory":
		return s_inmemory.NewInMemoryStateStore(testLogger)
	case "aws.dynamodb.docker":
		return s_awsdynamodb.NewDynamoDBStateStore(testLogger)
	case "aws.dynamodb.terraform":
		return s_awsdynamodb.NewDynamoDBStateStore(testLogger)
	case "etcd.v1":
		return s_etcd.NewEtcdStateStoreV1(testLogger)
	case "etcd.v2":
		return s_etcd.NewEtcdStateStoreV2(testLogger)
	case "gcp.firestore.docker":
		return s_gcpfirestore.NewFirestoreStateStore(testLogger)
	case "gcp.firestore.cloud":
		return s_gcpfirestore.NewFirestoreStateStore(testLogger)
	default:
		return nil
	}
}
