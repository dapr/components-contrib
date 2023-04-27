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

//nolint:nosnakecase
package conformance

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/configuration"
	contribCrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/workflows"
	"github.com/dapr/kit/logger"

	b_aws_s3 "github.com/dapr/components-contrib/bindings/aws/s3"
	b_azure_blobstorage "github.com/dapr/components-contrib/bindings/azure/blobstorage"
	b_azure_cosmosdb "github.com/dapr/components-contrib/bindings/azure/cosmosdb"
	b_azure_eventgrid "github.com/dapr/components-contrib/bindings/azure/eventgrid"
	b_azure_eventhubs "github.com/dapr/components-contrib/bindings/azure/eventhubs"
	b_azure_servicebusqueues "github.com/dapr/components-contrib/bindings/azure/servicebusqueues"
	b_azure_storagequeues "github.com/dapr/components-contrib/bindings/azure/storagequeues"
	b_cron "github.com/dapr/components-contrib/bindings/cron"
	b_http "github.com/dapr/components-contrib/bindings/http"
	b_influx "github.com/dapr/components-contrib/bindings/influx"
	b_kafka "github.com/dapr/components-contrib/bindings/kafka"
	b_kubemq "github.com/dapr/components-contrib/bindings/kubemq"
	b_mqtt3 "github.com/dapr/components-contrib/bindings/mqtt3"
	b_postgres "github.com/dapr/components-contrib/bindings/postgres"
	b_rabbitmq "github.com/dapr/components-contrib/bindings/rabbitmq"
	b_redis "github.com/dapr/components-contrib/bindings/redis"
	c_postgres "github.com/dapr/components-contrib/configuration/postgres"
	c_redis "github.com/dapr/components-contrib/configuration/redis"
	cr_azurekeyvault "github.com/dapr/components-contrib/crypto/azure/keyvault"
	cr_jwks "github.com/dapr/components-contrib/crypto/jwks"
	cr_localstorage "github.com/dapr/components-contrib/crypto/localstorage"
	p_snssqs "github.com/dapr/components-contrib/pubsub/aws/snssqs"
	p_eventhubs "github.com/dapr/components-contrib/pubsub/azure/eventhubs"
	p_servicebusqueues "github.com/dapr/components-contrib/pubsub/azure/servicebus/queues"
	p_servicebustopics "github.com/dapr/components-contrib/pubsub/azure/servicebus/topics"
	p_gcppubsub "github.com/dapr/components-contrib/pubsub/gcp/pubsub"
	p_inmemory "github.com/dapr/components-contrib/pubsub/in-memory"
	p_jetstream "github.com/dapr/components-contrib/pubsub/jetstream"
	p_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	p_kubemq "github.com/dapr/components-contrib/pubsub/kubemq"
	p_mqtt3 "github.com/dapr/components-contrib/pubsub/mqtt3"
	p_natsstreaming "github.com/dapr/components-contrib/pubsub/natsstreaming"
	p_pulsar "github.com/dapr/components-contrib/pubsub/pulsar"
	p_rabbitmq "github.com/dapr/components-contrib/pubsub/rabbitmq"
	p_redis "github.com/dapr/components-contrib/pubsub/redis"
	p_solaceamqp "github.com/dapr/components-contrib/pubsub/solace/amqp"
	ss_azure "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	ss_hashicorp_vault "github.com/dapr/components-contrib/secretstores/hashicorp/vault"
	ss_kubernetes "github.com/dapr/components-contrib/secretstores/kubernetes"
	ss_local_env "github.com/dapr/components-contrib/secretstores/local/env"
	ss_local_file "github.com/dapr/components-contrib/secretstores/local/file"
	s_awsdynamodb "github.com/dapr/components-contrib/state/aws/dynamodb"
	s_blobstorage "github.com/dapr/components-contrib/state/azure/blobstorage"
	s_cosmosdb "github.com/dapr/components-contrib/state/azure/cosmosdb"
	s_azuretablestorage "github.com/dapr/components-contrib/state/azure/tablestorage"
	s_cassandra "github.com/dapr/components-contrib/state/cassandra"
	s_cloudflareworkerskv "github.com/dapr/components-contrib/state/cloudflare/workerskv"
	s_cockroachdb "github.com/dapr/components-contrib/state/cockroachdb"
	s_etcd "github.com/dapr/components-contrib/state/etcd"
	s_gcpfirestore "github.com/dapr/components-contrib/state/gcp/firestore"
	s_inmemory "github.com/dapr/components-contrib/state/in-memory"
	s_memcached "github.com/dapr/components-contrib/state/memcached"
	s_mongodb "github.com/dapr/components-contrib/state/mongodb"
	s_mysql "github.com/dapr/components-contrib/state/mysql"
	s_oracledatabase "github.com/dapr/components-contrib/state/oracledatabase"
	s_postgresql "github.com/dapr/components-contrib/state/postgresql"
	s_redis "github.com/dapr/components-contrib/state/redis"
	s_rethinkdb "github.com/dapr/components-contrib/state/rethinkdb"
	s_sqlite "github.com/dapr/components-contrib/state/sqlite"
	s_sqlserver "github.com/dapr/components-contrib/state/sqlserver"
	conf_bindings "github.com/dapr/components-contrib/tests/conformance/bindings"
	conf_configuration "github.com/dapr/components-contrib/tests/conformance/configuration"
	conf_crypto "github.com/dapr/components-contrib/tests/conformance/crypto"
	conf_pubsub "github.com/dapr/components-contrib/tests/conformance/pubsub"
	conf_secret "github.com/dapr/components-contrib/tests/conformance/secretstores"
	conf_state "github.com/dapr/components-contrib/tests/conformance/state"
	conf_workflows "github.com/dapr/components-contrib/tests/conformance/workflows"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	cu_postgres "github.com/dapr/components-contrib/tests/utils/configupdater/postgres"
	cu_redis "github.com/dapr/components-contrib/tests/utils/configupdater/redis"
	wf_temporal "github.com/dapr/components-contrib/workflows/temporal"
)

const (
	eventhubs                 = "azure.eventhubs"
	redisv6                   = "redis.v6"
	redisv7                   = "redis.v7"
	postgres                  = "postgres"
	kafka                     = "kafka"
	generateUUID              = "$((uuid))"
	generateEd25519PrivateKey = "$((ed25519PrivateKey))"
)

//nolint:gochecknoglobals
var testLogger logger.Logger

func init() {
	testLogger = logger.NewLogger("testLogger")
	testLogger.SetOutputLevel(logger.DebugLevel)
}

type TestConfiguration struct {
	ComponentType string          `yaml:"componentType,omitempty"`
	Components    []TestComponent `yaml:"components,omitempty"`
}

type TestComponent struct {
	Component     string                 `yaml:"component,omitempty"`
	Profile       string                 `yaml:"profile,omitempty"`
	AllOperations bool                   `yaml:"allOperations,omitempty"`
	Operations    []string               `yaml:"operations,omitempty"`
	Config        map[string]interface{} `yaml:"config,omitempty"`
}

// NewTestConfiguration reads the tests.yml and loads the TestConfiguration.
func NewTestConfiguration(configFilepath string) (*TestConfiguration, error) {
	if isYaml(configFilepath) {
		b, err := readTestConfiguration(configFilepath)
		if err != nil {
			log.Printf("error reading file %s : %s", configFilepath, err)

			return nil, err
		}
		tc, err := decodeYaml(b)

		return &tc, err
	}

	return nil, errors.New("no test configuration file tests.yml found")
}

func LoadComponents(componentPath string) ([]Component, error) {
	standaloneComps := NewStandaloneComponents(componentPath)
	components, err := standaloneComps.LoadComponents()
	if err != nil {
		return nil, err
	}

	return components, nil
}

// LookUpEnv returns the value of the specified environment variable or the empty string.
func LookUpEnv(key string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}

	return ""
}

func ParseConfigurationMap(t *testing.T, configMap map[string]interface{}) {
	for k, v := range configMap {
		switch val := v.(type) {
		case string:
			if strings.EqualFold(val, generateUUID) {
				// check if generate uuid is specified
				val = uuid.New().String()
				t.Logf("Generated UUID %s", val)
				configMap[k] = val
			} else if strings.Contains(val, "${{") {
				s := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(val, "${{"), "}}"))
				v := LookUpEnv(s)
				configMap[k] = v
			} else {
				jsonMap := make(map[string]interface{})
				err := json.Unmarshal([]byte(val), &jsonMap)
				if err == nil {
					ParseConfigurationMap(t, jsonMap)
					mapBytes, err := json.Marshal(jsonMap)
					if err == nil {
						configMap[k] = string(mapBytes)
					}
				}
			}
		case map[string]interface{}:
			ParseConfigurationMap(t, val)
		case map[interface{}]interface{}:
			parseConfigurationInterfaceMap(t, val)
		}
	}
}

func parseConfigurationInterfaceMap(t *testing.T, configMap map[interface{}]interface{}) {
	for k, v := range configMap {
		switch val := v.(type) {
		case string:
			if strings.EqualFold(val, generateUUID) {
				// check if generate uuid is specified
				val = uuid.New().String()
				t.Logf("Generated UUID %s", val)
				configMap[k] = val
			} else if strings.Contains(val, "${{") {
				s := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(val, "${{"), "}}"))
				v := LookUpEnv(s)
				configMap[k] = v
			} else {
				jsonMap := make(map[string]interface{})
				err := json.Unmarshal([]byte(val), &jsonMap)
				if err == nil {
					ParseConfigurationMap(t, jsonMap)
					mapBytes, err := json.Marshal(jsonMap)
					if err == nil {
						configMap[k] = string(mapBytes)
					}
				}
			}
		case map[string]interface{}:
			ParseConfigurationMap(t, val)
		case map[interface{}]interface{}:
			parseConfigurationInterfaceMap(t, val)
		}
	}
}

func ConvertMetadataToProperties(items []MetadataItem) (map[string]string, error) {
	properties := map[string]string{}
	for _, c := range items {
		val, err := parseMetadataProperty(c.Value.String())
		if err != nil {
			return map[string]string{}, err
		}
		properties[c.Name] = val
	}

	return properties, nil
}

func parseMetadataProperty(val string) (string, error) {
	switch {
	case strings.HasPrefix(val, "${{"):
		// look up env var with that name. remove ${{}} and space
		k := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(val, "${{"), "}}"))
		v := LookUpEnv(k)
		if v == "" {
			return "", fmt.Errorf("required env var is not set %s", k)
		}
		return v, nil
		// Generate a random UUID
	case strings.EqualFold(val, generateUUID):
		val = uuid.New().String()
		return val, nil
	// Generate a random Ed25519 private key (PEM-encoded)
	case strings.EqualFold(val, generateEd25519PrivateKey):
		_, pk, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return "", fmt.Errorf("failed to generate Ed25519 private key: %w", err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(pk)
		if err != nil {
			return "", fmt.Errorf("failed to marshal Ed25519 private key to X.509: %w", err)
		}
		pemB := pem.EncodeToMemory(&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: der,
		})
		return string(pemB), nil
	default:
		return val, nil
	}
}

// isYaml checks whether the file is yaml or not.
func isYaml(fileName string) bool {
	extension := strings.ToLower(filepath.Ext(fileName))
	if extension == ".yaml" || extension == ".yml" {
		return true
	}

	return false
}

func readTestConfiguration(filePath string) ([]byte, error) {
	b, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s", filePath)
	}

	return b, nil
}

func decodeYaml(b []byte) (TestConfiguration, error) {
	var testConfig TestConfiguration
	err := yaml.Unmarshal(b, &testConfig)
	if err != nil {
		log.Printf("error parsing string as yaml %s", err)

		return TestConfiguration{}, err
	}

	return testConfig, nil
}

func (tc *TestConfiguration) loadComponentsAndProperties(t *testing.T, filepath string) (map[string]string, error) {
	comps, err := LoadComponents(filepath)
	require.NoError(t, err)
	require.Equal(t, 1, len(comps)) // We only expect a single component per file
	c := comps[0]
	props, err := ConvertMetadataToProperties(c.Spec.Metadata)

	return props, err
}

func convertComponentNameToPath(componentName, componentProfile string) string {
	pathName := componentName
	if strings.Contains(componentName, ".") {
		pathName = strings.Join(strings.Split(componentName, "."), "/")
	}

	if componentProfile != "" {
		pathName = path.Join(pathName, componentProfile)
	}

	return pathName
}

func (tc *TestConfiguration) Run(t *testing.T) {
	// Increase verbosity of tests to allow troubleshooting of runs.
	testLogger.SetOutputLevel(logger.DebugLevel)
	// For each component in the tests file run the conformance test
	for _, comp := range tc.Components {
		testName := comp.Component
		if comp.Profile != "" {
			testName += "-" + comp.Profile
		}
		t.Run(testName, func(t *testing.T) {
			// Parse and generate any keys
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			switch tc.ComponentType {
			case "state":
				filepath := fmt.Sprintf("../config/state/%s", componentConfigPath)
				props, err := tc.loadComponentsAndProperties(t, filepath)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				store := loadStateStore(comp)
				assert.NotNil(t, store)
				storeConfig := conf_state.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
				conf_state.ConformanceTests(t, props, store, storeConfig)
			case "secretstores":
				filepath := fmt.Sprintf("../config/secretstores/%s", componentConfigPath)
				props, err := tc.loadComponentsAndProperties(t, filepath)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				store := loadSecretStore(comp)
				assert.NotNil(t, store)
				storeConfig := conf_secret.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations)
				conf_secret.ConformanceTests(t, props, store, storeConfig)
			case "pubsub":
				filepath := fmt.Sprintf("../config/pubsub/%s", componentConfigPath)
				props, err := tc.loadComponentsAndProperties(t, filepath)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				pubsub := loadPubSub(comp)
				assert.NotNil(t, pubsub)
				pubsubConfig, err := conf_pubsub.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				conf_pubsub.ConformanceTests(t, props, pubsub, pubsubConfig)
			case "bindings":
				filepath := fmt.Sprintf("../config/bindings/%s", componentConfigPath)
				props, err := tc.loadComponentsAndProperties(t, filepath)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				inputBinding := loadInputBindings(comp)
				outputBinding := loadOutputBindings(comp)
				atLeastOne(t, func(item interface{}) bool {
					return item != nil
				}, inputBinding, outputBinding)
				bindingsConfig, err := conf_bindings.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				conf_bindings.ConformanceTests(t, props, inputBinding, outputBinding, bindingsConfig)
			case "workflows":
				filepath := fmt.Sprintf("../config/workflows/%s", componentConfigPath)
				props, err := tc.loadComponentsAndProperties(t, filepath)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				wf := loadWorkflow(comp)
				wfConfig := conf_workflows.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
				conf_workflows.ConformanceTests(t, props, wf, wfConfig)
			case "crypto":
				filepath := fmt.Sprintf("../config/crypto/%s", componentConfigPath)
				props, err := tc.loadComponentsAndProperties(t, filepath)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				component := loadCryptoProvider(comp)
				require.NotNil(t, component)
				cryptoConfig, err := conf_crypto.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				conf_crypto.ConformanceTests(t, props, component, cryptoConfig)
			case "configuration":
				filepath := fmt.Sprintf("../config/configuration/%s", componentConfigPath)
				props, err := tc.loadComponentsAndProperties(t, filepath)
				if err != nil {
					t.Errorf("error running conformance test for %s: %s", comp.Component, err)
					break
				}
				store, updater := loadConfigurationStore(comp)
				require.NotNil(t, store)
				require.NotNil(t, updater)
				configurationConfig := conf_configuration.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
				conf_configuration.ConformanceTests(t, props, store, updater, configurationConfig, comp.Component)
			default:
				t.Errorf("unknown component type %s", tc.ComponentType)
			}
		})
	}
}

func loadConfigurationStore(tc TestComponent) (configuration.Store, configupdater.Updater) {
	var store configuration.Store
	var updater configupdater.Updater
	switch tc.Component {
	case redisv6:
		store = c_redis.NewRedisConfigurationStore(testLogger)
		updater = cu_redis.NewRedisConfigUpdater(testLogger)
	case redisv7:
		store = c_redis.NewRedisConfigurationStore(testLogger)
		updater = cu_redis.NewRedisConfigUpdater(testLogger)
	case postgres:
		store = c_postgres.NewPostgresConfigurationStore(testLogger)
		updater = cu_postgres.NewPostgresConfigUpdater(testLogger)
	default:
		return nil, nil
	}
	return store, updater
}

func loadPubSub(tc TestComponent) pubsub.PubSub {
	var pubsub pubsub.PubSub
	switch tc.Component {
	case redisv6:
		pubsub = p_redis.NewRedisStreams(testLogger)
	case redisv7:
		pubsub = p_redis.NewRedisStreams(testLogger)
	case eventhubs:
		pubsub = p_eventhubs.NewAzureEventHubs(testLogger)
	case "azure.servicebus.topics":
		pubsub = p_servicebustopics.NewAzureServiceBusTopics(testLogger)
	case "azure.servicebus.queues":
		pubsub = p_servicebusqueues.NewAzureServiceBusQueues(testLogger)
	case "natsstreaming":
		pubsub = p_natsstreaming.NewNATSStreamingPubSub(testLogger)
	case "jetstream":
		pubsub = p_jetstream.NewJetStream(testLogger)
	case kafka:
		pubsub = p_kafka.NewKafka(testLogger)
	case "pulsar":
		pubsub = p_pulsar.NewPulsar(testLogger)
	case "mqtt3":
		pubsub = p_mqtt3.NewMQTTPubSub(testLogger)
	case "rabbitmq":
		pubsub = p_rabbitmq.NewRabbitMQ(testLogger)
	case "in-memory":
		pubsub = p_inmemory.New(testLogger)
	case "aws.snssqs.terraform":
		pubsub = p_snssqs.NewSnsSqs(testLogger)
	case "aws.snssqs.docker":
		pubsub = p_snssqs.NewSnsSqs(testLogger)
	case "gcp.pubsub.terraform":
		pubsub = p_gcppubsub.NewGCPPubSub(testLogger)
	case "gcp.pubsub.docker":
		pubsub = p_gcppubsub.NewGCPPubSub(testLogger)
	case "kubemq":
		pubsub = p_kubemq.NewKubeMQ(testLogger)
	case "solace.amqp":
		pubsub = p_solaceamqp.NewAMQPPubsub(testLogger)
	default:
		return nil
	}

	return pubsub
}

func loadSecretStore(tc TestComponent) secretstores.SecretStore {
	var store secretstores.SecretStore
	switch tc.Component {
	case "azure.keyvault.certificate":
		store = ss_azure.NewAzureKeyvaultSecretStore(testLogger)
	case "azure.keyvault.serviceprincipal":
		store = ss_azure.NewAzureKeyvaultSecretStore(testLogger)
	case "kubernetes":
		store = ss_kubernetes.NewKubernetesSecretStore(testLogger)
	case "local.env":
		store = ss_local_env.NewEnvSecretStore(testLogger)
	case "local.file":
		store = ss_local_file.NewLocalSecretStore(testLogger)
	case "hashicorp.vault":
		store = ss_hashicorp_vault.NewHashiCorpVaultSecretStore(testLogger)
	default:
		return nil
	}

	return store
}

func loadCryptoProvider(tc TestComponent) contribCrypto.SubtleCrypto {
	var component contribCrypto.SubtleCrypto
	switch tc.Component {
	case "azure.keyvault":
		component = cr_azurekeyvault.NewAzureKeyvaultCrypto(testLogger)
	case "localstorage":
		component = cr_localstorage.NewLocalStorageCrypto(testLogger)
	case "jwks":
		component = cr_jwks.NewJWKSCrypto(testLogger)
	}

	return component
}

func loadStateStore(tc TestComponent) state.Store {
	var store state.Store
	switch tc.Component {
	case redisv6:
		store = s_redis.NewRedisStateStore(testLogger)
	case redisv7:
		store = s_redis.NewRedisStateStore(testLogger)
	case "azure.blobstorage":
		store = s_blobstorage.NewAzureBlobStorageStore(testLogger)
	case "azure.cosmosdb":
		store = s_cosmosdb.NewCosmosDBStateStore(testLogger)
	case "mongodb":
		store = s_mongodb.NewMongoDB(testLogger)
	case "azure.sql":
		fallthrough
	case "sqlserver":
		store = s_sqlserver.New(testLogger)
	case "postgresql":
		store = s_postgresql.NewPostgreSQLStateStore(testLogger)
	case "sqlite":
		store = s_sqlite.NewSQLiteStateStore(testLogger)
	case "mysql.mysql":
		store = s_mysql.NewMySQLStateStore(testLogger)
	case "mysql.mariadb":
		store = s_mysql.NewMySQLStateStore(testLogger)
	case "oracledatabase":
		store = s_oracledatabase.NewOracleDatabaseStateStore(testLogger)
	case "azure.tablestorage.storage":
		store = s_azuretablestorage.NewAzureTablesStateStore(testLogger)
	case "azure.tablestorage.cosmosdb":
		store = s_azuretablestorage.NewAzureTablesStateStore(testLogger)
	case "cassandra":
		store = s_cassandra.NewCassandraStateStore(testLogger)
	case "cloudflare.workerskv":
		store = s_cloudflareworkerskv.NewCFWorkersKV(testLogger)
	case "cockroachdb":
		store = s_cockroachdb.New(testLogger)
	case "memcached":
		store = s_memcached.NewMemCacheStateStore(testLogger)
	case "rethinkdb":
		store = s_rethinkdb.NewRethinkDBStateStore(testLogger)
	case "in-memory":
		store = s_inmemory.NewInMemoryStateStore(testLogger)
	case "aws.dynamodb.docker":
		store = s_awsdynamodb.NewDynamoDBStateStore(testLogger)
	case "aws.dynamodb.terraform":
		store = s_awsdynamodb.NewDynamoDBStateStore(testLogger)
	case "etcd":
		store = s_etcd.NewEtcdStateStore(testLogger)
	case "gcp.firestore.docker":
		store = s_gcpfirestore.NewFirestoreStateStore(testLogger)
	case "gcp.firestore.cloud":
		store = s_gcpfirestore.NewFirestoreStateStore(testLogger)
	default:
		return nil
	}

	return store
}

func loadOutputBindings(tc TestComponent) bindings.OutputBinding {
	var binding bindings.OutputBinding

	switch tc.Component {
	case redisv6:
		binding = b_redis.NewRedis(testLogger)
	case redisv7:
		binding = b_redis.NewRedis(testLogger)
	case "azure.blobstorage":
		binding = b_azure_blobstorage.NewAzureBlobStorage(testLogger)
	case "azure.storagequeues":
		binding = b_azure_storagequeues.NewAzureStorageQueues(testLogger)
	case "azure.servicebusqueues":
		binding = b_azure_servicebusqueues.NewAzureServiceBusQueues(testLogger)
	case "azure.eventgrid":
		binding = b_azure_eventgrid.NewAzureEventGrid(testLogger)
	case eventhubs:
		binding = b_azure_eventhubs.NewAzureEventHubs(testLogger)
	case "azure.cosmosdb":
		binding = b_azure_cosmosdb.NewCosmosDB(testLogger)
	case kafka:
		binding = b_kafka.NewKafka(testLogger)
	case "http":
		binding = b_http.NewHTTP(testLogger)
	case "influx":
		binding = b_influx.NewInflux(testLogger)
	case "mqtt3":
		binding = b_mqtt3.NewMQTT(testLogger)
	case "rabbitmq":
		binding = b_rabbitmq.NewRabbitMQ(testLogger)
	case "kubemq":
		binding = b_kubemq.NewKubeMQ(testLogger)
	case "postgres":
		binding = b_postgres.NewPostgres(testLogger)
	case "aws.s3.docker":
		binding = b_aws_s3.NewAWSS3(testLogger)
	case "aws.s3.terraform":
		binding = b_aws_s3.NewAWSS3(testLogger)
	default:
		return nil
	}

	return binding
}

func loadInputBindings(tc TestComponent) bindings.InputBinding {
	var binding bindings.InputBinding

	switch tc.Component {
	case "azure.servicebusqueues":
		binding = b_azure_servicebusqueues.NewAzureServiceBusQueues(testLogger)
	case "azure.storagequeues":
		binding = b_azure_storagequeues.NewAzureStorageQueues(testLogger)
	case "azure.eventgrid":
		binding = b_azure_eventgrid.NewAzureEventGrid(testLogger)
	case "cron":
		binding = b_cron.NewCron(testLogger)
	case eventhubs:
		binding = b_azure_eventhubs.NewAzureEventHubs(testLogger)
	case kafka:
		binding = b_kafka.NewKafka(testLogger)
	case "mqtt3":
		binding = b_mqtt3.NewMQTT(testLogger)
	case "rabbitmq":
		binding = b_rabbitmq.NewRabbitMQ(testLogger)
	case "kubemq":
		binding = b_kubemq.NewKubeMQ(testLogger)
	default:
		return nil
	}

	return binding
}

func loadWorkflow(tc TestComponent) workflows.Workflow {
	var wf workflows.Workflow

	switch tc.Component {
	case "temporal":
		wf = wf_temporal.NewTemporalWorkflow(testLogger)
	default:
		return nil
	}

	return wf
}

func atLeastOne(t *testing.T, predicate func(interface{}) bool, items ...interface{}) {
	met := false

	for _, item := range items {
		met = met || predicate(item)
	}

	assert.True(t, met)
}
