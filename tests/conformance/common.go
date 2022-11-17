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
	"encoding/json"
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
	"gopkg.in/yaml.v3"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/workflows"
	"github.com/dapr/kit/logger"

	b_azure_blobstorage "github.com/dapr/components-contrib/bindings/azure/blobstorage"
	b_azure_cosmosdb "github.com/dapr/components-contrib/bindings/azure/cosmosdb"
	b_azure_eventgrid "github.com/dapr/components-contrib/bindings/azure/eventgrid"
	b_azure_eventhubs "github.com/dapr/components-contrib/bindings/azure/eventhubs"
	b_azure_servicebusqueues "github.com/dapr/components-contrib/bindings/azure/servicebusqueues"
	b_azure_storagequeues "github.com/dapr/components-contrib/bindings/azure/storagequeues"
	b_http "github.com/dapr/components-contrib/bindings/http"
	b_influx "github.com/dapr/components-contrib/bindings/influx"
	b_kafka "github.com/dapr/components-contrib/bindings/kafka"
	b_kubemq "github.com/dapr/components-contrib/bindings/kubemq"
	b_mqtt "github.com/dapr/components-contrib/bindings/mqtt"
	b_postgres "github.com/dapr/components-contrib/bindings/postgres"
	b_rabbitmq "github.com/dapr/components-contrib/bindings/rabbitmq"
	b_redis "github.com/dapr/components-contrib/bindings/redis"
	p_snssqs "github.com/dapr/components-contrib/pubsub/aws/snssqs"
	p_eventhubs "github.com/dapr/components-contrib/pubsub/azure/eventhubs"
	p_servicebusqueues "github.com/dapr/components-contrib/pubsub/azure/servicebus/queues"
	p_servicebustopics "github.com/dapr/components-contrib/pubsub/azure/servicebus/topics"
	p_hazelcast "github.com/dapr/components-contrib/pubsub/hazelcast"
	p_inmemory "github.com/dapr/components-contrib/pubsub/in-memory"
	p_jetstream "github.com/dapr/components-contrib/pubsub/jetstream"
	p_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	p_kubemq "github.com/dapr/components-contrib/pubsub/kubemq"
	p_mqtt "github.com/dapr/components-contrib/pubsub/mqtt"
	p_natsstreaming "github.com/dapr/components-contrib/pubsub/natsstreaming"
	p_pulsar "github.com/dapr/components-contrib/pubsub/pulsar"
	p_rabbitmq "github.com/dapr/components-contrib/pubsub/rabbitmq"
	p_redis "github.com/dapr/components-contrib/pubsub/redis"
	ss_azure "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	ss_hashicorp_vault "github.com/dapr/components-contrib/secretstores/hashicorp/vault"
	ss_kubernetes "github.com/dapr/components-contrib/secretstores/kubernetes"
	ss_local_env "github.com/dapr/components-contrib/secretstores/local/env"
	ss_local_file "github.com/dapr/components-contrib/secretstores/local/file"
	s_blobstorage "github.com/dapr/components-contrib/state/azure/blobstorage"
	s_cosmosdb "github.com/dapr/components-contrib/state/azure/cosmosdb"
	s_azuretablestorage "github.com/dapr/components-contrib/state/azure/tablestorage"
	s_cassandra "github.com/dapr/components-contrib/state/cassandra"
	s_cockroachdb "github.com/dapr/components-contrib/state/cockroachdb"
	s_memcached "github.com/dapr/components-contrib/state/memcached"
	s_mongodb "github.com/dapr/components-contrib/state/mongodb"
	s_mysql "github.com/dapr/components-contrib/state/mysql"
	s_postgresql "github.com/dapr/components-contrib/state/postgresql"
	s_redis "github.com/dapr/components-contrib/state/redis"
	s_rethinkdb "github.com/dapr/components-contrib/state/rethinkdb"
	s_sqlserver "github.com/dapr/components-contrib/state/sqlserver"
	conf_bindings "github.com/dapr/components-contrib/tests/conformance/bindings"
	conf_pubsub "github.com/dapr/components-contrib/tests/conformance/pubsub"
	conf_secret "github.com/dapr/components-contrib/tests/conformance/secretstores"
	conf_state "github.com/dapr/components-contrib/tests/conformance/state"
	conf_workflows "github.com/dapr/components-contrib/tests/conformance/workflows"
	wf_temporal "github.com/dapr/components-contrib/workflows/temporal"
)

const (
	eventhubs    = "azure.eventhubs"
	redis        = "redis"
	kafka        = "kafka"
	mqtt         = "mqtt"
	generateUUID = "$((uuid))"
)

//nolint:gochecknoglobals
var testLogger = logger.NewLogger("testLogger")

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
		val := c.Value.String()
		if strings.HasPrefix(c.Value.String(), "${{") {
			// look up env var with that name. remove ${{}} and space
			k := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(val, "${{"), "}}"))
			v := LookUpEnv(k)
			if v == "" {
				return map[string]string{}, fmt.Errorf("required env var is not set %s", k)
			}
			val = v
		}
		properties[c.Name] = val
	}

	return properties, nil
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
	assert.Nil(t, err)
	assert.Equal(t, 1, len(comps)) // We only expect a single component per file
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
			default:
				t.Errorf("unknown component type %s", tc.ComponentType)
			}
		})
	}
}

func loadPubSub(tc TestComponent) pubsub.PubSub {
	var pubsub pubsub.PubSub
	switch tc.Component {
	case redis:
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
	case mqtt:
		pubsub = p_mqtt.NewMQTTPubSub(testLogger)
	case "hazelcast":
		pubsub = p_hazelcast.NewHazelcastPubSub(testLogger)
	case "rabbitmq":
		pubsub = p_rabbitmq.NewRabbitMQ(testLogger)
	case "in-memory":
		pubsub = p_inmemory.New(testLogger)
	case "aws.snssqs":
		pubsub = p_snssqs.NewSnsSqs(testLogger)
	case "kubemq":
		pubsub = p_kubemq.NewKubeMQ(testLogger)
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
	case "localenv":
		store = ss_local_env.NewEnvSecretStore(testLogger)
	case "localfile":
		store = ss_local_file.NewLocalSecretStore(testLogger)
	case "hashicorp.vault":
		store = ss_hashicorp_vault.NewHashiCorpVaultSecretStore(testLogger)
	default:
		return nil
	}

	return store
}

func loadStateStore(tc TestComponent) state.Store {
	var store state.Store
	switch tc.Component {
	case redis:
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
		store = s_sqlserver.NewSQLServerStateStore(testLogger)
	case "postgresql":
		store = s_postgresql.NewPostgreSQLStateStore(testLogger)
	case "mysql.mysql":
		store = s_mysql.NewMySQLStateStore(testLogger)
	case "mysql.mariadb":
		store = s_mysql.NewMySQLStateStore(testLogger)
	case "azure.tablestorage.storage":
		store = s_azuretablestorage.NewAzureTablesStateStore(testLogger)
	case "azure.tablestorage.cosmosdb":
		store = s_azuretablestorage.NewAzureTablesStateStore(testLogger)
	case "cassandra":
		store = s_cassandra.NewCassandraStateStore(testLogger)
	case "cockroachdb":
		store = s_cockroachdb.New(testLogger)
	case "memcached":
		store = s_memcached.NewMemCacheStateStore(testLogger)
	case "rethinkdb":
		store = s_rethinkdb.NewRethinkDBStateStore(testLogger)
	default:
		return nil
	}

	return store
}

func loadOutputBindings(tc TestComponent) bindings.OutputBinding {
	var binding bindings.OutputBinding

	switch tc.Component {
	case redis:
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
	case mqtt:
		binding = b_mqtt.NewMQTT(testLogger)
	case "rabbitmq":
		binding = b_rabbitmq.NewRabbitMQ(testLogger)
	case "kubemq":
		binding = b_kubemq.NewKubeMQ(testLogger)
	case "postgres":
		binding = b_postgres.NewPostgres(testLogger)
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
	case eventhubs:
		binding = b_azure_eventhubs.NewAzureEventHubs(testLogger)
	case kafka:
		binding = b_kafka.NewKafka(testLogger)
	case mqtt:
		binding = b_mqtt.NewMQTT(testLogger)
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
