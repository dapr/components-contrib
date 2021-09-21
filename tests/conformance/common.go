// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package conformance

import (
	"errors"
	"fmt"
	"io/ioutil"
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
	"github.com/dapr/kit/logger"

	b_azure_blobstorage "github.com/dapr/components-contrib/bindings/azure/blobstorage"
	b_azure_eventgrid "github.com/dapr/components-contrib/bindings/azure/eventgrid"
	b_azure_eventhubs "github.com/dapr/components-contrib/bindings/azure/eventhubs"
	b_azure_servicebusqueues "github.com/dapr/components-contrib/bindings/azure/servicebusqueues"
	b_azure_storagequeues "github.com/dapr/components-contrib/bindings/azure/storagequeues"
	b_http "github.com/dapr/components-contrib/bindings/http"
	b_influx "github.com/dapr/components-contrib/bindings/influx"
	b_kafka "github.com/dapr/components-contrib/bindings/kafka"
	b_mqtt "github.com/dapr/components-contrib/bindings/mqtt"
	b_redis "github.com/dapr/components-contrib/bindings/redis"
	p_eventhubs "github.com/dapr/components-contrib/pubsub/azure/eventhubs"
	p_servicebus "github.com/dapr/components-contrib/pubsub/azure/servicebus"
	p_hazelcast "github.com/dapr/components-contrib/pubsub/hazelcast"
	p_inmemory "github.com/dapr/components-contrib/pubsub/in-memory"
	p_jetstream "github.com/dapr/components-contrib/pubsub/jetstream"
	p_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	p_mqtt "github.com/dapr/components-contrib/pubsub/mqtt"
	p_natsstreaming "github.com/dapr/components-contrib/pubsub/natsstreaming"
	p_pulsar "github.com/dapr/components-contrib/pubsub/pulsar"
	p_rabbitmq "github.com/dapr/components-contrib/pubsub/rabbitmq"
	p_redis "github.com/dapr/components-contrib/pubsub/redis"
	ss_azure "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	ss_kubernetes "github.com/dapr/components-contrib/secretstores/kubernetes"
	ss_local_env "github.com/dapr/components-contrib/secretstores/local/env"
	ss_local_file "github.com/dapr/components-contrib/secretstores/local/file"
	s_cosmosdb "github.com/dapr/components-contrib/state/azure/cosmosdb"
	s_mongodb "github.com/dapr/components-contrib/state/mongodb"
	s_mysql "github.com/dapr/components-contrib/state/mysql"
	s_redis "github.com/dapr/components-contrib/state/redis"
	s_sqlserver "github.com/dapr/components-contrib/state/sqlserver"
	conf_bindings "github.com/dapr/components-contrib/tests/conformance/bindings"
	conf_pubsub "github.com/dapr/components-contrib/tests/conformance/pubsub"
	conf_secret "github.com/dapr/components-contrib/tests/conformance/secretstores"
	conf_state "github.com/dapr/components-contrib/tests/conformance/state"
)

const (
	eventhubs    = "azure.eventhubs"
	redis        = "redis"
	kafka        = "kafka"
	mqtt         = "mqtt"
	generateUUID = "$((uuid))"
)

// nolint:gochecknoglobals
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
	b, err := ioutil.ReadFile(filePath)
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
	case "azure.servicebus":
		pubsub = p_servicebus.NewAzureServiceBus(testLogger)
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

	default:
		return nil
	}

	return pubsub
}

func loadSecretStore(tc TestComponent) secretstores.SecretStore {
	var store secretstores.SecretStore
	switch tc.Component {
	case "azure.keyvault":
		store = ss_azure.NewAzureKeyvaultSecretStore(testLogger)
	case "kubernetes":
		store = ss_kubernetes.NewKubernetesSecretStore(testLogger)
	case "localenv":
		store = ss_local_env.NewEnvSecretStore(testLogger)
	case "localfile":
		store = ss_local_file.NewLocalSecretStore(testLogger)
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
	case "cosmosdb":
		store = s_cosmosdb.NewCosmosDBStateStore(testLogger)
	case "mongodb":
		store = s_mongodb.NewMongoDB(testLogger)
	case "sqlserver":
		store = s_sqlserver.NewSQLServerStateStore(testLogger)
	case "mysql":
		store = s_mysql.NewMySQLStateStore(testLogger)
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
	case kafka:
		binding = b_kafka.NewKafka(testLogger)
	case "http":
		binding = b_http.NewHTTP(testLogger)
	case "influx":
		binding = b_influx.NewInflux(testLogger)
	case mqtt:
		binding = b_mqtt.NewMQTT(testLogger)
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
	default:
		return nil
	}

	return binding
}

func atLeastOne(t *testing.T, predicate func(interface{}) bool, items ...interface{}) {
	met := false

	for _, item := range items {
		met = met || predicate(item)
	}

	assert.True(t, met)
}
