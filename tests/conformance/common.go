package conformance

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fortio.org/fortio/log"
	"github.com/dapr/components-contrib/pubsub"
	p_redis "github.com/dapr/components-contrib/pubsub/redis"
	"github.com/dapr/components-contrib/secretstores"
	ss_local_env "github.com/dapr/components-contrib/secretstores/local/env"
	ss_local_file "github.com/dapr/components-contrib/secretstores/local/file"
	"github.com/dapr/components-contrib/state"
	s_redis "github.com/dapr/components-contrib/state/redis"
	conf_pubsub "github.com/dapr/components-contrib/tests/conformance/pubsub"
	conf_secret "github.com/dapr/components-contrib/tests/conformance/secretstores"
	conf_state "github.com/dapr/components-contrib/tests/conformance/state"
	"github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	"github.com/dapr/dapr/pkg/components"
	config "github.com/dapr/dapr/pkg/config/modes"

	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

// nolint:gochecknoglobals
var testLogger = logger.NewLogger("testLogger")

type TestConfiguration struct {
	ComponentType string          `yaml:"componentType,omitempty"`
	Components    []TestComponent `yaml:"components,omitempty"`
}
type TestComponent struct {
	Component     string            `yaml:"component,omitempty"`
	AllOperations bool              `yaml:"allOperations,omitempty"`
	Operations    []string          `yaml:"operations,omitempty"`
	Config        map[string]string `yaml:"config,omitempty"`
}

// NewTestConfiguration reads the tests.yml and loads the TestConfiguration
func NewTestConfiguration(configFilepath string) (*TestConfiguration, error) {
	if isYaml(configFilepath) {
		b, err := readTestConfiguration(configFilepath)
		if err != nil {
			log.Warnf("error reading file %s : %s", configFilepath, err)

			return nil, err
		}
		tc, err := decodeYaml(b)

		return &tc, err
	}

	return nil, errors.New("no test configuration file tests.yml found")
}

func LoadComponents(componentPath string) ([]v1alpha1.Component, error) {
	cfg := config.StandaloneConfig{
		ComponentsPath: componentPath,
	}
	standaloneComps := components.NewStandaloneComponents(cfg)
	components, err := standaloneComps.LoadComponents()
	if err != nil {
		return nil, err
	}

	return components, nil
}

func ConvertMetadataToProperties(items []v1alpha1.MetadataItem) map[string]string {
	properties := map[string]string{}
	for _, c := range items {
		properties[c.Name] = c.Value.String()
	}

	return properties
}

// nolint:gosec
func NewRandString(length int) string {
	rand.Seed(time.Now().Unix())
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

// isYaml checks whether the file is yaml or not
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
		log.Warnf("error parsing string as yaml %s", err)

		return TestConfiguration{}, err
	}

	return testConfig, nil
}

func (tc *TestConfiguration) loadComponentsAndProperties(t *testing.T, filepath string) map[string]string {
	comps, err := LoadComponents(filepath)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(comps)) // We only expect a single component per state store but on
	c := comps[0]
	props := ConvertMetadataToProperties(c.Spec.Metadata)

	return props
}

func (tc *TestConfiguration) Run(t *testing.T) {
	// For each component in the tests file run the conformance test
	for _, comp := range tc.Components {
		switch tc.ComponentType {
		case "state":
			filepath := fmt.Sprintf("../config/state/%s", comp.Component)
			props := tc.loadComponentsAndProperties(t, filepath)
			store := loadStateStore(comp)
			assert.NotNil(t, store)
			storeConfig := conf_state.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
			conf_state.ConformanceTests(t, props, store, storeConfig)
		case "secretstores":
			filepath := fmt.Sprintf("../config/secretstores/%s", comp.Component)
			props := tc.loadComponentsAndProperties(t, filepath)
			store := loadSecretStore(comp)
			assert.NotNil(t, store)
			storeConfig := conf_secret.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations)
			conf_secret.ConformanceTests(t, props, store, storeConfig)
		case "pubsub":
			filepath := fmt.Sprintf("../config/pubsub/%s", comp.Component)
			props := tc.loadComponentsAndProperties(t, filepath)
			pubsub := loadPubSub(comp)
			assert.NotNil(t, pubsub)
			pubsubConfig := conf_pubsub.NewTestConfig(comp.Component, comp.AllOperations, comp.Operations, comp.Config)
			conf_pubsub.ConformanceTests(t, props, pubsub, pubsubConfig)
		default:
			assert.Failf(t, "unknown component type %s", tc.ComponentType)
		}
	}
}

func loadPubSub(tc TestComponent) pubsub.PubSub {
	var pubsub pubsub.PubSub
	switch tc.Component {
	case "redis":
		pubsub = p_redis.NewRedisStreams(testLogger)
	default:
		return nil
	}

	return pubsub
}

func loadSecretStore(tc TestComponent) secretstores.SecretStore {
	var store secretstores.SecretStore
	switch tc.Component {
	case "localfile":
		store = ss_local_file.NewLocalSecretStore(testLogger)
	case "localenv":
		store = ss_local_env.NewEnvSecretStore(testLogger)
	default:
		return nil
	}

	return store
}

func loadStateStore(tc TestComponent) state.Store {
	var store state.Store
	switch tc.Component {
	case "redis":
		store = s_redis.NewRedisStateStore(testLogger)
	default:
		return nil
	}

	return store
}
