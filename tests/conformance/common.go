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
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/dapr/kit/logger"
)

const (
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
	TestFn        func(comp *TestComponent) func(t *testing.T)
}

type TestComponent struct {
	Component  string         `yaml:"component,omitempty"`
	Profile    string         `yaml:"profile,omitempty"`
	Operations []string       `yaml:"operations,omitempty"`
	Config     map[string]any `yaml:"config,omitempty"`
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

func loadComponentsAndProperties(t *testing.T, filepath string) (map[string]string, error) {
	comps, err := LoadComponents(filepath)
	require.NoError(t, err)
	require.Len(t, comps, 1) // We only expect a single component per file
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
	for i := range tc.Components {
		comp := tc.Components[i]
		testName := comp.Component
		if comp.Profile != "" {
			testName += "-" + comp.Profile
		}

		t.Run(testName, tc.TestFn(&comp))
	}
}
