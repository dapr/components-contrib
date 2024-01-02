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
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestDecodeYaml(t *testing.T) {
	t.Run("valid yaml", func(t *testing.T) {
		var config TestConfiguration
		yam := `componentType: state
components:
  - component: redis
    operations: ["foo", "bar"]
    config:
      maxInitDurationInMs: 20
      maxSetDurationInMs: 20
      maxDeleteDurationInMs: 10
      maxGetDurationInMs: 10
      numBulkRequests: 10`
		config, err := decodeYaml([]byte(yam))
		require.NoError(t, err)
		assert.NotNil(t, config)
		assert.Len(t, config.Components, 1)
		assert.Equal(t, "state", config.ComponentType)
		assert.Equal(t, []string{"foo", "bar"}, config.Components[0].Operations)
		assert.Len(t, config.Components[0].Config, 5)
	})

	t.Run("invalid yaml", func(t *testing.T) {
		var config TestConfiguration
		yam := `componentType: state
components:
- : redis`
		config, err := decodeYaml([]byte(yam))
		require.Error(t, err)
		assert.Equal(t, TestConfiguration{}, config)
	})
}

func TestIsYaml(t *testing.T) {
	var resp bool
	resp = isYaml("test.yaml")
	assert.True(t, resp)
	resp = isYaml("test.yml")
	assert.True(t, resp)
	resp = isYaml("test.exe")
	assert.False(t, resp)
}

func TestLookUpEnv(t *testing.T) {
	os.Setenv("CONF_TEST_KEY", "testval")
	defer os.Unsetenv("CONF_TEST_KEY")
	r := LookUpEnv("CONF_TEST_KEY")
	assert.Equal(t, "testval", r)
	r = LookUpEnv("CONF_TEST_NOT_THERE")
	assert.Equal(t, "", r)
}

func TestConvertMetadataToProperties(t *testing.T) {
	items := []MetadataItem{
		{
			Name: "test_key",
			Value: DynamicValue{
				JSON: v1.JSON{Raw: []byte("test")},
			},
		},
		{
			Name: "env_var_sub",
			Value: DynamicValue{
				JSON: v1.JSON{Raw: []byte("${{CONF_TEST_KEY}}")},
			},
		},
	}
	t.Run("env var set", func(t *testing.T) {
		os.Setenv("CONF_TEST_KEY", "testval")
		defer os.Unsetenv("CONF_TEST_KEY")
		resp, err := ConvertMetadataToProperties(items)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, resp, 2)
		assert.Equal(t, "test", resp["test_key"])
		assert.Equal(t, "testval", resp["env_var_sub"])
	})

	t.Run("env var not set", func(t *testing.T) {
		resp, err := ConvertMetadataToProperties(items)
		require.Error(t, err)
		assert.NotNil(t, resp)
		assert.Empty(t, resp)
	})
}

func TestParseConfigurationMap(t *testing.T) {
	testMap := map[string]interface{}{
		"key":       "$((uuid))",
		"blob":      "testblob",
		"mapString": `{"nestedkey": "$((uuid))", "somethingtested": "somevalue"}`,
		"map": map[string]interface{}{
			"nestedkey": "$((uuid))",
		},
	}

	ParseConfigurationMap(t, testMap)
	assert.Len(t, testMap, 4)
	assert.Equal(t, "testblob", testMap["blob"])
	_, err := uuid.ParseBytes([]byte(testMap["key"].(string)))
	require.NoError(t, err)

	var nestedMap map[string]interface{}
	json.Unmarshal([]byte(testMap["mapString"].(string)), &nestedMap)
	_, err = uuid.ParseBytes([]byte(nestedMap["nestedkey"].(string)))
	require.NoError(t, err)
	_, err = uuid.ParseBytes([]byte(testMap["map"].(map[string]interface{})["nestedkey"].(string)))
	require.NoError(t, err)
}

func TestConvertComponentNameToPath(t *testing.T) {
	val := convertComponentNameToPath("azure.servicebus", "")
	assert.Equal(t, "azure/servicebus", val)
	val = convertComponentNameToPath("a.b.c", "")
	assert.Equal(t, "a/b/c", val)
	val = convertComponentNameToPath("redis", "")
	assert.Equal(t, "redis", val)
}
