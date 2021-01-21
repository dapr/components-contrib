package conformance

import (
	"os"
	"testing"

	"github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestDecodeYaml(t *testing.T) {
	var config TestConfiguration
	yam := `
componentType: state
components:
  - component: redis
    allOperations: false
    operations: ["init", "set"]
    config:
      maxInitDurationInMs: 20
      maxSetDurationInMs: 20
      maxDeleteDurationInMs: 10
      maxGetDurationInMs: 10
      numBulkRequests: 10
`
	config, err := decodeYaml([]byte(yam))
	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, 1, len(config.Components))
	assert.False(t, config.Components[0].AllOperations)
	assert.Equal(t, "state", config.ComponentType)
	assert.Equal(t, 2, len(config.Components[0].Operations))
	assert.Equal(t, 5, len(config.Components[0].Config))
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
	items := []v1alpha1.MetadataItem{
		{
			Name: "test_key",
			Value: v1alpha1.DynamicValue{
				JSON: v1.JSON{Raw: []byte("test")},
			},
		},
		{
			Name: "env_var_sub",
			Value: v1alpha1.DynamicValue{
				JSON: v1.JSON{Raw: []byte("${{CONF_TEST_KEY}}")},
			},
		},
	}
	os.Setenv("CONF_TEST_KEY", "testval")
	defer os.Unsetenv("CONF_TEST_KEY")
	resp := ConvertMetadataToProperties(items)
	assert.NotNil(t, resp)
	assert.Equal(t, 2, len(resp))
	assert.Equal(t, "test", resp["test_key"])
	assert.Equal(t, "testval", resp["env_var_sub"])
}

func TestConvertComponentNameToPath(t *testing.T) {
	val := convertComponentNameToPath("azure.servicebus")
	assert.Equal(t, "azure/servicebus", val)
	val = convertComponentNameToPath("a.b.c")
	assert.Equal(t, "a/b/c", val)
	val = convertComponentNameToPath("redis")
	assert.Equal(t, "redis", val)
}
