package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
