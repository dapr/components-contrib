package nacos_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings/alicloud/nacos"
)

func TestParseMetadata(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"endpoint":        "a",
		"region":          "b",
		"namespace":       "c",
		"accessKey":       "d",
		"secretKey":       "e",
		"updateThreadNum": "3",
	}

	var settings nacos.Settings
	err := settings.Decode(props)
	require.NoError(t, err)
	assert.Equal(t, "a", settings.Endpoint)
	assert.Equal(t, "b", settings.RegionID)
	assert.Equal(t, "c", settings.NamespaceID)
	assert.Equal(t, "d", settings.AccessKey)
	assert.Equal(t, "e", settings.SecretKey)
	assert.Equal(t, 3, settings.UpdateThreadNum)
}
