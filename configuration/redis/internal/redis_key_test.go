package internal

import (
	"testing"

	"github.com/dapr/components-contrib/configuration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRedisKey(t *testing.T) {
	key, err := BuildRedisKey("appid1", "group1", "prod", "key1")

	require.NoError(t, err)
	assert.NotEmpty(t, key)
	assert.Equal(t, "dapr/configuration/appid1/group1/prod/key1", key)
}

func TestParseRedisKey(t *testing.T) {
	item := &configuration.Item{}
	err := ParseRedisKey("dapr/configuration/appid1/group1/prod/key1", item)

	require.NoError(t, err)
	assert.Equal(t, "group1", item.Group)
	assert.Equal(t, "prod", item.Label)
	assert.Equal(t, "key1", item.Key)
}