package internal

import (
	"testing"

	"github.com/dapr/components-contrib/configuration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRedisKeyPattern(t *testing.T) {
	pattern, err := BuildRedisKeyPattern("appid1")

	require.NoError(t, err)
	assert.NotEmpty(t, pattern)
	assert.Equal(t, "dapr/configuration/appid1/*", pattern)
}

func TestBuildRedisKey(t *testing.T) {
	key, err := BuildRedisKey("appid1", "key1")

	require.NoError(t, err)
	assert.NotEmpty(t, key)
	assert.Equal(t, "dapr/configuration/appid1/key1", key)
}

func TestBuildRedisKey4Revision(t *testing.T) {
	key, err := BuildRedisKey4Revision("appid1")

	require.NoError(t, err)
	assert.NotEmpty(t, key)
	assert.Equal(t, "dapr/configuration/appid1/_revision", key)
}

func TestParseRedisKey(t *testing.T) {
	item := &configuration.Item{}
	err := ParseRedisKey("dapr/configuration/appid1/key1", item)

	require.NoError(t, err)
	assert.Equal(t, "key1", item.Key)
}