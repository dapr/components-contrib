package internal

import (
	"testing"

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
	key, err := BuildRedisKey("appid1")

	require.NoError(t, err)
	assert.NotEmpty(t, key)
	assert.Equal(t, "dapr/configuration/appid1", key)
}

func TestParseRedisKey(t *testing.T) {
	appid, err := ParseRedisKey("dapr/configuration/appid1")

	require.NoError(t, err)
	assert.Equal(t, "appid1", appid)
}