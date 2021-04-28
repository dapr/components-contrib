package internal

import (
	"testing"

	"github.com/dapr/components-contrib/configuration"
	"github.com/stretchr/testify/require"
)

func TestBuildRedisValue(t *testing.T) {
	redisValue, err := BuildRedisValue("content1", map[string]string{"tag1": "t1", "tag2": "t2", "tag3": "t3"})
	require.NoError(t, err)
	require.NotEmpty(t, redisValue)
	require.Equal(t, map[string]string{"content":"content1", "tag-tag1":"t1", "tag-tag2":"t2", "tag-tag3":"t3"}, redisValue)
}

func TestParseRedisValue(t *testing.T) {
	item := configuration.Item{}
	ParseRedisValue(map[string]string{"content":"content1", "tag-tag1":"t1", "tag-tag2":"t2", "tag-tag3":"t3"}, &item)

	require.Equal(t, item.Content, "content1")
	require.NotEmpty(t, item.Tags)
	require.Equal(t, "t1", item.Tags["tag1"])
	require.Equal(t, "t2", item.Tags["tag2"])
	require.Equal(t, "t3", item.Tags["tag3"])
}