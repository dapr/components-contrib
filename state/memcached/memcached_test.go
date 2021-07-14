package memcached

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestMemcachedMetadata(t *testing.T) {
	t.Run("without required configuration", func(t *testing.T) {
		properties := map[string]string{}
		m := state.Metadata{
			Properties: properties,
		}
		_, err := getMemcachedMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("with required configuration, single host", func(t *testing.T) {
		properties := map[string]string{
			"hosts": "localhost:11211",
		}
		m := state.Metadata{
			Properties: properties,
		}
		metadata, err := getMemcachedMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, properties["hosts"], metadata.hosts[0])
		assert.Equal(t, defaultMaxIdleConnections, metadata.maxIdleConnections)
		assert.Equal(t, defaultTimeout, metadata.timeout)
	})

	t.Run("with required configuration, multiple host", func(t *testing.T) {
		properties := map[string]string{
			"hosts": "localhost:11211,10.0.0.1:11211,10.0.0.2:10000",
		}
		m := state.Metadata{
			Properties: properties,
		}
		split := strings.Split(properties["hosts"], ",")
		metadata, err := getMemcachedMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, split, metadata.hosts)
		assert.Equal(t, defaultMaxIdleConnections, metadata.maxIdleConnections)
		assert.Equal(t, defaultTimeout, metadata.timeout)
	})

	t.Run("with optional configuration, multiple hosts", func(t *testing.T) {
		properties := map[string]string{
			"hosts":              "localhost:11211,10.0.0.1:11211,10.0.0.2:10000",
			"maxIdleConnections": "10",
			"timeout":            "5000",
		}
		m := state.Metadata{
			Properties: properties,
		}
		split := strings.Split(properties["hosts"], ",")
		metadata, err := getMemcachedMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, split, metadata.hosts)
		assert.Equal(t, 10, metadata.maxIdleConnections)
		assert.Equal(t, 5000*time.Millisecond, metadata.timeout)
	})
}

func TestParseTTL(t *testing.T) {
	store := NewMemCacheStateStore(logger.NewLogger("test"))
	t.Run("TTL Not an integer", func(t *testing.T) {
		ttlInSeconds := "not an integer"
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": ttlInSeconds,
			},
		})

		assert.NotNil(t, err, "tll is not an integer")
		assert.Nil(t, ttl)
	})
	t.Run("TTL specified with wrong key", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"expirationTime": strconv.Itoa(ttlInSeconds),
			},
		})

		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL is a number", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, int(*ttl), ttlInSeconds)
	})

	t.Run("TTL never expires", func(t *testing.T) {
		ttlInSeconds := 0
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, int(*ttl), ttlInSeconds)
	})
}
