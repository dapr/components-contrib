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

package memcached

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestMemcachedMetadata(t *testing.T) {
	t.Run("without required configuration", func(t *testing.T) {
		properties := map[string]string{}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := getMemcachedMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("with required configuration, single host", func(t *testing.T) {
		properties := map[string]string{
			"hosts": "localhost:11211",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		metadata, err := getMemcachedMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, properties["hosts"], metadata.Hosts[0])
		assert.Equal(t, defaultMaxIdleConnections, metadata.MaxIdleConnections)
		assert.Equal(t, -1, metadata.Timeout)
	})

	t.Run("with required configuration, multiple host", func(t *testing.T) {
		properties := map[string]string{
			"hosts": "localhost:11211,10.0.0.1:11211,10.0.0.2:10000",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		split := strings.Split(properties["hosts"], ",")
		metadata, err := getMemcachedMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, split, metadata.Hosts)
		assert.Equal(t, defaultMaxIdleConnections, metadata.MaxIdleConnections)
		assert.Equal(t, -1, metadata.Timeout)
	})

	t.Run("with optional configuration, multiple hosts", func(t *testing.T) {
		properties := map[string]string{
			"hosts":              "localhost:11211,10.0.0.1:11211,10.0.0.2:10000",
			"maxIdleConnections": "10",
			"timeout":            "5000",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		split := strings.Split(properties["hosts"], ",")
		metadata, err := getMemcachedMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, split, metadata.Hosts)
		assert.Equal(t, 10, metadata.MaxIdleConnections)
		assert.Equal(t, int(5000*time.Millisecond), metadata.Timeout*int(time.Millisecond))
	})
}

func TestParseTTL(t *testing.T) {
	store := NewMemCacheStateStore(logger.NewLogger("test")).(*Memcached)
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
	t.Run("TTL is a negative integer ends up translated to 0", func(t *testing.T) {
		ttlInSeconds := -1
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, int(*ttl), 0)
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
