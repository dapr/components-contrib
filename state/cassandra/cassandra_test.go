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

package cassandra

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestGetCassandraMetadata(t *testing.T) {
	t.Run("With defaults", func(t *testing.T) {
		properties := map[string]string{
			hosts: "127.0.0.1",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getCassandraMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, properties[hosts], metadata.Hosts[0])
		assert.Equal(t, "All", metadata.Consistency)
		assert.Equal(t, defaultKeyspace, metadata.Keyspace)
		assert.Equal(t, defaultProtoVersion, metadata.ProtoVersion)
		assert.Equal(t, defaultReplicationFactor, metadata.ReplicationFactor)
		assert.Equal(t, defaultTable, metadata.Table)
		assert.Equal(t, defaultPort, metadata.Port)
	})

	t.Run("With custom values", func(t *testing.T) {
		properties := map[string]string{
			hosts:             "127.0.0.1,10.10.10.10",
			port:              "9043",
			consistency:       "Quorum",
			keyspace:          "keyspace",
			protoVersion:      "3",
			replicationFactor: "2",
			table:             "table",
			username:          "username",
			password:          "password",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getCassandraMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, strings.Split(properties[hosts], ",")[0], metadata.Hosts[0])
		assert.Equal(t, strings.Split(properties[hosts], ",")[1], metadata.Hosts[1])
		assert.Equal(t, properties[consistency], metadata.Consistency)
		assert.Equal(t, properties[keyspace], metadata.Keyspace)
		assert.Equal(t, 3, metadata.ProtoVersion)
		assert.Equal(t, 2, metadata.ReplicationFactor)
		assert.Equal(t, properties[table], metadata.Table)
		assert.Equal(t, properties[username], metadata.Username)
		assert.Equal(t, properties[password], metadata.Password)
		assert.Equal(t, 9043, metadata.Port)
	})

	t.Run("Incorrect proto version", func(t *testing.T) {
		properties := map[string]string{
			hosts:             "127.0.0.1",
			consistency:       "Quorum",
			keyspace:          "keyspace",
			protoVersion:      "incorrect",
			replicationFactor: "2",
			table:             "table",
			username:          "username",
			password:          "password",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := getCassandraMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("Missing hosts", func(t *testing.T) {
		properties := map[string]string{
			consistency:       "Quorum",
			keyspace:          "keyspace",
			protoVersion:      "incorrect",
			replicationFactor: "2",
			table:             "table",
			username:          "username",
			password:          "password",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := getCassandraMetadata(m)
		assert.NotNil(t, err)
	})
}

func TestParseTTL(t *testing.T) {
	t.Run("TTL Not an integer", func(t *testing.T) {
		ttlInSeconds := "not an integer"
		ttl, err := parseTTL(map[string]string{
			"ttlInSeconds": ttlInSeconds,
		})
		assert.Error(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL specified with wrong key", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := parseTTL(map[string]string{
			"expirationTime": strconv.Itoa(ttlInSeconds),
		})
		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL is a number", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := parseTTL(map[string]string{
			"ttlInSeconds": strconv.Itoa(ttlInSeconds),
		})
		assert.NoError(t, err)
		assert.Equal(t, *ttl, ttlInSeconds)
	})
	t.Run("TTL not set", func(t *testing.T) {
		ttl, err := parseTTL(map[string]string{})
		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
}
