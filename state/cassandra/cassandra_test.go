// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cassandra

import (
	"testing"

	"github.com/dapr/components-contrib/state"
	"github.com/stretchr/testify/assert"
)

func TestGetCassandraMetadata(t *testing.T) {
	t.Run("With defaults", func(t *testing.T) {
		properties := map[string]string{
			hosts: "127.0.0.1",
		}
		m := state.Metadata{
			Properties: properties,
		}

		metadata, err := getCassandraMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, properties[hosts], metadata.hosts[0])
		assert.Equal(t, "All", metadata.consistency)
		assert.Equal(t, defaultKeyspace, metadata.keyspace)
		assert.Equal(t, defaultProtoVersion, metadata.protoVersion)
		assert.Equal(t, defaultReplicationFactor, metadata.replicationFactor)
		assert.Equal(t, defaultTable, metadata.table)
		assert.Equal(t, defaultPort, metadata.port)
	})

	t.Run("With custom values", func(t *testing.T) {
		properties := map[string]string{
			hosts:             "127.0.0.1",
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
			Properties: properties,
		}

		metadata, err := getCassandraMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, properties[hosts], metadata.hosts[0])
		assert.Equal(t, properties[consistency], metadata.consistency)
		assert.Equal(t, properties[keyspace], metadata.keyspace)
		assert.Equal(t, 3, metadata.protoVersion)
		assert.Equal(t, 2, metadata.replicationFactor)
		assert.Equal(t, properties[table], metadata.table)
		assert.Equal(t, properties[username], metadata.username)
		assert.Equal(t, properties[password], metadata.password)
		assert.Equal(t, 9043, metadata.port)
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
			Properties: properties,
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
			Properties: properties,
		}

		_, err := getCassandraMetadata(m)
		assert.NotNil(t, err)
	})
}
