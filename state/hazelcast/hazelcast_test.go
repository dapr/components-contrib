package hazelcast

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
)

func TestValidateMetadata(t *testing.T) {
	t.Run("without required configuration", func(t *testing.T) {
		properties := map[string]string{}
		m := state.Metadata{
			Properties: properties,
		}
		err := validateMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("without server configuration", func(t *testing.T) {
		properties := map[string]string{
			"hazelcastMap": "foo-map",
		}
		m := state.Metadata{
			Properties: properties,
		}
		err := validateMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("without map configuration", func(t *testing.T) {
		properties := map[string]string{
			"hazelcastServers": "hz1:5701",
		}
		m := state.Metadata{
			Properties: properties,
		}
		err := validateMetadata(m)
		assert.NotNil(t, err)
	})

	t.Run("with valid configuration", func(t *testing.T) {
		properties := map[string]string{
			"hazelcastServers": "hz1:5701",
			"hazelcastMap":     "foo-map",
		}
		m := state.Metadata{
			Properties: properties,
		}
		err := validateMetadata(m)
		assert.Nil(t, err)
	})
}
