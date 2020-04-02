package hazelcast

import (
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestValidateMetadata(t *testing.T) {
	t.Run("return error when required servers is empty", func(t *testing.T) {
		fakeMetaData := pubsub.Metadata{
			Properties: map[string]string{
				hazelcastServers: "",
			},
		}

		m, err := parseHazelcastMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
		assert.Empty(t, m)
	})
}
