// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nats

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		natsURL: "fakeNatsURL",
		queue:   "fakeNatsQ",
	}
}

func TestParseNATSMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := parseNATSMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[queue], m.queue)
	})

	t.Run("queue is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[queue] = ""

		// act
		m, err := parseNATSMetadata(fakeMetaData)
		// assert
		assert.Error(t, errors.New("nats error: missing queue name"), err)
		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Empty(t, m.queue)
	})

	t.Run("nats url is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[natsURL] = ""

		// act
		m, err := parseNATSMetadata(fakeMetaData)
		// assert
		assert.Equal(t, fakeProperties[queue], m.queue)
		assert.Error(t, errors.New("nats error: missing nats URL"), err)
		assert.Empty(t, m.natsURL)
	})

}
