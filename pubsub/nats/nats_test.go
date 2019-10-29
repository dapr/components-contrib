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

func TestParseNATSMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:            "foonats1",
			natsQueueGroupName: "fooq1",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := parseNATSMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.NotEmpty(t, m.natsURL)
		assert.NotEmpty(t, m.natsQueueGroupName)
		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Equal(t, fakeProperties[natsQueueGroupName], m.natsQueueGroupName)
	})

	t.Run("queue is not given", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:            "foonats2",
			natsQueueGroupName: "",
		}

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := parseNATSMetadata(fakeMetaData)
		// assert
		assert.Error(t, errors.New("nats error: missing queue name"), err)
		assert.Equal(t, fakeProperties[natsURL], m.natsURL)
		assert.Empty(t, m.natsQueueGroupName)
	})

	t.Run("nats url is not given", func(t *testing.T) {
		fakeProperties := map[string]string{
			natsURL:            "",
			natsQueueGroupName: "fooq2",
		}
		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		// act
		m, err := parseNATSMetadata(fakeMetaData)
		// assert
		assert.Error(t, errors.New("nats error: missing nats URL"), err)
		assert.Empty(t, m.natsURL)
	})

}
