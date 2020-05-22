// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

import (
	"errors"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/stretchr/testify/assert"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		mqttURL:          "fakeUser:fakePassword@fake.mqtt.host:1883",
		mqttQOS:          "1",
		mqttRetain:       "true",
		mqttCleanSession: "false",
	}
}

func TestParseMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		m, err := parseMQTTMetaData(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.url)
		assert.Equal(t, byte(1), m.qos)
		assert.Equal(t, true, m.retain)
		assert.Equal(t, false, m.cleanSession)
	})

	t.Run("url is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[mqttURL] = ""

		m, err := parseMQTTMetaData(fakeMetaData)

		// assert
		assert.EqualError(t, err, errors.New("mqtt pub sub error: missing url").Error())
		assert.Equal(t, fakeProperties[mqttURL], m.url)
	})

	t.Run("qos and retain is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[mqttQOS] = ""
		fakeMetaData.Properties[mqttRetain] = ""

		m, err := parseMQTTMetaData(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.url)
		assert.Equal(t, byte(0), m.qos)
		assert.Equal(t, false, m.retain)
	})

	t.Run("invalid clean session field", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[mqttCleanSession] = "randomString"

		m, err := parseMQTTMetaData(fakeMetaData)

		// assert
		assert.Contains(t, err.Error(), "invalid clean session")
		assert.Equal(t, fakeProperties[mqttURL], m.url)
	})
}
