// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCloudEventsEnvelope(t *testing.T) {
	envelope := NewCloudEventsEnvelope("a", "source", "eventType", "", nil)
	assert.NotNil(t, envelope)
}

func TestCreateCloudEventsEnvelopeDefaults(t *testing.T) {
	t.Run("default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", nil)
		assert.Equal(t, DefaultCloudEventType, envelope.Type)
	})

	t.Run("non-default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "e1", "", nil)
		assert.Equal(t, "e1", envelope.Type)
	})

	t.Run("spec version", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", nil)
		assert.Equal(t, CloudEventsSpecVersion, envelope.SpecVersion)
	})

	t.Run("has data", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", []byte("data"))
		t.Logf("data: %v", envelope.Data)
		assert.Equal(t, "data", envelope.Data.(string))
	})

	t.Run("string data content type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", []byte("data"))
		assert.Equal(t, "text/plain", envelope.DataContentType)
	})

	t.Run("json data content type", func(t *testing.T) {
		str := `{ "data": "1" }`
		envelope := NewCloudEventsEnvelope("a", "source", "", "", []byte(str))
		assert.Equal(t, "application/json", envelope.DataContentType)
	})

	t.Run("cloud event content", func(t *testing.T) {
		str := `{
			"specversion" : "1.0",
			"type" : "com.github.pull.create",
			"source" : "https://github.com/cloudevents/spec/pull",
			"subject" : "123",
			"id" : "A234-1234-1234",
			"time" : "2018-04-05T17:31:00Z",
			"comexampleextension1" : "value",
			"comexampleothervalue" : 5,
			"datacontenttype" : "text/xml",
			"data" : "<much wow=\"xml\"/>"
		}`
		envelope := NewCloudEventsEnvelope("a", "", "", "", []byte(str))
		assert.Equal(t, "A234-1234-1234", envelope.ID)
		assert.Equal(t, "text/xml", envelope.DataContentType)
		assert.Equal(t, "1.0", envelope.SpecVersion)
	})
}
