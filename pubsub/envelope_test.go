// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestCreateCloudEventsEnvelope(t *testing.T) {
	envelope := NewCloudEventsEnvelope("a", "source", "eventType", "", "", "", "", []byte{})
	assert.NotNil(t, envelope)
}

func TestEnvelopeWrapExistingCloudEvents(t *testing.T) {
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
		envelope := NewCloudEventsEnvelope("a", "", "", "", "", "routed.topic", "mypubsub", []byte(str))
		var j interface{}
		err := jsoniter.ConfigFastest.Unmarshal(envelope.Data, &j)
		if err != nil {
			t.Error(err)
		}
		m, isMap := j.(map[string]interface{})
		if !isMap {
			t.Error("Unexpect Json Type")
		}
		assert.Equal(t, "A234-1234-1234", m["id"])
		assert.Equal(t, "text/xml", m["datacontenttype"])
		assert.Equal(t, "1.0", m["specversion"])
		assert.Equal(t, "routed.topic", envelope.Topic)
		assert.Equal(t, "mypubsub", envelope.PubsubName)
	})
}

func TestCreateCloudEventsEnvelopeDefaults(t *testing.T) {
	t.Run("default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "", "mypubsub", nil)
		assert.Equal(t, DefaultCloudEventType, envelope.Type)
	})

	t.Run("non-default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "e1", "", "", "", "mypubsub", nil)
		assert.Equal(t, "e1", envelope.Type)
	})

	t.Run("spec version", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "", "mypubsub", nil)
		assert.Equal(t, CloudEventsSpecVersion, envelope.SpecVersion)
	})

	t.Run("string data content type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "", "mypubsub", []byte("data"))
		assert.Equal(t, "application/json", envelope.DataContentType)
	})
}
