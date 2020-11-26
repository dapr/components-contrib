// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCloudEventsEnvelope(t *testing.T) {
	envelope := NewCloudEventsEnvelope("a", "source", "eventType", "", "", "", "", nil)
	assert.NotNil(t, envelope)
}

func TestEnvelopeXML(t *testing.T) {
	t.Run("xml content", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "application/xml", []byte(str))
		assert.Equal(t, "application/xml", envelope.DataContentType)
		assert.Equal(t, str, envelope.Data)
		assert.Equal(t, "1.0", envelope.SpecVersion)
		assert.Equal(t, "routed.topic", envelope.Topic)
		assert.Equal(t, "mypubsub", envelope.PubsubName)
	})

	t.Run("xml without content-type", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str))
		assert.Equal(t, "text/plain", envelope.DataContentType)
		assert.Equal(t, str, envelope.Data)
		assert.Equal(t, "1.0", envelope.SpecVersion)
		assert.Equal(t, "routed.topic", envelope.Topic)
		assert.Equal(t, "mypubsub", envelope.PubsubName)
	})
}

func TestEnvelopeUsingExistingCloudEvents(t *testing.T) {
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
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str))
		assert.Equal(t, "A234-1234-1234", envelope.ID)
		assert.Equal(t, "text/xml", envelope.DataContentType)
		assert.Equal(t, "1.0", envelope.SpecVersion)
		assert.Equal(t, "routed.topic", envelope.Topic)
		assert.Equal(t, "mypubsub", envelope.PubsubName)
	})
}

func TestCreateFromJSON(t *testing.T) {
	t.Run("has JSON object", func(t *testing.T) {
		obj1 := struct {
			Val1 string
			Val2 int
		}{
			"test",
			1,
		}
		data, _ := json.Marshal(obj1)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", data)
		t.Logf("data: %v", envelope.Data)
		assert.Equal(t, "application/json", envelope.DataContentType)

		obj2 := struct {
			Val1 string
			Val2 int
		}{}
		err := json.Unmarshal(data, &obj2)
		assert.NoError(t, err)
		assert.Equal(t, obj1.Val1, obj2.Val1)
		assert.Equal(t, obj1.Val2, obj2.Val2)
	})
}

func TestCreateCloudEventsEnvelopeDefaults(t *testing.T) {
	t.Run("default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", nil)
		assert.Equal(t, DefaultCloudEventType, envelope.Type)
	})

	t.Run("non-default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "e1", "", "", "mypubsub", "", nil)
		assert.Equal(t, "e1", envelope.Type)
	})

	t.Run("spec version", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", nil)
		assert.Equal(t, CloudEventsSpecVersion, envelope.SpecVersion)
	})

	t.Run("quoted data", func(t *testing.T) {
		list := []string{"v1", "v2", "v3"}
		data := strings.Join(list, ",")
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", []byte(data))
		t.Logf("data: %v", envelope.Data)
		assert.Equal(t, "text/plain", envelope.DataContentType)
		assert.Equal(t, data, envelope.Data.(string))
	})

	t.Run("string data content type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", []byte("data"))
		assert.Equal(t, "text/plain", envelope.DataContentType)
	})
}
