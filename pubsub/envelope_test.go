// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	dataContentTypeField = "datacontenttype"
	dataField            = "data"
	specVersionField     = "specversion"
	topicField           = "topic"
	pubsubNameField      = "pubsubname"
	typeField            = "type"
)

func TestCreateCloudEventsEnvelope(t *testing.T) {
	envelope := NewCloudEventsEnvelope("a", "source", "eventType", "", "", "", "", nil, "")
	assert.NotNil(t, envelope)
}

func TestEnvelopeXML(t *testing.T) {
	t.Run("xml content", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "application/xml", []byte(str), "")
		assert.Equal(t, "application/xml", envelope[dataContentTypeField])
		assert.Equal(t, str, envelope[dataField])
		assert.Equal(t, "1.0", envelope[specVersionField])
		assert.Equal(t, "routed.topic", envelope[topicField])
		assert.Equal(t, "mypubsub", envelope[pubsubNameField])
	})

	t.Run("xml without content-type", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		assert.Equal(t, "text/plain", envelope[dataContentTypeField])
		assert.Equal(t, str, envelope[dataField])
		assert.Equal(t, "1.0", envelope[specVersionField])
		assert.Equal(t, "routed.topic", envelope[topicField])
		assert.Equal(t, "mypubsub", envelope[pubsubNameField])
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
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", data, "1")
		t.Logf("data: %v", envelope[dataField])
		assert.Equal(t, "application/json", envelope[dataContentTypeField])

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
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", nil, "")
		assert.Equal(t, DefaultCloudEventType, envelope[typeField])
	})

	t.Run("non-default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "e1", "", "", "mypubsub", "", nil, "")
		assert.Equal(t, "e1", envelope[typeField])
	})

	t.Run("spec version", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", nil, "")
		assert.Equal(t, CloudEventsSpecVersion, envelope[specVersionField])
	})

	t.Run("quoted data", func(t *testing.T) {
		list := []string{"v1", "v2", "v3"}
		data := strings.Join(list, ",")
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", []byte(data), "")
		t.Logf("data: %v", envelope[dataField])
		assert.Equal(t, "text/plain", envelope[dataContentTypeField])
		assert.Equal(t, data, envelope[dataField].(string))
	})

	t.Run("string data content type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", []byte("data"), "")
		assert.Equal(t, "text/plain", envelope[dataContentTypeField])
	})

	t.Run("trace id", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", []byte("data"), "1")
		assert.Equal(t, "1", envelope[TraceIDField])
	})
}

func TestCreateCloudEventsEnvelopeExpiration(t *testing.T) {
	str := `{
		"specversion" : "1.0",
		"type" : "com.github.pull.create",
		"source" : "https://github.com/cloudevents/spec/pull",
		"subject" : "123",
		"id" : "A234-1234-1234",
		"comexampleextension1" : "value",
		"comexampleothervalue" : 5,
		"datacontenttype" : "text/xml",
		"data" : "<much wow=\"xml\"/>"
	}`

	t.Run("cloud event not expired", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		envelope[expirationField] = time.Now().UTC().Add(time.Hour * 24).Format(time.RFC3339)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event expired", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		envelope[expirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		assert.True(t, HasExpired(envelope))
	})

	t.Run("cloud event expired but applied new TTL from metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		envelope[expirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.NotEqual(t, "", envelope[expirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event TTL from metadata does not apply due to component feature", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		ApplyMetadata(envelope, []Feature{FeatureMessageTTL}, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.Equal(t, nil, envelope[expirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with max TTL metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": fmt.Sprintf("%v", math.MaxInt64),
		})
		assert.NotEqual(t, "", envelope[expirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with invalid expiration format", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		envelope[expirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC1123)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event without expiration", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event without expiration, without metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		ApplyMetadata(envelope, nil, map[string]string{})
		assert.False(t, HasExpired(envelope))
	})
}

func TestSetTraceID(t *testing.T) {
	t.Run("trace id is present", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
		}

		setTraceContext(m, "1")
		assert.Equal(t, "1", m[TraceIDField])
	})
}

func TestNewFromExisting(t *testing.T) {
	t.Run("valid cloudevent", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "1")
		assert.NoError(t, err)
		assert.Equal(t, "1.0", n["specversion"])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "1", n["traceid"])
	})

	t.Run("invalid cloudevent", func(t *testing.T) {
		_, err := FromCloudEvent([]byte("a"), "1")
		assert.Error(t, err)
	})
}
