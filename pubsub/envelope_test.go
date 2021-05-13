// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateCloudEventsEnvelope(t *testing.T) {
	envelope := NewCloudEventsEnvelope("a", "source", "eventType", "", "", "", "", nil, "")
	assert.NotNil(t, envelope)
}

func TestEnvelopeXML(t *testing.T) {
	t.Run("xml content", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "application/xml", []byte(str), "")
		assert.Equal(t, "application/xml", envelope[DataContentTypeField])
		assert.Equal(t, str, envelope[DataField])
		assert.Equal(t, "1.0", envelope[SpecVersionField])
		assert.Equal(t, "routed.topic", envelope[TopicField])
		assert.Equal(t, "mypubsub", envelope[PubsubField])
	})

	t.Run("xml without content-type", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
		assert.Equal(t, str, envelope[DataField])
		assert.Equal(t, "1.0", envelope[SpecVersionField])
		assert.Equal(t, "routed.topic", envelope[TopicField])
		assert.Equal(t, "mypubsub", envelope[PubsubField])
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
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "application/json", data, "1")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "application/json", envelope[DataContentTypeField])
		assert.Equal(t, map[string]interface{}{"Val1": "test", "Val2": float64(1)}, envelope[DataField])
	})

	t.Run("has JSON string with rich contenttype", func(t *testing.T) {
		obj1 := "message"
		data, _ := json.Marshal(obj1)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "application/JSON; charset=utf-8", data, "1")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "application/JSON; charset=utf-8", envelope[DataContentTypeField])
		assert.Equal(t, "message", envelope[DataField])
	})
}

func TestCreateCloudEventsEnvelopeDefaults(t *testing.T) {
	t.Run("default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", nil, "")
		assert.Equal(t, DefaultCloudEventType, envelope[TypeField])
	})

	t.Run("non-default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "e1", "", "", "mypubsub", "", nil, "")
		assert.Equal(t, "e1", envelope[TypeField])
	})

	t.Run("spec version", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", nil, "")
		assert.Equal(t, CloudEventsSpecVersion, envelope[SpecVersionField])
	})

	t.Run("quoted data", func(t *testing.T) {
		list := []string{"v1", "v2", "v3"}
		data := strings.Join(list, ",")
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", []byte(data), "")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
		assert.Equal(t, data, envelope[DataField].(string))
	})

	t.Run("has subject", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "subject1", "", "mypubsub", "", []byte(""), "")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "subject1", envelope[SubjectField])
	})

	t.Run("no subject", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "topic", "mypubsub", "", []byte(""), "")
		t.Logf("data: %v", envelope[DataField])
		assert.Empty(t, envelope[SubjectField])
	})

	t.Run("string data content type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "", "mypubsub", "", []byte("data"), "")
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
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
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * 24).Format(time.RFC3339)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event expired", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		assert.True(t, HasExpired(envelope))
	})

	t.Run("cloud event expired but applied new TTL from metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.NotEqual(t, "", envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event TTL from metadata does not apply due to component feature", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		ApplyMetadata(envelope, []Feature{FeatureMessageTTL}, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.Equal(t, nil, envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with max TTL metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": fmt.Sprintf("%v", math.MaxInt64),
		})
		assert.NotEqual(t, "", envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with invalid expiration format", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic", "mypubsub", "", []byte(str), "")
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC1123)
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

func TestNewFromExisting(t *testing.T) {
	t.Run("valid cloudevent", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1")
		assert.NoError(t, err)
		assert.Equal(t, "1.0", n["specversion"])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n["topic"])
		assert.Equal(t, "pubsub", n["pubsubname"])
		assert.Equal(t, "1", n["traceid"])
		assert.Nil(t, n["data"])
		assert.Nil(t, n["data_base64"])
	})

	t.Run("invalid cloudevent", func(t *testing.T) {
		_, err := FromCloudEvent([]byte("a"), "1", "", "")
		assert.Error(t, err)
	})

	t.Run("valid cloudevent with text data", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
			"data":        "hello world",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1")
		assert.NoError(t, err)
		assert.Equal(t, "1.0", n["specversion"])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n["topic"])
		assert.Equal(t, "pubsub", n["pubsubname"])
		assert.Equal(t, "1", n["traceid"])
		assert.Nil(t, n["data_base64"])
		assert.Equal(t, "hello world", n["data"])
	})

	t.Run("valid cloudevent with binary data", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
			"data_base64": base64.StdEncoding.EncodeToString([]byte{0x1}),
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1")
		assert.NoError(t, err)
		assert.Equal(t, "1.0", n["specversion"])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n["topic"])
		assert.Equal(t, "pubsub", n["pubsubname"])
		assert.Equal(t, "1", n["traceid"])
		assert.Nil(t, n["data"])
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{0x1}), n["data_base64"])
	})
}

func TestCreateFromBinaryPayload(t *testing.T) {
	base64Encoding := base64.StdEncoding.EncodeToString([]byte{0x1})
	envelope := NewCloudEventsEnvelope("", "", "", "", "", "", "application/octet-stream", []byte{0x1}, "trace")
	assert.Equal(t, base64Encoding, envelope[DataBase64Field])
	assert.Nil(t, envelope[DataField])
}

func TestNewFromRawPayload(t *testing.T) {
	t.Run("string data", func(t *testing.T) {
		n := FromRawPayload([]byte("hello world"), "mytopic", "mypubsub")
		assert.NotNil(t, n["id"])
		assert.Equal(t, "1.0", n["specversion"])
		assert.Equal(t, "mytopic", n["topic"])
		assert.Equal(t, "mypubsub", n["pubsubname"])
		assert.Nil(t, n["traceid"])
		assert.Nil(t, n["data"])
		assert.Equal(t, "aGVsbG8gd29ybGQ=", n["data_base64"])
	})
}
