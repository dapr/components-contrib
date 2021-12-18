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
	envelope := NewCloudEventsEnvelope("a", "source", "eventType", "", "",
		"", "", nil, "", "", nil)
	assert.NotNil(t, envelope)
}

func TestEnvelopeXML(t *testing.T) {
	t.Run("xml content", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "application/xml", []byte(str), "", "", nil)
		assert.Equal(t, "application/xml", envelope[DataContentTypeField])
		assert.Equal(t, str, envelope[DataField])
		assert.Equal(t, "1.0", envelope[SpecVersionField])
		assert.Equal(t, "routed.topic", envelope[TopicField])
		assert.Equal(t, "mypubsub", envelope[PubsubField])
	})

	t.Run("xml without content-type", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
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
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "application/json", data, "1", "key=value", nil)
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "application/json", envelope[DataContentTypeField])
		assert.Equal(t, map[string]interface{}{"Val1": "test", "Val2": float64(1)}, envelope[DataField])
	})

	t.Run("has JSON string with rich contenttype", func(t *testing.T) {
		obj1 := "message"
		data, _ := json.Marshal(obj1)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "application/JSON; charset=utf-8", data,
			"1", "key=value", nil)
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "application/JSON; charset=utf-8", envelope[DataContentTypeField])
		assert.Equal(t, "message", envelope[DataField])
	})
}

func TestCreateCloudEventsEnvelopeDefaults(t *testing.T) {
	t.Run("default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", nil, "", "", nil)
		assert.Equal(t, DefaultCloudEventType, envelope[TypeField])
	})

	t.Run("non-default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "e1", "", "",
			"mypubsub", "", nil, "", "", nil)
		assert.Equal(t, "e1", envelope[TypeField])
	})

	t.Run("spec version", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", nil, "", "", nil)
		assert.Equal(t, CloudEventsSpecVersion, envelope[SpecVersionField])
	})

	t.Run("quoted data", func(t *testing.T) {
		list := []string{"v1", "v2", "v3"}
		data := strings.Join(list, ",")
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte(data), "", "", nil)
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
		assert.Equal(t, data, envelope[DataField].(string))
	})

	t.Run("has subject", func(t *testing.T) {
		attributes := map[string]interface{}{
			SubjectField: "subject1",
		}
		attrBytes, _ := json.Marshal(attributes)
		envelope := NewCloudEventsEnvelope("a", "source", "", "subject1", "",
			"mypubsub", "", []byte(""), "", "", attrBytes)
		t.Logf("data: %v", envelope[DataField])

		assert.Equal(t, "subject1", envelope[SubjectField])

		var actual map[string]interface{}
		err := json.Unmarshal(envelope[AttributesField].([]byte), &actual)

		assert.Equal(t, 1, len(actual))
		assert.Nil(t, err)
		assert.Equal(t, "subject1", actual[SubjectField])
	})

	t.Run("no subject", func(t *testing.T) {
		attributes := map[string]interface{}{}
		attrBytes, _ := json.Marshal(attributes)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "topic",
			"mypubsub", "", []byte(""), "", "", attrBytes)
		t.Logf("data: %v", envelope[DataField])

		assert.Empty(t, envelope[SubjectField])

		var actual map[string]interface{}
		err := json.Unmarshal(envelope[AttributesField].([]byte), &actual)

		assert.Equal(t, 0, len(actual))
		assert.Nil(t, err)
		assert.Empty(t, actual[SubjectField])
	})

	t.Run("has dataschema", func(t *testing.T) {
		attributes := map[string]interface{}{
			DataSchemaField: "http://localhost:8080/data",
		}
		attrBytes, _ := json.Marshal(attributes)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte(""), "", "", attrBytes)
		t.Logf("data: %v", envelope[DataField])

		var actual map[string]interface{}
		err := json.Unmarshal(envelope[AttributesField].([]byte), &actual)

		assert.Equal(t, 1, len(actual))
		assert.Nil(t, err)
		assert.Equal(t, "http://localhost:8080/data", actual[DataSchemaField])
	})

	t.Run("no dataschema", func(t *testing.T) {
		attributes := map[string]interface{}{}
		attrBytes, _ := json.Marshal(attributes)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "topic",
			"mypubsub", "", []byte(""), "", "", attrBytes)
		t.Logf("data: %v", envelope[DataField])

		var actual map[string]interface{}
		err := json.Unmarshal(envelope[AttributesField].([]byte), &actual)

		assert.Equal(t, 0, len(actual))
		assert.Nil(t, err)
		assert.Empty(t, actual[DataSchemaField])
	})

	t.Run("has time", func(t *testing.T) {
		timeAsString := time.Now().String()
		attributes := map[string]interface{}{
			TimeField: timeAsString,
		}
		attrBytes, _ := json.Marshal(attributes)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte(""), "", "", attrBytes)
		t.Logf("data: %v", envelope[DataField])

		var actual map[string]interface{}
		err := json.Unmarshal(envelope[AttributesField].([]byte), &actual)

		assert.Equal(t, 1, len(actual))
		assert.Nil(t, err)
		assert.Equal(t, timeAsString, actual[TimeField])
	})

	t.Run("no time", func(t *testing.T) {
		attributes := map[string]interface{}{}
		attrBytes, _ := json.Marshal(attributes)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "topic",
			"mypubsub", "", []byte(""), "", "", attrBytes)
		t.Logf("data: %v", envelope[DataField])

		var actual map[string]interface{}
		err := json.Unmarshal(envelope[AttributesField].([]byte), &actual)

		assert.Equal(t, 0, len(actual))
		assert.Nil(t, err)
		assert.Empty(t, actual[TimeField])
	})

	t.Run("has extension attributes", func(t *testing.T) {
		attributes := map[string]interface{}{
			"extension": "extension1",
		}
		attrBytes, _ := json.Marshal(attributes)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte(""), "", "", attrBytes)
		t.Logf("data: %v", envelope[DataField])

		var actual map[string]interface{}
		err := json.Unmarshal(envelope[AttributesField].([]byte), &actual)

		assert.Equal(t, 1, len(actual))
		assert.Nil(t, err)
		assert.Equal(t, "extension1", actual["extension"])
	})

	t.Run("no extension attributes", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "topic",
			"mypubsub", "", []byte(""), "", "", nil)
		t.Logf("data: %v", envelope[DataField])

		assert.Nil(t, envelope[AttributesField])
	})

	t.Run("string data content type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte("data"), "", "", nil)
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
	})

	t.Run("trace id", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte("data"), "1", "", nil)
		assert.Equal(t, "1", envelope[TraceIDField])
	})

	t.Run("trace state", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte("data"), "1", "key=value", nil)
		assert.Equal(t, "key=value", envelope[TraceStateField])
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
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * 24).Format(time.RFC3339)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event expired", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		assert.True(t, HasExpired(envelope))
	})

	t.Run("cloud event expired but applied new TTL from metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.NotEqual(t, "", envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event TTL from metadata does not apply due to component feature", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
		ApplyMetadata(envelope, []Feature{FeatureMessageTTL}, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.Equal(t, nil, envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with max TTL metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": fmt.Sprintf("%v", math.MaxInt64),
		})
		assert.NotEqual(t, "", envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with invalid expiration format", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC1123)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event without expiration", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event without expiration, without metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "", nil)
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

		n, err := FromCloudEvent(b, "b", "pubsub", "1", "key=value")
		assert.NoError(t, err)
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n[TopicField])
		assert.Equal(t, "pubsub", n[PubsubField])
		assert.Equal(t, "1", n[TraceIDField])
		assert.Equal(t, "key=value", n[TraceStateField])
		assert.Nil(t, n[DataField])
		assert.Nil(t, n[DataBase64Field])
	})

	t.Run("invalid cloudevent", func(t *testing.T) {
		_, err := FromCloudEvent([]byte("a"), "1", "", "", "")
		assert.Error(t, err)
	})

	t.Run("valid cloudevent with text data", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
			"data":        "hello world",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1", "key=value")
		assert.NoError(t, err)
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n[TopicField])
		assert.Equal(t, "pubsub", n[PubsubField])
		assert.Equal(t, "1", n[TraceIDField])
		assert.Equal(t, "key=value", n[TraceStateField])
		assert.Nil(t, n[DataBase64Field])
		assert.Equal(t, "hello world", n[DataField])
	})

	t.Run("valid cloudevent with binary data", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
			"data_base64": base64.StdEncoding.EncodeToString([]byte{0x1}),
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1", "key=value")
		assert.NoError(t, err)
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n[TopicField])
		assert.Equal(t, "pubsub", n[PubsubField])
		assert.Equal(t, "1", n[TraceIDField])
		assert.Equal(t, "key=value", n[TraceStateField])
		assert.Nil(t, n[DataField])
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{0x1}), n[DataBase64Field])
	})
}

func TestCreateFromBinaryPayload(t *testing.T) {
	base64Encoding := base64.StdEncoding.EncodeToString([]byte{0x1})
	envelope := NewCloudEventsEnvelope("", "", "", "", "", "",
		"application/octet-stream", []byte{0x1}, "trace", "", nil)
	assert.Equal(t, base64Encoding, envelope[DataBase64Field])
	assert.Nil(t, envelope[DataField])
}

func TestNewFromRawPayload(t *testing.T) {
	t.Run("string data", func(t *testing.T) {
		n := FromRawPayload([]byte("hello world"), "mytopic", "mypubsub")
		assert.NotNil(t, n[IDField])
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "mytopic", n[TopicField])
		assert.Equal(t, "mypubsub", n[PubsubField])
		assert.Nil(t, n["traceid"])
		assert.Nil(t, n[TraceIDField])
		assert.Nil(t, n[DataField])
		assert.Equal(t, "aGVsbG8gd29ybGQ=", n[DataBase64Field])
	})
}
