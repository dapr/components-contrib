/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pubsub

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	format "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	contribContenttype "github.com/dapr/components-contrib/contenttype"
)

func TestCreateCloudEventsEnvelope(t *testing.T) {
	envelope := NewCloudEventsEnvelope("a", "source", "eventType", "", "",
		"", "", nil, "", "")
	assert.NotNil(t, envelope)
}

func TestEnvelopeXML(t *testing.T) {
	t.Run("xml content", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "application/xml", []byte(str), "", "")
		assert.Equal(t, "application/xml", envelope[DataContentTypeField])
		assert.Equal(t, str, envelope[DataField])
		assert.Equal(t, "1.0", envelope[SpecVersionField])
		assert.Equal(t, "routed.topic", envelope[TopicField])
		assert.Equal(t, "mypubsub", envelope[PubsubField])
		assert.NotNil(t, envelope[TimeField])
	})

	t.Run("xml without content-type", func(t *testing.T) {
		str := `<root/>`
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
		assert.Equal(t, str, envelope[DataField])
		assert.Equal(t, "1.0", envelope[SpecVersionField])
		assert.Equal(t, "routed.topic", envelope[TopicField])
		assert.Equal(t, "mypubsub", envelope[PubsubField])
		assert.NotNil(t, envelope[TimeField])
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
			"mypubsub", "application/json", data, "1", "key=value")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "application/json", envelope[DataContentTypeField])
		assert.Equal(t, map[string]interface{}{"Val1": "test", "Val2": json.Number("1")}, envelope[DataField])
	})

	t.Run("has JSON object with large number", func(t *testing.T) {
		obj1 := struct {
			Val1 string
			Val2 int
		}{
			"test",
			637831415180045507,
		}
		data, _ := json.Marshal(obj1)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "application/json", data, "1", "key=value")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "application/json", envelope[DataContentTypeField])
		assert.Equal(t, map[string]interface{}{"Val1": "test", "Val2": json.Number("637831415180045507")}, envelope[DataField])
	})

	t.Run("has JSON string with rich contenttype", func(t *testing.T) {
		obj1 := "message"
		data, _ := json.Marshal(obj1)
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "application/JSON; charset=utf-8", data,
			"1", "key=value")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "application/JSON; charset=utf-8", envelope[DataContentTypeField])
		assert.Equal(t, "message", envelope[DataField])
	})
}

func TestCreateCloudEventsEnvelopeDefaults(t *testing.T) {
	t.Run("default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", nil, "", "")
		assert.Equal(t, DefaultCloudEventType, envelope[TypeField])
	})

	t.Run("non-default event type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "e1", "", "",
			"mypubsub", "", nil, "", "")
		assert.Equal(t, "e1", envelope[TypeField])
	})

	t.Run("spec version", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", nil, "", "")
		assert.Equal(t, CloudEventsSpecVersion, envelope[SpecVersionField])
	})

	t.Run("quoted data", func(t *testing.T) {
		list := []string{"v1", "v2", "v3"}
		data := strings.Join(list, ",")
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte(data), "", "")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
		assert.Equal(t, data, envelope[DataField].(string))
	})

	t.Run("has subject", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "subject1", "",
			"mypubsub", "", []byte(""), "", "")
		t.Logf("data: %v", envelope[DataField])
		assert.Equal(t, "subject1", envelope[SubjectField])
	})

	t.Run("no subject", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "topic",
			"mypubsub", "", []byte(""), "", "")
		t.Logf("data: %v", envelope[DataField])
		assert.Empty(t, envelope[SubjectField])
	})

	t.Run("string data content type", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte("data"), "", "")
		assert.Equal(t, "text/plain", envelope[DataContentTypeField])
	})

	t.Run("trace id", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte("data"), "1", "")
		assert.Equal(t, "1", envelope[TraceParentField])
	})

	t.Run("trace state", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "source", "", "", "",
			"mypubsub", "", []byte("data"), "1", "key=value")
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
			"mypubsub", "", []byte(str), "", "")
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * 24).Format(time.RFC3339)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event expired", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		assert.True(t, HasExpired(envelope))
	})

	t.Run("cloud event expired but applied new TTL from metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC3339)
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.NotEqual(t, "", envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event TTL from metadata does not apply due to component feature", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		ApplyMetadata(envelope, []Feature{FeatureMessageTTL}, map[string]string{
			"ttlInSeconds": "10000",
		})
		assert.Nil(t, envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with max TTL metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		ApplyMetadata(envelope, nil, map[string]string{
			"ttlInSeconds": strconv.Itoa(math.MaxInt64),
		})
		assert.NotEqual(t, "", envelope[ExpirationField])
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event with invalid expiration format", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		envelope[ExpirationField] = time.Now().UTC().Add(time.Hour * -24).Format(time.RFC1123)
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event without expiration", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		assert.False(t, HasExpired(envelope))
	})

	t.Run("cloud event without expiration, without metadata", func(t *testing.T) {
		envelope := NewCloudEventsEnvelope("a", "", "", "", "routed.topic",
			"mypubsub", "", []byte(str), "", "")
		ApplyMetadata(envelope, nil, map[string]string{})
		assert.False(t, HasExpired(envelope))
	})
}

func TestNewFromExisting(t *testing.T) {
	t.Run("valid cloudevent", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
			"time":        "2021-08-02T09:00:00Z",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1", "key=value")
		require.NoError(t, err)
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n[TopicField])
		assert.Equal(t, "pubsub", n[PubsubField])
		assert.Equal(t, "1", n[TraceParentField])
		assert.Equal(t, "key=value", n[TraceStateField])
		assert.Equal(t, "2021-08-02T09:00:00Z", n[TimeField])
		assert.Nil(t, n[DataField])
		assert.Nil(t, n[DataBase64Field])
	})

	t.Run("invalid cloudevent", func(t *testing.T) {
		_, err := FromCloudEvent([]byte("a"), "1", "", "", "")
		require.Error(t, err)
	})

	t.Run("valid cloudevent with text data", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
			"data":        "hello world",
			"time":        "2021-08-02T09:00:00Z",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1", "key=value")
		require.NoError(t, err)
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n[TopicField])
		assert.Equal(t, "pubsub", n[PubsubField])
		assert.Equal(t, "1", n[TraceParentField])
		assert.Equal(t, "key=value", n[TraceStateField])
		assert.Equal(t, "2021-08-02T09:00:00Z", n[TimeField])
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
		require.NoError(t, err)
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "a", n["customfield"])
		assert.Equal(t, "b", n[TopicField])
		assert.Equal(t, "pubsub", n[PubsubField])
		assert.Equal(t, "1", n[TraceParentField])
		assert.Equal(t, "key=value", n[TraceStateField])
		assert.NotNil(t, n[TimeField])
		assert.Nil(t, n[DataField])
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{0x1}), n[DataBase64Field])
	})

	t.Run("populate traceid, traceparent and tracestate when provided via metadata", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion": "1.0",
			"customfield": "a",
			"time":        "2021-08-02T09:00:00Z",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "1", "2")
		require.NoError(t, err)
		assert.Equal(t, "1", n[TraceIDField])
		assert.Equal(t, "1", n[TraceParentField])
		assert.Equal(t, "2", n[TraceStateField])
	})

	t.Run("populate traceid, traceparent and tracestate from existing cloudevent", func(t *testing.T) {
		m := map[string]interface{}{
			"specversion":    "1.0",
			"customfield":    "a",
			"time":           "2021-08-02T09:00:00Z",
			TraceIDField:     "e",
			TraceStateField:  "f",
			TraceParentField: "g",
		}
		b, _ := json.Marshal(&m)

		n, err := FromCloudEvent(b, "b", "pubsub", "", "")
		require.NoError(t, err)
		assert.Equal(t, "e", n[TraceIDField])
		assert.Equal(t, "f", n[TraceStateField])
		assert.Equal(t, "g", n[TraceParentField])
	})
}

func TestCreateFromBinaryPayload(t *testing.T) {
	base64Encoding := base64.StdEncoding.EncodeToString([]byte{0x1})
	envelope := NewCloudEventsEnvelope("", "", "", "", "", "",
		"application/octet-stream", []byte{0x1}, "trace", "")
	assert.Equal(t, base64Encoding, envelope[DataBase64Field])
	assert.NotNil(t, envelope[TimeField])
	assert.Nil(t, envelope[DataField])
}

func TestCreateFromCloudEventsProtobufPayload(t *testing.T) {
	jsondata := make(map[string]interface{})
	jsondata["string"] = "hello world"
	jsondata["number"] = 3.1415
	jsonbytes, _ := json.Marshal(jsondata)

	myevent := event.New()
	myevent.SetData("application/json", jsonbytes)
	myevent.SetID("1234")

	ceProtoBytes, _ := format.Protobuf.Marshal(&myevent)
	ceProtoBytesBase64Encoding := base64.StdEncoding.EncodeToString(ceProtoBytes)

	contenttypes := []string{contribContenttype.CloudEventProtobufContentType, contribContenttype.ProtobufContentType}

	for i := range contenttypes {
		envelope := NewCloudEventsEnvelope("", "", "", "", "", "",
			contenttypes[i], ceProtoBytes, "trace", "")

		assert.Equal(t, ceProtoBytesBase64Encoding, envelope[DataBase64Field])
		assert.Nil(t, envelope[DataField])
		assert.NotNil(t, envelope[TimeField])

		receivedProtoBytes, _ := base64.StdEncoding.DecodeString(envelope[DataBase64Field].(string))

		var e event.Event
		format.Protobuf.Unmarshal(receivedProtoBytes, &e)

		assert.Equal(t, "1234", e.Context.GetID())
		var receivedjsondata map[string]interface{}
		_ = json.Unmarshal(e.Data(), &receivedjsondata)
		assert.Equal(t, "hello world", receivedjsondata["string"])
		assert.Equal(t, 3.1415, receivedjsondata["number"])
	}
}

func TestNewFromRawPayload(t *testing.T) {
	t.Run("string data", func(t *testing.T) {
		n := FromRawPayload([]byte("hello world"), "mytopic", "mypubsub")
		assert.NotNil(t, n[IDField])
		assert.Equal(t, "1.0", n[SpecVersionField])
		assert.Equal(t, "mytopic", n[TopicField])
		assert.Equal(t, "mypubsub", n[PubsubField])
		assert.Nil(t, n[TraceParentField])
		assert.Nil(t, n[DataField])
		assert.Equal(t, "aGVsbG8gd29ybGQ=", n[DataBase64Field])
	})
}
