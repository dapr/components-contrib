/*
Copyright 2026 The Dapr Authors
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

package pulsar

import (
	"encoding/json"
	"testing"

	goavro "github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrapInCloudEventsAvroSchema(t *testing.T) {
	innerSchema := `{
		"type": "record",
		"name": "OrderEvent",
		"namespace": "com.example",
		"fields": [
			{"name": "orderId", "type": "string"},
			{"name": "amount", "type": "double"}
		]
	}`

	t.Run("generates valid Avro schema", func(t *testing.T) {
		ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
		require.NoError(t, err)

		// Must compile as a valid Avro schema
		codec, err := goavro.NewCodecForStandardJSONFull(ceSchema)
		require.NoError(t, err)
		assert.NotNil(t, codec)
	})

	t.Run("contains all CloudEvents required fields", func(t *testing.T) {
		ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
		require.NoError(t, err)

		var schema map[string]interface{}
		err = json.Unmarshal([]byte(ceSchema), &schema)
		require.NoError(t, err)

		assert.Equal(t, "CloudEvent", schema["name"])
		assert.Equal(t, "io.cloudevents", schema["namespace"])

		fields := schema["fields"].([]interface{})
		fieldNames := make([]string, len(fields))
		for i, f := range fields {
			fieldNames[i] = f.(map[string]interface{})["name"].(string)
		}

		// Required CE attributes
		assert.Contains(t, fieldNames, "id")
		assert.Contains(t, fieldNames, "source")
		assert.Contains(t, fieldNames, "specversion")
		assert.Contains(t, fieldNames, "type")
		// Optional CE attributes
		assert.Contains(t, fieldNames, "datacontenttype")
		assert.Contains(t, fieldNames, "subject")
		assert.Contains(t, fieldNames, "time")
		// Dapr extension attributes
		assert.Contains(t, fieldNames, "topic")
		assert.Contains(t, fieldNames, "pubsubname")
		assert.Contains(t, fieldNames, "traceid")
		assert.Contains(t, fieldNames, "traceparent")
		assert.Contains(t, fieldNames, "tracestate")
		// Data field
		assert.Contains(t, fieldNames, "data")
	})

	t.Run("validates a full CloudEvents envelope", func(t *testing.T) {
		ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
		require.NoError(t, err)

		codec, err := goavro.NewCodecForStandardJSONFull(ceSchema)
		require.NoError(t, err)

		// Standard JSON encoding: union values are plain values, not {"type": value}
		ceJSON := `{
			"id": "abc-123",
			"source": "Dapr",
			"specversion": "1.0",
			"type": "com.dapr.event.sent",
			"datacontenttype": "application/json",
			"subject": null,
			"time": "2026-03-20T10:00:00Z",
			"topic": "orders",
			"pubsubname": "mypubsub",
			"traceid": null,
			"traceparent": null,
			"tracestate": null,
			"expiration": null,
			"data": {"orderId": "order-1", "amount": 99.99},
			"data_base64": null
		}`

		native, _, err := codec.NativeFromTextual([]byte(ceJSON))
		require.NoError(t, err)
		assert.NotNil(t, native)
	})

	t.Run("rejects envelope missing required CE field", func(t *testing.T) {
		ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
		require.NoError(t, err)

		codec, err := goavro.NewCodecForStandardJSONFull(ceSchema)
		require.NoError(t, err)

		// Missing "source" field
		ceJSON := `{
			"id": "abc-123",
			"specversion": "1.0",
			"type": "com.dapr.event.sent",
			"datacontenttype": null,
			"subject": null,
			"time": null,
			"topic": null,
			"pubsubname": null,
			"traceid": null,
			"traceparent": null,
			"tracestate": null,
			"expiration": null,
			"data": null,
			"data_base64": null
		}`

		_, _, err = codec.NativeFromTextual([]byte(ceJSON))
		require.Error(t, err)
	})

	t.Run("validates envelope with null data", func(t *testing.T) {
		ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
		require.NoError(t, err)

		codec, err := goavro.NewCodecForStandardJSONFull(ceSchema)
		require.NoError(t, err)

		ceJSON := `{
			"id": "abc-123",
			"source": "Dapr",
			"specversion": "1.0",
			"type": "com.dapr.event.sent",
			"datacontenttype": null,
			"subject": null,
			"time": null,
			"topic": null,
			"pubsubname": null,
			"traceid": null,
			"traceparent": null,
			"tracestate": null,
			"expiration": null,
			"data": null,
			"data_base64": null
		}`

		native, _, err := codec.NativeFromTextual([]byte(ceJSON))
		require.NoError(t, err)
		assert.NotNil(t, native)
	})
}

func TestWrapInCloudEventsAvroSchema_NestedRecord(t *testing.T) {
	innerSchema := `{
		"type": "record",
		"name": "Enrollment",
		"namespace": "test",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "student", "type": {
				"type": "record",
				"name": "Student",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "age", "type": "int"}
				]
			}}
		]
	}`

	ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
	require.NoError(t, err)

	codec, err := goavro.NewCodecForStandardJSONFull(ceSchema)
	require.NoError(t, err)

	ceJSON := `{
		"id": "evt-1",
		"source": "Dapr",
		"specversion": "1.0",
		"type": "com.dapr.event.sent",
		"datacontenttype": null,
		"subject": null,
		"time": null,
		"topic": null,
		"pubsubname": null,
		"traceid": null,
		"traceparent": null,
		"tracestate": null,
		"expiration": null,
		"data": {"id": 1, "student": {"name": "Alice", "age": 20}},
		"data_base64": null
	}`

	native, _, err := codec.NativeFromTextual([]byte(ceJSON))
	require.NoError(t, err)
	assert.NotNil(t, native)
}

func TestWrapInCloudEventsAvroSchema_Expiration(t *testing.T) {
	innerSchema := `{
		"type": "record",
		"name": "Event",
		"fields": [{"name": "id", "type": "int"}]
	}`

	ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
	require.NoError(t, err)

	codec, err := goavro.NewCodecForStandardJSONFull(ceSchema)
	require.NoError(t, err)

	ceJSON := `{
		"id": "evt-1",
		"source": "Dapr",
		"specversion": "1.0",
		"type": "com.dapr.event.sent",
		"datacontenttype": null,
		"subject": null,
		"time": null,
		"topic": null,
		"pubsubname": null,
		"traceid": null,
		"traceparent": null,
		"tracestate": null,
		"expiration": "2026-03-20T12:00:00Z",
		"data": {"id": 1},
		"data_base64": null
	}`

	native, _, err := codec.NativeFromTextual([]byte(ceJSON))
	require.NoError(t, err)
	assert.NotNil(t, native)
}

func TestWrapInCloudEventsAvroSchema_DataBase64(t *testing.T) {
	innerSchema := `{
		"type": "record",
		"name": "Event",
		"fields": [{"name": "id", "type": "int"}]
	}`

	ceSchema, err := wrapInCloudEventsAvroSchema(innerSchema)
	require.NoError(t, err)

	codec, err := goavro.NewCodecForStandardJSONFull(ceSchema)
	require.NoError(t, err)

	// Binary content uses data_base64 instead of data
	ceJSON := `{
		"id": "evt-1",
		"source": "Dapr",
		"specversion": "1.0",
		"type": "com.dapr.event.sent",
		"datacontenttype": "application/octet-stream",
		"subject": null,
		"time": null,
		"topic": null,
		"pubsubname": null,
		"traceid": null,
		"traceparent": null,
		"tracestate": null,
		"expiration": null,
		"data": null,
		"data_base64": "SGVsbG8gV29ybGQ="
	}`

	native, _, err := codec.NativeFromTextual([]byte(ceJSON))
	require.NoError(t, err)
	assert.NotNil(t, native)
}

func TestWrapInCloudEventsAvroSchema_InvalidInput(t *testing.T) {
	t.Run("malformed JSON", func(t *testing.T) {
		_, err := wrapInCloudEventsAvroSchema(`{not valid json}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse inner Avro schema")
	})

	t.Run("empty string", func(t *testing.T) {
		_, err := wrapInCloudEventsAvroSchema(``)
		require.Error(t, err)
	})
}
