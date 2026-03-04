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

package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	goavro "github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// newAvroSchemaMetadata creates a schemaMetadata with a pre-compiled goavro codec,
// matching the production path where codecs are compiled once at init.
func newAvroSchemaMetadata(t *testing.T, avroSchemaJSON string) schemaMetadata {
	t.Helper()
	codec, err := goavro.NewCodecForStandardJSONFull(avroSchemaJSON)
	require.NoError(t, err, "failed to compile test avro schema")
	return schemaMetadata{
		protocol: avroProtocol,
		value:    avroSchemaJSON,
		codec:    codec,
	}
}

func TestParsePulsarMetadata(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{
		"host":                    "a",
		"enableTLS":               "false",
		"disableBatching":         "true",
		"batchingMaxPublishDelay": "5s",
		"batchingMaxSize":         "100",
		"batchingMaxMessages":     "200",
		"maxConcurrentHandlers":   "333",
	}
	meta, err := parsePulsarMetadata(m)

	require.NoError(t, err)
	assert.Equal(t, "a", meta.Host)
	assert.False(t, meta.EnableTLS)
	assert.True(t, meta.DisableBatching)
	assert.Equal(t, defaultTenant, meta.Tenant)
	assert.Equal(t, defaultNamespace, meta.Namespace)
	assert.Equal(t, 5*time.Second, meta.BatchingMaxPublishDelay)
	assert.Equal(t, uint(100), meta.BatchingMaxSize)
	assert.Equal(t, uint(200), meta.BatchingMaxMessages)
	assert.Equal(t, uint(333), meta.MaxConcurrentHandlers)
	assert.Empty(t, meta.internalTopicSchemas)
	assert.Equal(t, "shared", meta.SubscriptionType)
}

func TestParsePulsarMetadataSubscriptionType(t *testing.T) {
	tt := []struct {
		name          string
		subscribeType string
		expected      string
		err           bool
	}{
		{
			name:          "test valid subscribe type - key_shared",
			subscribeType: "key_shared",
			expected:      "key_shared",
			err:           false,
		},
		{
			name:          "test valid subscribe type - shared",
			subscribeType: "shared",
			expected:      "shared",
			err:           false,
		},
		{
			name:          "test valid subscribe type - failover",
			subscribeType: "failover",
			expected:      "failover",
			err:           false,
		},
		{
			name:          "test valid subscribe type - exclusive",
			subscribeType: "exclusive",
			expected:      "exclusive",
			err:           false,
		},
		{
			name:          "test valid subscribe type - empty",
			subscribeType: "",
			expected:      "shared",
			err:           false,
		},
		{
			name:          "test invalid subscribe type",
			subscribeType: "invalid",
			err:           true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}

			m.Properties = map[string]string{
				"host":          "a",
				"subscribeType": tc.subscribeType,
			}
			meta, err := parsePulsarMetadata(m)

			if tc.err {
				require.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expected, meta.SubscriptionType)
		})
	}
}

func TestParsePulsarMetadataSubscriptionInitialPosition(t *testing.T) {
	tt := []struct {
		name                     string
		subscribeInitialPosition string
		expected                 string
		err                      bool
	}{
		{
			name:                     "test valid subscribe initial position - earliest",
			subscribeInitialPosition: "earliest",
			expected:                 "earliest",
			err:                      false,
		},
		{
			name:                     "test valid subscribe initial position - latest",
			subscribeInitialPosition: "latest",
			expected:                 "latest",
			err:                      false,
		},
		{
			name:                     "test valid subscribe initial position - empty",
			subscribeInitialPosition: "",
			expected:                 "latest",
			err:                      false,
		},
		{
			name:                     "test invalid subscribe initial position",
			subscribeInitialPosition: "invalid",
			err:                      true,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}

			m.Properties = map[string]string{
				"host":                     "a",
				"subscribeInitialPosition": tc.subscribeInitialPosition,
			}
			meta, err := parsePulsarMetadata(m)

			if tc.err {
				require.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expected, meta.SubscriptionInitialPosition)
		})
	}
}

func TestParsePulsarMetadataSubscriptionMode(t *testing.T) {
	tt := []struct {
		name          string
		subscribeMode string
		expected      string
		err           bool
	}{
		{
			name:          "test valid subscribe mode - durable",
			subscribeMode: "durable",
			expected:      "durable",
			err:           false,
		},
		{
			name:          "test valid subscribe mode - non_durable",
			subscribeMode: "non_durable",
			expected:      "non_durable",
			err:           false,
		},
		{
			name:          "test valid subscribe mode - empty",
			subscribeMode: "",
			expected:      "durable",
			err:           false,
		},
		{
			name:          "test invalid subscribe mode",
			subscribeMode: "invalid",
			err:           true,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}

			m.Properties = map[string]string{
				"host":          "a",
				"subscribeMode": tc.subscribeMode,
			}
			meta, err := parsePulsarMetadata(m)

			if tc.err {
				require.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expected, meta.SubscriptionMode)
		})
	}
}

func TestParsePulsarMetadataSubscriptionCombination(t *testing.T) {
	tt := []struct {
		name                     string
		subscribeType            string
		subscribeInitialPosition string
		subscribeMode            string
		expectedType             string
		expectedInitialPosition  string
		expectedMode             string
		err                      bool
	}{
		{
			name:                     "test valid subscribe - default",
			subscribeType:            "",
			subscribeInitialPosition: "",
			subscribeMode:            "",
			expectedType:             "shared",
			expectedInitialPosition:  "latest",
			expectedMode:             "durable",
			err:                      false,
		},
		{
			name:                     "test valid subscribe - pass case 1",
			subscribeType:            "key_shared",
			subscribeInitialPosition: "earliest",
			subscribeMode:            "non_durable",
			expectedType:             "key_shared",
			expectedInitialPosition:  "earliest",
			expectedMode:             "non_durable",
			err:                      false,
		},
		{
			name:                     "test valid subscribe - pass case 2",
			subscribeType:            "exclusive",
			subscribeInitialPosition: "latest",
			subscribeMode:            "durable",
			expectedType:             "exclusive",
			expectedInitialPosition:  "latest",
			expectedMode:             "durable",
			err:                      false,
		},
		{
			name:                     "test valid subscribe - pass case 3",
			subscribeType:            "failover",
			subscribeInitialPosition: "earliest",
			subscribeMode:            "durable",
			expectedType:             "failover",
			expectedInitialPosition:  "earliest",
			expectedMode:             "durable",
			err:                      false,
		},
		{
			name:                     "test valid subscribe - pass case 4",
			subscribeType:            "shared",
			subscribeInitialPosition: "latest",
			subscribeMode:            "non_durable",
			expectedType:             "shared",
			expectedInitialPosition:  "latest",
			expectedMode:             "non_durable",
			err:                      false,
		},
		{
			name:          "test valid subscribe - fail case 1",
			subscribeType: "invalid",
			err:           true,
		},
		{
			name:                     "test valid subscribe - fail case 2",
			subscribeInitialPosition: "invalid",
			err:                      true,
		},
		{
			name:          "test valid subscribe - fail case 3",
			subscribeMode: "invalid",
			err:           true,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}

			m.Properties = map[string]string{
				"host":                     "a",
				"subscribeType":            tc.subscribeType,
				"subscribeInitialPosition": tc.subscribeInitialPosition,
				"subscribeMode":            tc.subscribeMode,
			}
			meta, err := parsePulsarMetadata(m)

			if tc.err {
				require.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedType, meta.SubscriptionType)
			assert.Equal(t, tc.expectedInitialPosition, meta.SubscriptionInitialPosition)
			assert.Equal(t, tc.expectedMode, meta.SubscriptionMode)
		})
	}
}

// Simple valid Avro schemas for metadata parsing tests.
const (
	testAvroSchema1 = `{"type":"record","name":"S1","fields":[{"name":"id","type":"int"}]}`
	testAvroSchema2 = `{"type":"record","name":"S2","fields":[{"name":"id","type":"int"}]}`
	testAvroSchema3 = `{"type":"record","name":"S3","fields":[{"name":"id","type":"int"}]}`
)

func TestParsePulsarSchemaMetadata(t *testing.T) {
	t.Run("test json", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":                         "a",
			"obiwan.jsonschema":            "1",
			"kenobi.jsonschema.jsonschema": "2",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi.jsonschema"].value)
	})

	t.Run("test avro", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":                         "a",
			"obiwan.avroschema":            testAvroSchema1,
			"kenobi.avroschema.avroschema": testAvroSchema2,
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.JSONEq(t, testAvroSchema1, meta.internalTopicSchemas["obiwan"].value)
		assert.NotNil(t, meta.internalTopicSchemas["obiwan"].codec)
		assert.JSONEq(t, testAvroSchema2, meta.internalTopicSchemas["kenobi.avroschema"].value)
		assert.NotNil(t, meta.internalTopicSchemas["kenobi.avroschema"].codec)
	})

	t.Run("test proto", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":                           "a",
			"obiwan.avroschema":              testAvroSchema1,
			"kenobi.protoschema.protoschema": "2",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.JSONEq(t, testAvroSchema1, meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi.protoschema"].value)
	})

	t.Run("test combined avro/json", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":              "a",
			"obiwan.avroschema": testAvroSchema1,
			"kenobi.jsonschema": "2",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.JSONEq(t, testAvroSchema1, meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi"].value)
		assert.Equal(t, avroProtocol, meta.internalTopicSchemas["obiwan"].protocol)
		assert.Equal(t, jsonProtocol, meta.internalTopicSchemas["kenobi"].protocol) //nolint:testifylint
	})

	t.Run("test combined avro/json/proto", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":              "a",
			"obiwan.avroschema": testAvroSchema1,
			"kenobi.jsonschema": "2",
			"darth.protoschema": "3",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 3)
		assert.JSONEq(t, testAvroSchema1, meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi"].value)
		assert.Equal(t, "3", meta.internalTopicSchemas["darth"].value)
		assert.Equal(t, avroProtocol, meta.internalTopicSchemas["obiwan"].protocol)
		assert.Equal(t, jsonProtocol, meta.internalTopicSchemas["kenobi"].protocol) //nolint:testifylint
		assert.Equal(t, protoProtocol, meta.internalTopicSchemas["darth"].protocol)
	})

	t.Run("test funky edge case", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":                         "a",
			"obiwan.jsonschema.avroschema": testAvroSchema1,
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 1)
		assert.JSONEq(t, testAvroSchema1, meta.internalTopicSchemas["obiwan.jsonschema"].value)
		assert.NotNil(t, meta.internalTopicSchemas["obiwan.jsonschema"].codec)
	})
}

func TestGetPulsarSchema(t *testing.T) {
	t.Run("json schema", func(t *testing.T) {
		s := getPulsarSchema(schemaMetadata{
			protocol: "json",
			value: "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
				"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}",
		})
		assert.IsType(t, &pulsar.JSONSchema{}, s)
	})

	t.Run("avro schema", func(t *testing.T) {
		s := getPulsarSchema(schemaMetadata{
			protocol: "avro",
			value: "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
				"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}",
		})
		assert.IsType(t, &pulsar.AvroSchema{}, s)
	})
}

func TestParsePublishMetadata(t *testing.T) {
	m := &pubsub.PublishRequest{}
	m.Metadata = map[string]string{
		"deliverAt":    "2021-08-31T11:45:02Z",
		"deliverAfter": "60s",
	}
	msg, err := parsePublishMetadata(m, schemaMetadata{})
	require.NoError(t, err)

	val, _ := time.ParseDuration("60s")
	assert.Equal(t, val, msg.DeliverAfter)
	assert.Equal(t, "2021-08-31T11:45:02Z",
		msg.DeliverAt.Format(time.RFC3339))
}

func TestParsePublishMetadataAvroSchemaValidation(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Student",
		"namespace": "test",
		"fields": [
			{"name": "studentId", "type": "int"},
			{"name": "studentName", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid message", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"studentId": 1, "studentName": "John", "age": 25}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
		assert.NotNil(t, msg.Value)
	})

	t.Run("invalid type for age field", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"studentId": 1, "studentName": "John", "age": "not_a_number"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
		assert.Contains(t, err.Error(), "age")
	})

	t.Run("missing required field", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"studentId": 1, "studentName": "John"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("wrong type for studentName field", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"studentId": 1, "studentName": 123, "age": 25}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
		assert.Contains(t, err.Error(), "studentName")
	})

	t.Run("invalid JSON payload", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`not valid json`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("floating-point value for int field", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"studentId": 1, "studentName": "John", "age": 25.5}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaWithNullableFields(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Person",
		"namespace": "test",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "nickname", "type": ["null", "string"], "default": null}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("nullable field with null value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "John", "nickname": null}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("nullable field with string value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "John", "nickname": "Johnny"}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("nullable field omitted with default", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "John"}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("nullable field with wrong type", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "John", "nickname": 123}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaWithNestedRecord(t *testing.T) {
	avroSchemaJSON := `{
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

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid nested record", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"id": 1, "student": {"name": "John", "age": 25}}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("invalid nested record type", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"id": 1, "student": {"name": "John", "age": "twenty"}}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "age")
	})

	t.Run("nested record not an object", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"id": 1, "student": "not_an_object"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaWithArrays(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Classroom",
		"namespace": "test",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "scores", "type": {"type": "array", "items": "int"}}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid array", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "Math", "scores": [90, 85, 95]}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("invalid array element type", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "Math", "scores": [90, "eighty-five", 95]}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaWithMap(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Config",
		"namespace": "test",
		"fields": [
			{"name": "settings", "type": {"type": "map", "values": "string"}}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid map", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"settings": {"key1": "value1", "key2": "value2"}}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("invalid map value type", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"settings": {"key1": 123}}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("map field is not an object", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"settings": "not_a_map"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaWithEnum(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Shirt",
		"namespace": "test",
		"fields": [
			{"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid enum value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"color": "RED"}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("invalid enum value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"color": "YELLOW"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("enum field wrong type", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"color": 42}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaWithBoolean(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Feature",
		"namespace": "test",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "enabled", "type": "boolean"}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid boolean true", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "dark_mode", "enabled": true}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("valid boolean false", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "dark_mode", "enabled": false}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("boolean field wrong type", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "dark_mode", "enabled": "yes"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaWithFixed(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Hash",
		"namespace": "test",
		"fields": [
			{"name": "md5", "type": {"type": "fixed", "name": "MD5", "size": 16}}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid fixed raw bytes length", func(t *testing.T) {
		// goavro expects fixed values as strings with exact byte length matching schema size.
		req := &pubsub.PublishRequest{
			Data: []byte(`{"md5": "abcdefghijklmnop"}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("invalid fixed wrong size", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"md5": "tooshort"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("fixed field wrong type", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"md5": 12345}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaIntOverflow(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Numbers",
		"namespace": "test",
		"fields": [
			{"name": "small", "type": "int"},
			{"name": "big", "type": "long"}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("int within range", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"small": 2147483647, "big": 100}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("int overflow positive", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"small": 2147483648, "big": 100}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("int overflow negative", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"small": -2147483649, "big": 100}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("long accepts large value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"small": 1, "big": 2147483648}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})
}

func TestParsePublishMetadataAvroSchemaUnknownFields(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Person",
		"namespace": "test",
		"fields": [
			{"name": "name", "type": "string"}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("extra field rejected", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"name": "John", "extra": "field"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaFloatDoubleBytes(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Measurement",
		"namespace": "test",
		"fields": [
			{"name": "temperature", "type": "float"},
			{"name": "precise", "type": "double"},
			{"name": "payload", "type": "bytes"}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("valid float value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"temperature": 36.6, "precise": 3.14159, "payload": "aGVsbG8="}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("float field wrong type string", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"temperature": "hot", "precise": 3.14159, "payload": "aGVsbG8="}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("double field wrong type string", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"temperature": 36.6, "precise": "not_a_number", "payload": "aGVsbG8="}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("bytes field wrong type number", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"temperature": 36.6, "precise": 3.14159, "payload": 12345}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaFloatOverflow(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Measurement",
		"namespace": "test",
		"fields": [
			{"name": "temperature", "type": "float"},
			{"name": "precise", "type": "double"}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("float field rejects overflow value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"temperature": 1e300, "precise": 1.0}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})

	t.Run("double field accepts large value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"temperature": 36.6, "precise": 1e300}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})
}

func TestParsePublishMetadataAvroSchemaLongRejectsFloat(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Counter",
		"namespace": "test",
		"fields": [
			{"name": "count", "type": "long"}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("long field rejects float value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"count": 1.5}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaFixedLengthValidation(t *testing.T) {
	// goavro validates fixed values by raw string byte length matching schema size.
	avroSchemaJSON := `{
		"type": "record",
		"name": "Token",
		"namespace": "test",
		"fields": [
			{"name": "id", "type": {"type": "fixed", "name": "FixedID", "size": 3}}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("fixed value with correct raw byte length", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"id": "abc"}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("fixed value with wrong raw byte length", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"id": "AQID"}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaMultiTypeUnion(t *testing.T) {
	avroSchemaJSON := `{
		"type": "record",
		"name": "Flexible",
		"namespace": "test",
		"fields": [
			{"name": "value", "type": ["null", "string", "int"]}
		]
	}`

	sm := newAvroSchemaMetadata(t, avroSchemaJSON)

	t.Run("union with null value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"value": null}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("union with string value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"value": "hello"}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("union with int value", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"value": 42}`),
		}
		msg, err := parsePublishMetadata(req, sm)
		require.NoError(t, err)
		assert.NotNil(t, msg)
	})

	t.Run("union rejects boolean not in union types", func(t *testing.T) {
		req := &pubsub.PublishRequest{
			Data: []byte(`{"value": true}`),
		}
		_, err := parsePublishMetadata(req, sm)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avro schema validation failed")
	})
}

func TestParsePublishMetadataAvroSchemaInvalidSchemaDefinition(t *testing.T) {
	// Invalid Avro schemas are now rejected at init time (parsePulsarMetadata),
	// not at publish time, since the codec is compiled once and cached.
	m := pubsub.Metadata{}
	m.Properties = map[string]string{
		"host":                                "a",
		"mytopic" + topicAvroSchemaIdentifier: `{this is not valid json or avro schema`,
	}
	_, err := parsePulsarMetadata(m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse avro schema")
}

func TestMissingHost(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": ""}
	meta, err := parsePulsarMetadata(m)

	require.Error(t, err)
	assert.Nil(t, meta)
	assert.Equal(t, "pulsar error: missing pulsar host", err.Error())
}

func TestInvalidTLSInputDefaultsToFalse(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "enableTLS": "honk"}
	meta, err := parsePulsarMetadata(m)

	require.NoError(t, err)
	assert.NotNil(t, meta)
	assert.False(t, meta.EnableTLS)
}

func TestValidTenantAndNS(t *testing.T) {
	var (
		testTenant                = "testTenant"
		testNamespace             = "testNamespace"
		testTopic                 = "testTopic"
		expectPersistentResult    = "persistent://testTenant/testNamespace/testTopic"
		expectNonPersistentResult = "non-persistent://testTenant/testNamespace/testTopic"
	)
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", tenant: testTenant, namespace: testNamespace}

	t.Run("test vaild tenant and namespace", func(t *testing.T) {
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, testTenant, meta.Tenant)
		assert.Equal(t, testNamespace, meta.Namespace)
	})

	t.Run("test persistent format topic", func(t *testing.T) {
		meta, err := parsePulsarMetadata(m)
		p := Pulsar{metadata: *meta}
		res := p.formatTopic(testTopic)

		require.NoError(t, err)
		assert.True(t, meta.Persistent)
		assert.Equal(t, expectPersistentResult, res)
	})

	t.Run("test non-persistent format topic", func(t *testing.T) {
		m.Properties[persistent] = "false"
		meta, err := parsePulsarMetadata(m)
		p := Pulsar{metadata: *meta}
		res := p.formatTopic(testTopic)

		require.NoError(t, err)
		assert.False(t, meta.Persistent)
		assert.Equal(t, expectNonPersistentResult, res)
	})
}

func TestEncryptionKeys(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "privateKey": "111", "publicKey": "222", "keys": "a,b"}

	t.Run("test encryption metadata", func(t *testing.T) {
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "111", meta.PrivateKey)
		assert.Equal(t, "222", meta.PublicKey)
		assert.Equal(t, "a,b", meta.Keys)
	})

	t.Run("test valid producer encryption", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{"host": "a", "publicKey": "222", "keys": "a,b"}

		meta, _ := parsePulsarMetadata(m)
		p := &Pulsar{metadata: *meta}
		r := p.useProducerEncryption()

		assert.True(t, r)
	})

	t.Run("test invalid producer encryption missing public key", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{"host": "a", "keys": "a,b"}

		meta, _ := parsePulsarMetadata(m)
		p := &Pulsar{metadata: *meta}
		r := p.useProducerEncryption()

		assert.False(t, r)
	})

	t.Run("test invalid producer encryption missing keys", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{"host": "a", "publicKey": "222"}

		meta, _ := parsePulsarMetadata(m)
		p := &Pulsar{metadata: *meta}
		r := p.useProducerEncryption()

		assert.False(t, r)
	})

	t.Run("test valid consumer encryption", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{"host": "a", "privateKey": "222", "publicKey": "333"}

		meta, _ := parsePulsarMetadata(m)
		p := &Pulsar{metadata: *meta}
		r := p.useConsumerEncryption()

		assert.True(t, r)
	})

	t.Run("test invalid consumer encryption missing public key", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{"host": "a", "privateKey": "222"}

		meta, _ := parsePulsarMetadata(m)
		p := &Pulsar{metadata: *meta}
		r := p.useConsumerEncryption()

		assert.False(t, r)
	})

	t.Run("test invalid producer encryption missing private key", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{"host": "a", "privateKey": "222"}

		meta, _ := parsePulsarMetadata(m)
		p := &Pulsar{metadata: *meta}
		r := p.useConsumerEncryption()

		assert.False(t, r)
	})
}

func TestParsePulsarMetadataReplicateSubscriptionState(t *testing.T) {
	tt := []struct {
		name                       string
		replicateSubscriptionState string
		expected                   bool
	}{
		{
			name:                       "test replicateSubscriptionState true",
			replicateSubscriptionState: "true",
			expected:                   true,
		},
		{
			name:                       "test replicateSubscriptionState false",
			replicateSubscriptionState: "false",
			expected:                   false,
		},
		{
			name:                       "test replicateSubscriptionState empty (defaults to false)",
			replicateSubscriptionState: "",
			expected:                   false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}
			m.Properties = map[string]string{
				"host": "a",
			}

			if tc.replicateSubscriptionState != "" {
				m.Properties["replicateSubscriptionState"] = tc.replicateSubscriptionState
			}

			meta, err := parsePulsarMetadata(m)

			require.NoError(t, err)
			assert.Equal(t, tc.expected, meta.ReplicateSubscriptionState)
		})
	}
}

func TestSanitiseURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"With pulsar+ssl prefix", "pulsar+ssl://localhost:6650", "pulsar+ssl://localhost:6650"},
		{"With pulsar prefix", "pulsar://localhost:6650", "pulsar://localhost:6650"},
		{"With http prefix", "http://localhost:6650", "http://localhost:6650"},
		{"With https prefix", "https://localhost:6650", "https://localhost:6650"},
		{"Without prefix", "localhost:6650", "pulsar://localhost:6650"},
		{"Empty string", "", "pulsar://"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := sanitiseURL(test.input)
			if actual != test.expected {
				t.Errorf("sanitiseURL(%q) = %q; want %q", test.input, actual, test.expected)
			}
		})
	}
}

func TestInitUsesTokenSupplierWhenClientSecretPathProvided(t *testing.T) {
	server := newOAuthTestServer(t)
	secretPath := writeTempFile(t, "rotating-secret")

	var capturedOpts pulsar.ClientOptions
	p := NewPulsar(logger.NewLogger("test")).(*Pulsar)
	t.Cleanup(func() {
		p.newClientFn = pulsar.NewClient
	})
	p.newClientFn = func(opts pulsar.ClientOptions) (pulsar.Client, error) {
		capturedOpts = opts
		return nil, nil
	}

	md := pubsub.Metadata{}
	md.Properties = map[string]string{
		"host":                   "localhost:6650",
		"oauth2TokenURL":         server.URL,
		"oauth2ClientID":         "client-id",
		"oauth2ClientSecretPath": secretPath,
		"oauth2Scopes":           "scope1",
		"oauth2Audiences":        "aud1",
	}
	err := p.Init(t.Context(), md)

	require.NoError(t, err)
	require.NotNil(t, capturedOpts.Authentication)
	// Should use TokenSupplier, not TokenFromFile
	expected := pulsar.NewAuthenticationTokenFromSupplier(func() (string, error) {
		return "", nil
	})
	assert.IsType(t, expected, capturedOpts.Authentication)
}

func TestInitUsesTokenSupplierWithPlainTextSecretFile(t *testing.T) {
	server := newOAuthTestServer(t)
	secretPath := writeTempFile(t, "plain-text-secret-12345")

	var capturedOpts pulsar.ClientOptions
	p := NewPulsar(logger.NewLogger("test")).(*Pulsar)
	t.Cleanup(func() {
		p.newClientFn = pulsar.NewClient
	})
	p.newClientFn = func(opts pulsar.ClientOptions) (pulsar.Client, error) {
		capturedOpts = opts
		return nil, nil
	}

	md := pubsub.Metadata{}
	md.Properties = map[string]string{
		"host":                   "localhost:6650",
		"oauth2TokenURL":         server.URL,
		"oauth2ClientID":         "client-id",
		"oauth2ClientSecretPath": secretPath,
		"oauth2Scopes":           "scope1",
		"oauth2Audiences":        "aud1",
	}
	err := p.Init(t.Context(), md)

	require.NoError(t, err)
	require.NotNil(t, capturedOpts.Authentication)
	expected := pulsar.NewAuthenticationTokenFromSupplier(func() (string, error) {
		return "", nil
	})
	assert.IsType(t, expected, capturedOpts.Authentication)
}

func TestInitUsesTokenSupplierWithJSONSecretFile(t *testing.T) {
	server := newOAuthTestServer(t)
	credentialsPath := writeTempFile(t, fmt.Sprintf(`{
		"client_id": "json-id-from-file",
		"client_secret": "json-secret-from-file",
		"issuer_url": "%s"
	}`, server.URL))

	var capturedOpts pulsar.ClientOptions
	p := NewPulsar(logger.NewLogger("test")).(*Pulsar)
	t.Cleanup(func() {
		p.newClientFn = pulsar.NewClient
	})
	p.newClientFn = func(opts pulsar.ClientOptions) (pulsar.Client, error) {
		capturedOpts = opts
		return nil, nil
	}

	md := pubsub.Metadata{}
	md.Properties = map[string]string{
		"host":                  "localhost:6650",
		"oauth2CredentialsFile": credentialsPath,
		"oauth2Scopes":          "scope1",
		"oauth2Audiences":       "aud1",
	}
	err := p.Init(t.Context(), md)

	require.NoError(t, err)
	require.NotNil(t, capturedOpts.Authentication)
	expected := pulsar.NewAuthenticationTokenFromSupplier(func() (string, error) {
		return "", nil
	})
	assert.IsType(t, expected, capturedOpts.Authentication)
}

func TestInitUsesClientIDFromMetadataWhenFileHasOnlySecret(t *testing.T) {
	server := newOAuthTestServer(t)
	// Test that oauth2ClientSecretPath works with plain text (client_id comes from metadata)
	//nolint:gosec
	plainTextSecret := "plain-text-secret-12345"
	secretPath := writeTempFile(t, plainTextSecret)

	var capturedOpts pulsar.ClientOptions
	p := NewPulsar(logger.NewLogger("test")).(*Pulsar)
	t.Cleanup(func() {
		p.newClientFn = pulsar.NewClient
	})
	p.newClientFn = func(opts pulsar.ClientOptions) (pulsar.Client, error) {
		capturedOpts = opts
		return nil, nil
	}

	md := pubsub.Metadata{}
	md.Properties = map[string]string{
		"host":                   "localhost:6650",
		"oauth2TokenURL":         server.URL,
		"oauth2ClientID":         "metadata-client-id", // client_id from metadata
		"oauth2ClientSecretPath": secretPath,           // plain text secret in file
		"oauth2Scopes":           "scope1",
		"oauth2Audiences":        "aud1",
	}
	err := p.Init(t.Context(), md)

	require.NoError(t, err)
	require.NotNil(t, capturedOpts.Authentication)
	expected := pulsar.NewAuthenticationTokenFromSupplier(func() (string, error) {
		return "", nil
	})
	assert.IsType(t, expected, capturedOpts.Authentication)
}

func TestInitFailsWhenClientCredentialsTypeMissingClientSecret(t *testing.T) {
	// Test that credentials file requires client_secret
	//nolint:gosec
	credentialsJSON := `{
		"client_id": "test-id",
		"issuer_url": "https://oauth.example.com/token"
	}`
	secretPath := writeTempFile(t, credentialsJSON)

	md := pubsub.Metadata{}
	md.Properties = map[string]string{
		"host":                  "localhost:6650",
		"oauth2CredentialsFile": secretPath,
		"oauth2Scopes":          "scope1",
		"oauth2Audiences":       "aud1",
	}
	p := NewPulsar(logger.NewLogger("test"))
	err := p.Init(t.Context(), md)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "must contain client_id and client_secret")
}

func TestInitUsesTokenSupplierWhenClientSecretPathMissing(t *testing.T) {
	server := newOAuthTestServer(t)

	var capturedOpts pulsar.ClientOptions
	p := NewPulsar(logger.NewLogger("test")).(*Pulsar)
	t.Cleanup(func() {
		p.newClientFn = pulsar.NewClient
	})
	p.newClientFn = func(opts pulsar.ClientOptions) (pulsar.Client, error) {
		capturedOpts = opts
		return nil, nil
	}

	md := pubsub.Metadata{}
	md.Properties = map[string]string{
		"host":               "localhost:6650",
		"oauth2TokenURL":     server.URL,
		"oauth2ClientID":     "client-id",
		"oauth2ClientSecret": "client-secret",
		"oauth2Scopes":       "scope1",
		"oauth2Audiences":    "aud1",
	}
	err := p.Init(t.Context(), md)

	require.NoError(t, err)
	require.NotNil(t, capturedOpts.Authentication)
	expected := pulsar.NewAuthenticationTokenFromSupplier(func() (string, error) {
		return "", nil
	})
	assert.IsType(t, expected, capturedOpts.Authentication)
}

func TestInitUsesTokenWhenProvided(t *testing.T) {
	var capturedOpts pulsar.ClientOptions
	p := NewPulsar(logger.NewLogger("test")).(*Pulsar)
	t.Cleanup(func() {
		p.newClientFn = pulsar.NewClient
	})
	p.newClientFn = func(opts pulsar.ClientOptions) (pulsar.Client, error) {
		capturedOpts = opts
		return nil, nil
	}

	md := pubsub.Metadata{}
	md.Properties = map[string]string{
		"host":  "localhost:6650",
		"token": "my-token",
	}
	err := p.Init(t.Context(), md)

	require.NoError(t, err)
	require.NotNil(t, capturedOpts.Authentication)
	expected := pulsar.NewAuthenticationToken("my-token")
	assert.IsType(t, expected, capturedOpts.Authentication)
}

func newOAuthTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"token","token_type":"bearer","expires_in":3600}`))
	}))
	t.Cleanup(server.Close)

	return server
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "pulsar-secret-*")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestSubscribe_AppliesMetadataOptions(t *testing.T) {
	p := NewPulsar(logger.NewLogger("test")).(*Pulsar)

	md := pubsub.Metadata{
		Base: metadata.Base{Properties: map[string]string{
			"host":                     "localhost:6650",
			"consumerID":               "my-test-consumer",
			"topic":                    "my-topic",
			"subscribeInitialPosition": "earliest",
			"subscribeMode":            "non_durable",
		}},
	}

	var capturedOptions pulsar.ConsumerOptions
	mockClient := &MockPulsarClient{
		SubscribeFn: func(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
			capturedOptions = options
			return &MockPulsarConsumer{
				Ch: make(chan pulsar.ConsumerMessage),
			}, nil
		},
	}
	p.client = mockClient

	parsedMeta, err := parsePulsarMetadata(md)
	require.NoError(t, err)
	p.metadata = *parsedMeta

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := pubsub.SubscribeRequest{
		Topic:    "my-topic",
		Metadata: md.Properties,
	}

	err = p.Subscribe(ctx, req, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return nil
	})
	require.NoError(t, err)

	// Verify Initial Position: Should be Earliest (1), NOT Latest (0)
	assert.Equal(t, pulsar.SubscriptionPositionEarliest, capturedOptions.SubscriptionInitialPosition,
		"Bug: SubscriptionInitialPosition defaulted to 'Latest' instead of 'Earliest'")

	// Verify Subscription Mode: Should be NonDurable (1), NOT Durable (0)
	assert.Equal(t, pulsar.NonDurable, capturedOptions.SubscriptionMode,
		"Bug: SubscriptionMode defaulted to 'Durable' instead of 'NonDurable'")
}

type MockPulsarClient struct {
	pulsar.Client
	SubscribeFn func(pulsar.ConsumerOptions) (pulsar.Consumer, error)
}

func (m *MockPulsarClient) Subscribe(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	if m.SubscribeFn != nil {
		return m.SubscribeFn(options)
	}
	return nil, nil
}

func (m *MockPulsarClient) Close() {}

type MockPulsarConsumer struct {
	pulsar.Consumer
	Ch chan pulsar.ConsumerMessage
}

func (m *MockPulsarConsumer) Chan() <-chan pulsar.ConsumerMessage {
	return m.Ch
}

func (m *MockPulsarConsumer) Close() {}
