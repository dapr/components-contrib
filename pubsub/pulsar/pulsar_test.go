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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

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
			"obiwan.avroschema":            "1",
			"kenobi.avroschema.avroschema": "2",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi.avroschema"].value)
	})

	t.Run("test proto", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":                           "a",
			"obiwan.avroschema":              "1",
			"kenobi.protoschema.protoschema": "2",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi.protoschema"].value)
	})

	t.Run("test combined avro/json", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":              "a",
			"obiwan.avroschema": "1",
			"kenobi.jsonschema": "2",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi"].value)
		assert.Equal(t, avroProtocol, meta.internalTopicSchemas["obiwan"].protocol)
		assert.Equal(t, jsonProtocol, meta.internalTopicSchemas["kenobi"].protocol) //nolint:testifylint
	})

	t.Run("test combined avro/json/proto", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":              "a",
			"obiwan.avroschema": "1",
			"kenobi.jsonschema": "2",
			"darth.protoschema": "3",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 3)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan"].value)
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
			"obiwan.jsonschema.avroschema": "1",
		}
		meta, err := parsePulsarMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 1)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan.jsonschema"].value)
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

func TestParsePulsarMetadataCompressionType(t *testing.T) {
	tt := []struct {
		name            string
		compressionType string
		expected        string
		expectedPulsar  pulsar.CompressionType
		err             bool
	}{
		{
			name:            "test valid compression type - none",
			compressionType: "none",
			expected:        "none",
			expectedPulsar:  pulsar.NoCompression,
			err:             false,
		},
		{
			name:            "test valid compression type - lz4",
			compressionType: "lz4",
			expected:        "lz4",
			expectedPulsar:  pulsar.LZ4,
			err:             false,
		},
		{
			name:            "test valid compression type - zlib",
			compressionType: "zlib",
			expected:        "zlib",
			expectedPulsar:  pulsar.ZLib,
			err:             false,
		},
		{
			name:            "test valid compression type - zstd",
			compressionType: "zstd",
			expected:        "zstd",
			expectedPulsar:  pulsar.ZSTD,
			err:             false,
		},
		{
			name:            "test valid compression type - empty defaults to none",
			compressionType: "",
			expected:        "none",
			expectedPulsar:  pulsar.NoCompression,
			err:             false,
		},
		{
			name:            "test valid compression type - case insensitive",
			compressionType: "LZ4",
			expected:        "lz4",
			expectedPulsar:  pulsar.LZ4,
			err:             false,
		},
		{
			name:            "test invalid compression type",
			compressionType: "invalid",
			err:             true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}
			m.Properties = map[string]string{
				"host":            "a",
				"compressionType": tc.compressionType,
			}
			meta, err := parsePulsarMetadata(m)

			if tc.err {
				require.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expected, meta.CompressionType)
			assert.Equal(t, tc.expectedPulsar, getCompressionType(meta.CompressionType))
		})
	}
}

func TestParsePulsarMetadataCompressionLevel(t *testing.T) {
	tt := []struct {
		name             string
		compressionLevel string
		expected         string
		expectedPulsar   pulsar.CompressionLevel
		err              bool
	}{
		{
			name:             "test valid compression level - default",
			compressionLevel: "default",
			expected:         "default",
			expectedPulsar:   pulsar.Default,
			err:              false,
		},
		{
			name:             "test valid compression level - faster",
			compressionLevel: "faster",
			expected:         "faster",
			expectedPulsar:   pulsar.Faster,
			err:              false,
		},
		{
			name:             "test valid compression level - better",
			compressionLevel: "better",
			expected:         "better",
			expectedPulsar:   pulsar.Better,
			err:              false,
		},
		{
			name:             "test valid compression level - empty defaults to default",
			compressionLevel: "",
			expected:         "default",
			expectedPulsar:   pulsar.Default,
			err:              false,
		},
		{
			name:             "test valid compression level - case insensitive",
			compressionLevel: "FASTER",
			expected:         "faster",
			expectedPulsar:   pulsar.Faster,
			err:              false,
		},
		{
			name:             "test invalid compression level",
			compressionLevel: "invalid",
			err:              true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}
			m.Properties = map[string]string{
				"host":             "a",
				"compressionLevel": tc.compressionLevel,
			}
			meta, err := parsePulsarMetadata(m)

			if tc.err {
				require.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expected, meta.CompressionLevel)
			assert.Equal(t, tc.expectedPulsar, getCompressionLevel(meta.CompressionLevel))
		})
	}
}

func TestParsePulsarMetadataCompressionCombination(t *testing.T) {
	tt := []struct {
		name             string
		compressionType  string
		compressionLevel string
		expectedType     string
		expectedLevel    string
		err              bool
	}{
		{
			name:             "test default compression settings",
			compressionType:  "",
			compressionLevel: "",
			expectedType:     "none",
			expectedLevel:    "default",
			err:              false,
		},
		{
			name:             "test lz4 with faster compression",
			compressionType:  "lz4",
			compressionLevel: "faster",
			expectedType:     "lz4",
			expectedLevel:    "faster",
			err:              false,
		},
		{
			name:             "test zstd with better compression",
			compressionType:  "zstd",
			compressionLevel: "better",
			expectedType:     "zstd",
			expectedLevel:    "better",
			err:              false,
		},
		{
			name:            "test invalid compression type",
			compressionType: "invalid",
			err:             true,
		},
		{
			name:             "test invalid compression level",
			compressionType:  "lz4",
			compressionLevel: "invalid",
			err:              true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := pubsub.Metadata{}
			m.Properties = map[string]string{
				"host":             "a",
				"compressionType":  tc.compressionType,
				"compressionLevel": tc.compressionLevel,
			}
			meta, err := parsePulsarMetadata(m)

			if tc.err {
				require.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedType, meta.CompressionType)
			assert.Equal(t, tc.expectedLevel, meta.CompressionLevel)
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
	expected := pulsar.NewAuthenticationTokenFromSupplier(func() (string, error) {
		return "", nil
	})
	assert.IsType(t, expected, capturedOpts.Authentication)
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
