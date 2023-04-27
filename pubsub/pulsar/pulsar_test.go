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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
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
	}
	meta, err := parsePulsarMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "a", meta.Host)
	assert.Equal(t, false, meta.EnableTLS)
	assert.Equal(t, true, meta.DisableBatching)
	assert.Equal(t, defaultTenant, meta.Tenant)
	assert.Equal(t, defaultNamespace, meta.Namespace)
	assert.Equal(t, 5*time.Second, meta.BatchingMaxPublishDelay)
	assert.Equal(t, uint(100), meta.BatchingMaxSize)
	assert.Equal(t, uint(200), meta.BatchingMaxMessages)
	assert.Empty(t, meta.internalTopicSchemas)
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

		assert.Nil(t, err)
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

		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi.avroschema"].value)
	})

	t.Run("test combined avro/json", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":              "a",
			"obiwan.avroschema": "1",
			"kenobi.jsonschema": "2",
		}
		meta, err := parsePulsarMetadata(m)

		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Host)
		assert.Len(t, meta.internalTopicSchemas, 2)
		assert.Equal(t, "1", meta.internalTopicSchemas["obiwan"].value)
		assert.Equal(t, "2", meta.internalTopicSchemas["kenobi"].value)
		assert.Equal(t, avroProtocol, meta.internalTopicSchemas["obiwan"].protocol)
		assert.Equal(t, jsonProtocol, meta.internalTopicSchemas["kenobi"].protocol)
	})

	t.Run("test funky edge case", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"host":                         "a",
			"obiwan.jsonschema.avroschema": "1",
		}
		meta, err := parsePulsarMetadata(m)

		assert.Nil(t, err)
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
	assert.Nil(t, err)

	val, _ := time.ParseDuration("60s")
	assert.Equal(t, val, msg.DeliverAfter)
	assert.Equal(t, "2021-08-31T11:45:02Z",
		msg.DeliverAt.Format(time.RFC3339))
}

func TestMissingHost(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": ""}
	meta, err := parsePulsarMetadata(m)

	assert.Error(t, err)
	assert.Nil(t, meta)
	assert.Equal(t, "pulsar error: missing pulsar host", err.Error())
}

func TestInvalidTLSInputDefaultsToFalse(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "enableTLS": "honk"}
	meta, err := parsePulsarMetadata(m)

	assert.NoError(t, err)
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

		assert.Nil(t, err)
		assert.Equal(t, testTenant, meta.Tenant)
		assert.Equal(t, testNamespace, meta.Namespace)
	})

	t.Run("test persistent format topic", func(t *testing.T) {
		meta, err := parsePulsarMetadata(m)
		p := Pulsar{metadata: *meta}
		res := p.formatTopic(testTopic)

		assert.Nil(t, err)
		assert.Equal(t, true, meta.Persistent)
		assert.Equal(t, expectPersistentResult, res)
	})

	t.Run("test non-persistent format topic", func(t *testing.T) {
		m.Properties[persistent] = "false"
		meta, err := parsePulsarMetadata(m)
		p := Pulsar{metadata: *meta}
		res := p.formatTopic(testTopic)

		assert.Nil(t, err)
		assert.Equal(t, false, meta.Persistent)
		assert.Equal(t, expectNonPersistentResult, res)
	})
}

func TestEncryptionKeys(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"host": "a", "privateKey": "111", "publicKey": "222", "keys": "a,b"}

	t.Run("test encryption metadata", func(t *testing.T) {
		meta, err := parsePulsarMetadata(m)

		assert.Nil(t, err)
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
