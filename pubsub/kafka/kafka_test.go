// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func getKafkaPubsub() *Kafka {
	return &Kafka{logger: logger.NewLogger("kafka_test")}
}

func TestParseMetadata(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"consumerID": "a", "brokers": "a", "authRequired": "false"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", meta.Brokers[0])
	assert.Equal(t, "a", meta.ConsumerID)
}

func TestMissingBrokers(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	assert.Error(t, err)
	assert.Nil(t, meta)

	assert.Equal(t, "kafka error: missing 'brokers' attribute", err.Error())
}

func TestMissingAuthRequired(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"brokers": "akfak.com:9092"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	assert.Error(t, err)
	assert.Nil(t, meta)

	assert.Equal(t, "kafka error: missing 'authRequired' attribute", err.Error())
}

func TestMissingSaslValues(t *testing.T) {
	m := pubsub.Metadata{}
	k := getKafkaPubsub()
	m.Properties = map[string]string{"brokers": "akfak.com:9092", "authRequired": "true"}
	meta, err := k.getKafkaMetadata(m)
	assert.Error(t, err)
	assert.Nil(t, meta)

	assert.Equal(t, "kafka error: missing SASL Username", err.Error())

	m.Properties["saslUsername"] = "sassafras"

	meta, err = k.getKafkaMetadata(m)
	assert.Error(t, err)
	assert.Nil(t, meta)

	assert.Equal(t, "kafka error: missing SASL Password", err.Error())
}

func TestPresentSaslValues(t *testing.T) {
	m := pubsub.Metadata{}
	k := getKafkaPubsub()
	m.Properties = map[string]string{
		"brokers":      "akfak.com:9092",
		"authRequired": "true",
		"saslUsername": "sassafras",
		"saslPassword": "sassapass",
	}
	meta, err := k.getKafkaMetadata(m)
	assert.NoError(t, err)
	assert.NotNil(t, meta)

	assert.Equal(t, "sassafras", meta.SaslUsername)
	assert.Equal(t, "sassapass", meta.SaslPassword)
}

func TestInvalidAuthRequiredFlag(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"brokers": "akfak.com:9092", "authRequired": "maybe?????????????"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	assert.Error(t, err)
	assert.Nil(t, meta)

	assert.Equal(t, "kafka error: invalid value for 'authRequired' attribute", err.Error())
}
