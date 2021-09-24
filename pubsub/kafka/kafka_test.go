// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"testing"

	"github.com/Shopify/sarama"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func getKafkaPubsub() *Kafka {
	return &Kafka{logger: logger.NewLogger("kafka_test")}
}

func TestParseMetadata(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"consumerGroup": "a", "clientID": "a", "brokers": "a", "authRequired": "false", "maxMessageBytes": "2048"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", meta.Brokers[0])
	assert.Equal(t, "a", meta.ConsumerGroup)
	assert.Equal(t, "a", meta.ClientID)
	assert.Equal(t, 2048, meta.MaxMessageBytes)
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

func TestInitialOffset(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"consumerGroup": "a", "brokers": "a", "authRequired": "false", "initialOffset": "oldest"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, sarama.OffsetOldest, meta.InitialOffset)
	m.Properties["initialOffset"] = "newest"
	meta, err = k.getKafkaMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, sarama.OffsetNewest, meta.InitialOffset)
}
