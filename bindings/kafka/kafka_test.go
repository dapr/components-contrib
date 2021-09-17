// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMetadata(t *testing.T) {
	logger := logger.NewLogger("test")

	t.Run("correct metadata (authRequired false)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "false"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.False(t, meta.AuthRequired)
	})

	t.Run("correct metadata (authRequired FALSE)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "FALSE"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.False(t, meta.AuthRequired)
	})

	t.Run("correct metadata (authRequired False)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "False"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.False(t, meta.AuthRequired)
	})

	t.Run("correct metadata (authRequired F)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "F"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.False(t, meta.AuthRequired)
	})

	t.Run("correct metadata (authRequired f)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "f"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.False(t, meta.AuthRequired)
	})

	t.Run("correct metadata (authRequired 0)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "0"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.False(t, meta.AuthRequired)
	})

	t.Run("correct metadata (authRequired F)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "F"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.False(t, meta.AuthRequired)
	})

	t.Run("correct metadata (authRequired true)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "true", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
	})

	t.Run("correct metadata (authRequired TRUE)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "TRUE", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
	})

	t.Run("correct metadata (authRequired True)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "True", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
	})

	t.Run("correct metadata (authRequired T)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "T", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
	})

	t.Run("correct metadata (authRequired t)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "t", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
	})

	t.Run("correct metadata (authRequired 1)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "1", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
	})

	t.Run("correct metadata (maxMessageBytes 2048)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "1", "saslUsername": "foo", "saslPassword": "bar", "maxMessageBytes": "2048"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
		assert.Equal(t, 2048, meta.MaxMessageBytes)
	})

	t.Run("correct metadata (no maxMessageBytes)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "1", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.True(t, meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
		assert.Equal(t, 0, meta.MaxMessageBytes)
	})

	t.Run("missing authRequired", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: missing 'authRequired' attribute"), err)
		assert.Nil(t, meta)
	})

	t.Run("empty authRequired", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"authRequired": "", "consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: 'authRequired' attribute was empty"), err)
		assert.Nil(t, meta)
	})

	t.Run("invalid authRequired", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"authRequired": "not_sure", "consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: invalid value for 'authRequired' attribute. use true or false"), err)
		assert.Nil(t, meta)
	})

	t.Run("SASL username required if authRequired is true", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"authRequired": "true", "saslPassword": "t0ps3cr3t", "consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: missing SASL Username"), err)
		assert.Nil(t, meta)
	})
	t.Run("SASL password required if authRequired is true", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"authRequired": "true", "saslUsername": "foobar", "consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: missing SASL Password"), err)
		assert.Nil(t, meta)
	})

	t.Run("correct metadata (initialOffset)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "false", "initialOffset": "oldest"}
		k := Kafka{logger: logger}
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, sarama.OffsetOldest, meta.InitialOffset)
		m.Properties["initialOffset"] = "newest"
		meta, err = k.getKafkaMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, sarama.OffsetNewest, meta.InitialOffset)
	})
}
