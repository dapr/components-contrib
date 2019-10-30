// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"errors"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {

	t.Run("correct metadata (authRequired false)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "false"}
		k := Kafka{}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
	})

	t.Run("correct metadata (authRequired true)", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a", "authRequired": "true", "saslUsername": "foo", "saslPassword": "bar"}
		k := Kafka{}
		meta, err := k.getKafkaMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "a", meta.Brokers[0])
		assert.Equal(t, "a", meta.ConsumerGroup)
		assert.Equal(t, "a", meta.PublishTopic)
		assert.Equal(t, "a", meta.Topics[0])
		assert.Equal(t, "true", meta.AuthRequired)
		assert.Equal(t, "foo", meta.SaslUsername)
		assert.Equal(t, "bar", meta.SaslPassword)
	})

	t.Run("missing authRequired", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: invalid value for 'authRequired' attribute. use true or false"), err)
		assert.Nil(t, meta)
	})

	t.Run("invalid authRequired", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"authRequired": "not_sure", "consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: invalid value for 'authRequired' attribute. use true or false"), err)
		assert.Nil(t, meta)
	})

	t.Run("SASL username required if authRequired is true", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"authRequired": "true", "saslPassword": "t0ps3cr3t", "consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: missing SASL Username"), err)
		assert.Nil(t, meta)
	})
	t.Run("SASL password required if authRequired is true", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"authRequired": "true", "saslUsername": "foobar", "consumerGroup": "a", "publishTopic": "a", "brokers": "a", "topics": "a"}
		k := Kafka{}
		meta, err := k.getKafkaMetadata(m)
		assert.Error(t, errors.New("kafka error: missing SASL Password"), err)
		assert.Nil(t, meta)
	})
}
