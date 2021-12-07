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

package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

var (
	clientCertPemMock = `-----BEGIN CERTIFICATE-----
Y2xpZW50Q2VydA==
-----END CERTIFICATE-----`
	clientKeyMock = `-----BEGIN RSA PRIVATE KEY-----
Y2xpZW50S2V5
-----END RSA PRIVATE KEY-----`
	caCertMock = `-----BEGIN CERTIFICATE-----
Y2FDZXJ0
-----END CERTIFICATE-----`
)

func getKafkaPubsub() *Kafka {
	return &Kafka{logger: logger.NewLogger("kafka_test")}
}

func getBaseMetadata() pubsub.Metadata {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"consumerGroup": "a", "clientID": "a", "brokers": "a", "authRequired": "false", "maxMessageBytes": "2048"}
	return m
}

func getCompleteMetadata() pubsub.Metadata {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{
		"consumerGroup": "a", "clientID": "a", "brokers": "a", "authRequired": "false", "maxMessageBytes": "2048",
		skipVerify: "true", clientCert: clientCertPemMock, clientKey: clientKeyMock, caCert: caCertMock,
		"consumeRetryInterval": "200",
	}
	return m
}

func TestParseMetadata(t *testing.T) {
	k := getKafkaPubsub()
	t.Run("default kafka version", func(t *testing.T) {
		m := getCompleteMetadata()
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assertMetadata(t, meta)
		require.Equal(t, sarama.V2_0_0_0, meta.Version)
	})

	t.Run("specific kafka version", func(t *testing.T) {
		m := getCompleteMetadata()
		m.Properties["version"] = "0.10.2.0"
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assertMetadata(t, meta)
		require.Equal(t, sarama.V0_10_2_0, meta.Version)
	})

	t.Run("invalid kafka version", func(t *testing.T) {
		m := getCompleteMetadata()
		m.Properties["version"] = "not_valid_version"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)
		require.Equal(t, "kafka error: invalid kafka version", err.Error())
	})
}

func assertMetadata(t *testing.T, meta *kafkaMetadata) {
	require.Equal(t, "a", meta.Brokers[0])
	require.Equal(t, "a", meta.ConsumerGroup)
	require.Equal(t, "a", meta.ClientID)
	require.Equal(t, 2048, meta.MaxMessageBytes)
	require.Equal(t, true, meta.TLSSkipVerify)
	require.Equal(t, clientCertPemMock, meta.TLSClientCert)
	require.Equal(t, clientKeyMock, meta.TLSClientKey)
	require.Equal(t, caCertMock, meta.TLSCaCert)
	require.Equal(t, 200*time.Millisecond, meta.ConsumeRetryInterval)
}

func TestMissingBrokers(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, "kafka error: missing 'brokers' attribute", err.Error())
}

func TestMissingAuthRequired(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"brokers": "akfak.com:9092"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, "kafka error: missing 'authRequired' attribute", err.Error())
}

func TestMissingSaslValues(t *testing.T) {
	m := pubsub.Metadata{}
	k := getKafkaPubsub()
	m.Properties = map[string]string{"brokers": "akfak.com:9092", "authRequired": "true"}
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, "kafka error: missing SASL Username", err.Error())

	m.Properties["saslUsername"] = "sassafras"

	meta, err = k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, "kafka error: missing SASL Password", err.Error())
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
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, "sassafras", meta.SaslUsername)
	require.Equal(t, "sassapass", meta.SaslPassword)
}

func TestInvalidAuthRequiredFlag(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"brokers": "akfak.com:9092", "authRequired": "maybe?????????????"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, "kafka error: invalid value for 'authRequired' attribute", err.Error())
}

func TestInitialOffset(t *testing.T) {
	m := pubsub.Metadata{}
	m.Properties = map[string]string{"consumerGroup": "a", "brokers": "a", "authRequired": "false", "initialOffset": "oldest"}
	k := getKafkaPubsub()
	meta, err := k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.Equal(t, sarama.OffsetOldest, meta.InitialOffset)
	m.Properties["initialOffset"] = "newest"
	meta, err = k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.Equal(t, sarama.OffsetNewest, meta.InitialOffset)
}

func TestTls(t *testing.T) {
	k := getKafkaPubsub()

	t.Run("disable tls", func(t *testing.T) {
		m := getBaseMetadata()
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		c := &sarama.Config{}
		err = updateTLSConfig(c, meta)
		require.NoError(t, err)
		require.Equal(t, false, c.Net.TLS.Enable)
	})

	t.Run("wrong client cert format", func(t *testing.T) {
		m := getBaseMetadata()
		m.Properties[clientCert] = "clientCert"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: invalid client certificate", err.Error())
	})

	t.Run("wrong client key format", func(t *testing.T) {
		m := getBaseMetadata()
		m.Properties[clientKey] = "clientKey"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: invalid client key", err.Error())
	})

	t.Run("miss client key", func(t *testing.T) {
		m := getBaseMetadata()
		m.Properties[clientCert] = clientCertPemMock
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: clientKey or clientCert is missing", err.Error())
	})

	t.Run("miss client cert", func(t *testing.T) {
		m := getBaseMetadata()
		m.Properties[clientKey] = clientKeyMock
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: clientKey or clientCert is missing", err.Error())
	})

	t.Run("wrong ca cert format", func(t *testing.T) {
		m := getBaseMetadata()
		m.Properties[caCert] = "caCert"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: invalid ca certificate", err.Error())
	})
}
