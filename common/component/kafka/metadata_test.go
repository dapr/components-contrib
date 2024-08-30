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
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

//nolint:gosec
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

func getKafka() *Kafka {
	return &Kafka{logger: logger.NewLogger("kafka_test")}
}

func getBaseMetadata() map[string]string {
	return map[string]string{"consumerGroup": "a", "clientID": "a", "brokers": "a", "disableTls": "true", "authType": mtlsAuthType, "maxMessageBytes": "2048"}
}

func getCompleteMetadata() map[string]string {
	return map[string]string{
		"consumerGroup":        "a",
		"clientID":             "a",
		"brokers":              "a",
		"authType":             mtlsAuthType,
		"maxMessageBytes":      "2048",
		skipVerify:             "true",
		clientCert:             clientCertPemMock,
		clientKey:              clientKeyMock,
		caCert:                 caCertMock,
		"consumeRetryInterval": "200",
		consumerFetchDefault:   "1048576",
		consumerFetchMin:       "1",
		channelBufferSize:      "256",
		"heartbeatInterval":    "2s",
		"sessionTimeout":       "30s",
	}
}

func TestParseMetadata(t *testing.T) {
	k := getKafka()
	t.Run("default kafka version", func(t *testing.T) {
		m := getCompleteMetadata()
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assertMetadata(t, meta)
		require.Equal(t, sarama.V2_0_0_0, meta.internalVersion) //nolint:nosnakecase
	})

	t.Run("specific kafka version", func(t *testing.T) {
		m := getCompleteMetadata()
		m["version"] = "0.10.2.0"
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assertMetadata(t, meta)
		require.Equal(t, sarama.V0_10_2_0, meta.internalVersion) //nolint:nosnakecase
	})

	t.Run("invalid kafka version", func(t *testing.T) {
		m := getCompleteMetadata()
		m["version"] = "not_valid_version"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)
		require.Equal(t, "kafka error: invalid kafka version", err.Error())
	})
}

func TestConsumerIDFallback(t *testing.T) {
	k := getKafka()

	m := getCompleteMetadata()
	m["consumerGroup"] = "foo"
	m["consumerid"] = "bar" // Lowercase to test case-insensitivity

	t.Run("with consumerGroup set", func(t *testing.T) {
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.Equal(t, "foo", meta.ConsumerGroup)
	})

	t.Run("with consumerGroup not set", func(t *testing.T) {
		delete(m, "consumerGroup")

		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.Equal(t, "bar", meta.ConsumerGroup)
	})
}

func assertMetadata(t *testing.T, meta *KafkaMetadata) {
	require.Equal(t, "a", meta.internalBrokers[0])
	require.Equal(t, "a", meta.ConsumerGroup)
	require.Equal(t, "a", meta.ClientID)
	require.Equal(t, 2048, meta.MaxMessageBytes)
	require.True(t, meta.TLSSkipVerify)
	require.Equal(t, clientCertPemMock, meta.TLSClientCert)
	require.Equal(t, clientKeyMock, meta.TLSClientKey)
	require.Equal(t, caCertMock, meta.TLSCaCert)
	require.Equal(t, 200*time.Millisecond, meta.ConsumeRetryInterval)
	require.Equal(t, int32(1024*1024), meta.consumerFetchDefault)
	require.Equal(t, int32(1), meta.consumerFetchMin)
	require.Equal(t, 256, meta.channelBufferSize)
	require.Equal(t, 2*time.Second, meta.HeartbeatInterval)
	require.Equal(t, 30*time.Second, meta.SessionTimeout)
	require.Equal(t, 8*time.Minute, defaultClientConnectionTopicMetadataRefreshInterval)
	require.Equal(t, 0*time.Minute, defaultClientConnectionKeepAliveInterval)
}

func TestMissingBrokers(t *testing.T) {
	m := map[string]string{}
	k := getKafka()
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, "kafka error: missing 'brokers' attribute", err.Error())
}

func TestMissingAuthType(t *testing.T) {
	m := map[string]string{"brokers": "akfak.com:9092"}
	k := getKafka()
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, "kafka error: 'authType' attribute was missing or empty", err.Error())
}

func TestMetadataUpgradeNoAuth(t *testing.T) {
	m := map[string]string{"brokers": "akfak.com:9092", "authRequired": "false"}
	k := getKafka()
	upgraded, err := k.upgradeMetadata(m)
	require.NoError(t, err)
	require.Equal(t, noAuthType, upgraded["authType"])
}

func TestMetadataUpgradePasswordAuth(t *testing.T) {
	k := getKafka()
	m := map[string]string{"brokers": "akfak.com:9092", "authRequired": "true", "saslPassword": "sassapass"}
	upgraded, err := k.upgradeMetadata(m)
	require.NoError(t, err)
	require.Equal(t, passwordAuthType, upgraded["authType"])
}

func TestMetadataUpgradePasswordMTLSAuth(t *testing.T) {
	k := getKafka()
	m := map[string]string{"brokers": "akfak.com:9092", "authRequired": "true"}
	upgraded, err := k.upgradeMetadata(m)
	require.NoError(t, err)
	require.Equal(t, mtlsAuthType, upgraded["authType"])
}

func TestMissingSaslValues(t *testing.T) {
	k := getKafka()
	m := map[string]string{"brokers": "akfak.com:9092", "authType": "password"}
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, fmt.Sprintf("kafka error: missing SASL Username for authType '%s'", passwordAuthType), err.Error())

	m["saslUsername"] = "sassafras"

	meta, err = k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, fmt.Sprintf("kafka error: missing SASL Password for authType '%s'", passwordAuthType), err.Error())
}

func TestMissingSaslValuesOnUpgrade(t *testing.T) {
	k := getKafka()
	m := map[string]string{"brokers": "akfak.com:9092", "authRequired": "true", "saslPassword": "sassapass"}
	upgraded, err := k.upgradeMetadata(m)
	require.NoError(t, err)
	meta, err := k.getKafkaMetadata(upgraded)
	require.Error(t, err)
	require.Nil(t, meta)

	require.Equal(t, fmt.Sprintf("kafka error: missing SASL Username for authType '%s'", passwordAuthType), err.Error())
}

func TestMissingOidcValues(t *testing.T) {
	k := getKafka()
	m := map[string]string{"brokers": "akfak.com:9092", "authType": oidcAuthType}
	meta, err := k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)
	require.Equal(t, fmt.Sprintf("kafka error: missing OIDC Token Endpoint for authType '%s'", oidcAuthType), err.Error())

	m["oidcTokenEndpoint"] = "https://sassa.fra/"
	meta, err = k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)
	require.Equal(t, fmt.Sprintf("kafka error: missing OIDC Client ID for authType '%s'", oidcAuthType), err.Error())

	m["oidcClientID"] = "sassafras"
	meta, err = k.getKafkaMetadata(m)
	require.Error(t, err)
	require.Nil(t, meta)
	require.Equal(t, fmt.Sprintf("kafka error: missing OIDC Client Secret for authType '%s'", oidcAuthType), err.Error())

	// Check if missing scopes causes the default 'openid' to be used.
	m["oidcClientSecret"] = "sassapass"
	meta, err = k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.Contains(t, meta.internalOidcScopes, "openid")
}

func TestPresentSaslValues(t *testing.T) {
	k := getKafka()
	m := map[string]string{
		"brokers":      "akfak.com:9092",
		"authType":     passwordAuthType,
		"saslUsername": "sassafras",
		"saslPassword": "sassapass",
	}
	meta, err := k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, "sassafras", meta.SaslUsername)
	require.Equal(t, "sassapass", meta.SaslPassword)
}

func TestPresentOidcValues(t *testing.T) {
	k := getKafka()
	m := map[string]string{
		"brokers":           "akfak.com:9092",
		"authType":          oidcAuthType,
		"oidcTokenEndpoint": "https://sassa.fras",
		"oidcClientID":      "sassafras",
		"oidcClientSecret":  "sassapass",
		"oidcScopes":        "akfak",
	}
	meta, err := k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, "https://sassa.fras", meta.OidcTokenEndpoint)
	require.Equal(t, "sassafras", meta.OidcClientID)
	require.Equal(t, "sassapass", meta.OidcClientSecret)
	require.Contains(t, meta.internalOidcScopes, "akfak")
}

func TestInvalidAuthRequiredFlag(t *testing.T) {
	m := map[string]string{"brokers": "akfak.com:9092", "authRequired": "maybe?????????????"}
	k := getKafka()
	_, err := k.upgradeMetadata(m)
	require.Error(t, err)

	require.Equal(t, "kafka error: invalid value for 'authRequired' attribute", err.Error())
}

func TestInitialOffset(t *testing.T) {
	m := map[string]string{"consumerGroup": "a", "brokers": "a", "authRequired": "false", "initialOffset": "oldest"}
	k := getKafka()
	upgraded, err := k.upgradeMetadata(m)
	require.NoError(t, err)
	meta, err := k.getKafkaMetadata(upgraded)
	require.NoError(t, err)
	require.Equal(t, sarama.OffsetOldest, meta.internalInitialOffset)
	m["initialOffset"] = "newest"
	meta, err = k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.Equal(t, sarama.OffsetNewest, meta.internalInitialOffset)
}

func TestTls(t *testing.T) {
	k := getKafka()

	t.Run("disable tls", func(t *testing.T) {
		m := getBaseMetadata()
		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.NotNil(t, meta)
		c := &sarama.Config{}
		err = updateTLSConfig(c, meta)
		require.NoError(t, err)
		require.False(t, c.Net.TLS.Enable)
	})

	t.Run("wrong client cert format", func(t *testing.T) {
		m := getBaseMetadata()
		m[clientCert] = "clientCert"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: invalid client certificate", err.Error())
	})

	t.Run("wrong client key format", func(t *testing.T) {
		m := getBaseMetadata()
		m[clientKey] = "clientKey"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: invalid client key", err.Error())
	})

	t.Run("miss client key", func(t *testing.T) {
		m := getBaseMetadata()
		m[clientCert] = clientCertPemMock
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: clientKey or clientCert is missing", err.Error())
	})

	t.Run("miss client cert", func(t *testing.T) {
		m := getBaseMetadata()
		m[clientKey] = clientKeyMock
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: clientKey or clientCert is missing", err.Error())
	})

	t.Run("wrong ca cert format", func(t *testing.T) {
		m := getBaseMetadata()
		m[caCert] = "caCert"
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "kafka error: invalid ca certificate", err.Error())
	})

	t.Run("missing certificate for certificate auth", func(t *testing.T) {
		m := getBaseMetadata()
		m[authType] = certificateAuthType
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "missing CA certificate property 'caCert' for authType 'certificate'", err.Error())
	})
}

func TestAwsIam(t *testing.T) {
	k := getKafka()

	t.Run("missing aws region", func(t *testing.T) {
		m := getBaseMetadata()
		m[authType] = awsIAMAuthType
		meta, err := k.getKafkaMetadata(m)
		require.Error(t, err)
		require.Nil(t, meta)

		require.Equal(t, "missing AWS region property 'awsRegion' for authType 'awsiam'", err.Error())
	})
}

func TestMetadataConsumerFetchValues(t *testing.T) {
	k := getKafka()
	m := getCompleteMetadata()
	m["consumerFetchMin"] = "3"
	m["consumerFetchDefault"] = "2048"

	meta, err := k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.Equal(t, int32(3), meta.consumerFetchMin)
	require.Equal(t, int32(2048), meta.consumerFetchDefault)
}

func TestMetadataProducerValues(t *testing.T) {
	t.Run("using default producer values", func(t *testing.T) {
		k := getKafka()
		m := getCompleteMetadata()

		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.Equal(t, defaultClientConnectionTopicMetadataRefreshInterval, meta.ClientConnectionTopicMetadataRefreshInterval)
		require.Equal(t, defaultClientConnectionKeepAliveInterval, meta.ClientConnectionKeepAliveInterval)
	})

	t.Run("setting producer values explicitly", func(t *testing.T) {
		k := getKafka()
		m := getCompleteMetadata()
		m[clientConnectionTopicMetadataRefreshInterval] = "3m0s"
		m[clientConnectionKeepAliveInterval] = "4m0s"

		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.Equal(t, 3*time.Minute, meta.ClientConnectionTopicMetadataRefreshInterval)
		require.Equal(t, 4*time.Minute, meta.ClientConnectionKeepAliveInterval)
	})

	t.Run("setting producer invalid values so defaults take over", func(t *testing.T) {
		k := getKafka()
		m := getCompleteMetadata()
		m[clientConnectionTopicMetadataRefreshInterval] = "-1h40m0s"
		m[clientConnectionKeepAliveInterval] = "-1h40m0s"

		meta, err := k.getKafkaMetadata(m)
		require.NoError(t, err)
		require.Equal(t, defaultClientConnectionTopicMetadataRefreshInterval, meta.ClientConnectionTopicMetadataRefreshInterval)
		require.Equal(t, defaultClientConnectionKeepAliveInterval, meta.ClientConnectionKeepAliveInterval)
	})
}

func TestMetadataChannelBufferSize(t *testing.T) {
	k := getKafka()
	m := getCompleteMetadata()
	m["channelBufferSize"] = "128"

	meta, err := k.getKafkaMetadata(m)
	require.NoError(t, err)
	require.Equal(t, 128, meta.channelBufferSize)
}

func TestMetadataHeartbeartInterval(t *testing.T) {
	k := getKafka()

	t.Run("default heartbeat interval", func(t *testing.T) {
		// arrange
		m := getBaseMetadata()

		// act
		meta, err := k.getKafkaMetadata(m)

		// assert
		require.NoError(t, err)
		require.Equal(t, 3*time.Second, meta.HeartbeatInterval)
	})

	t.Run("with heartbeat interval set", func(t *testing.T) {
		// arrange
		m := getBaseMetadata()
		m["heartbeatInterval"] = "1s"

		// act
		meta, err := k.getKafkaMetadata(m)

		// assert
		require.NoError(t, err)
		require.Equal(t, 1*time.Second, meta.HeartbeatInterval)
	})
}

func TestMetadataSessionTimeout(t *testing.T) {
	k := getKafka()

	t.Run("default session timeout", func(t *testing.T) {
		// arrange
		m := getBaseMetadata()

		// act
		meta, err := k.getKafkaMetadata(m)

		// assert
		require.NoError(t, err)
		require.Equal(t, 10*time.Second, meta.SessionTimeout)
	})

	t.Run("with session timeout set", func(t *testing.T) {
		// arrange
		m := getBaseMetadata()
		m["sessionTimeout"] = "20s"

		// act
		meta, err := k.getKafkaMetadata(m)

		// assert
		require.NoError(t, err)
		require.Equal(t, 20*time.Second, meta.SessionTimeout)
	})
}

func TestGetEventMetadata(t *testing.T) {
	ts := time.Now()

	t.Run("no headers", func(t *testing.T) {
		m := sarama.ConsumerMessage{
			Headers: nil, Timestamp: ts, Key: []byte("MyKey"), Value: []byte("MyValue"), Partition: 0, Offset: 123, Topic: "TestTopic",
		}
		act := GetEventMetadata(&m, false)
		require.Len(t, act, 5)
		require.Equal(t, strconv.FormatInt(ts.UnixMilli(), 10), act["__timestamp"])
		require.Equal(t, "MyKey", act["__key"])
		require.Equal(t, "0", act["__partition"])
		require.Equal(t, "123", act["__offset"])
		require.Equal(t, "TestTopic", act["__topic"])
	})

	t.Run("with headers", func(t *testing.T) {
		headers := []*sarama.RecordHeader{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		}
		m := sarama.ConsumerMessage{
			Headers: headers, Timestamp: ts, Key: []byte("MyKey"), Value: []byte("MyValue"), Partition: 0, Offset: 123, Topic: "TestTopic",
		}
		act := GetEventMetadata(&m, false)
		require.Len(t, act, 7)
		require.Equal(t, strconv.FormatInt(ts.UnixMilli(), 10), act["__timestamp"])
		require.Equal(t, "MyKey", act["__key"])
		require.Equal(t, "0", act["__partition"])
		require.Equal(t, "123", act["__offset"])
		require.Equal(t, "TestTopic", act["__topic"])
		require.Equal(t, "value1", act["key1"])
		require.Equal(t, "value2", act["key2"])
	})

	t.Run("no key", func(t *testing.T) {
		m := sarama.ConsumerMessage{
			Headers: nil, Timestamp: ts, Key: nil, Value: []byte("MyValue"), Partition: 0, Offset: 123, Topic: "TestTopic",
		}
		act := GetEventMetadata(&m, false)
		require.Len(t, act, 4)
		require.Equal(t, strconv.FormatInt(ts.UnixMilli(), 10), act["__timestamp"])
		require.Equal(t, "0", act["__partition"])
		require.Equal(t, "123", act["__offset"])
		require.Equal(t, "TestTopic", act["__topic"])
	})

	t.Run("null message", func(t *testing.T) {
		act := GetEventMetadata(nil, false)
		require.Nil(t, act)
	})

	t.Run("key with invalid value escapeHeaders true", func(t *testing.T) {
		keyValue := "key1\xFF"
		escapedKeyValue := url.QueryEscape(keyValue)

		m := sarama.ConsumerMessage{
			Headers: nil, Timestamp: ts, Key: []byte(keyValue), Value: []byte("MyValue"), Partition: 0, Offset: 123, Topic: "TestTopic",
		}
		act := GetEventMetadata(&m, true)
		require.Equal(t, escapedKeyValue, act[keyMetadataKey])
	})

	t.Run("key with invalid value escapeHeaders false", func(t *testing.T) {
		keyValue := "key1\xFF"

		m := sarama.ConsumerMessage{
			Headers: nil, Timestamp: ts, Key: []byte(keyValue), Value: []byte("MyValue"), Partition: 0, Offset: 123, Topic: "TestTopic",
		}
		act := GetEventMetadata(&m, false)
		require.Equal(t, keyValue, act[keyMetadataKey])
	})

	t.Run("header with invalid value escapeHeaders true", func(t *testing.T) {
		headerKey := "key1"
		headerValue := "value1\xFF"
		escapedHeaderValue := url.QueryEscape(headerValue)

		headers := []*sarama.RecordHeader{
			{Key: []byte(headerKey), Value: []byte(headerValue)},
		}
		m := sarama.ConsumerMessage{
			Headers: headers, Timestamp: ts, Key: []byte("MyKey"), Value: []byte("MyValue"), Partition: 0, Offset: 123, Topic: "TestTopic",
		}
		act := GetEventMetadata(&m, true)
		require.Len(t, act, 6)
		require.Equal(t, escapedHeaderValue, act[headerKey])
	})

	t.Run("header with invalid value escapeHeaders false", func(t *testing.T) {
		headerKey := "key1"
		headerValue := "value1\xFF"

		headers := []*sarama.RecordHeader{
			{Key: []byte(headerKey), Value: []byte(headerValue)},
		}
		m := sarama.ConsumerMessage{
			Headers: headers, Timestamp: ts, Key: []byte("MyKey"), Value: []byte("MyValue"), Partition: 0, Offset: 123, Topic: "TestTopic",
		}
		act := GetEventMetadata(&m, false)
		require.Len(t, act, 6)
		require.Equal(t, headerValue, act[headerKey])
	})
}
