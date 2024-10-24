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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/dapr/kit/metadata"
)

const (
	key                  = "partitionKey"
	keyMetadataKey       = "__key"
	timestampMetadataKey = "__timestamp"
	offsetMetadataKey    = "__offset"
	partitionMetadataKey = "__partition"
	topicMetadataKey     = "__topic"
	skipVerify           = "skipVerify"
	caCert               = "caCert"
	certificateAuthType  = "certificate"
	clientCert           = "clientCert"
	clientKey            = "clientKey"
	consumeRetryEnabled  = "consumeRetryEnabled"
	consumeRetryInterval = "consumeRetryInterval"
	authType             = "authType"
	passwordAuthType     = "password"
	oidcAuthType         = "oidc"
	mtlsAuthType         = "mtls"
	awsIAMAuthType       = "awsiam"
	noAuthType           = "none"
	consumerFetchMin     = "consumerFetchMin"
	consumerFetchDefault = "consumerFetchDefault"
	channelBufferSize    = "channelBufferSize"
	valueSchemaType      = "valueSchemaType"

	// Kafka client config default values.
	// Refresh interval < keep alive time so that way connection can be kept alive indefinitely if desired.
	// This prevents write: broken pipe err when writer does not know connection was closed,
	// and continues to publish to closed connection.
	clientConnectionTopicMetadataRefreshInterval        = "clientConnectionTopicMetadataRefreshInterval"
	defaultClientConnectionTopicMetadataRefreshInterval = 8 * time.Minute // needs to be 8 as kafka default for killing idle connections is 9 min
	clientConnectionKeepAliveInterval                   = "clientConnectionKeepAliveInterval"
	defaultClientConnectionKeepAliveInterval            = time.Duration(0) // default to keep connection alive
)

type KafkaMetadata struct {
	Brokers                string              `mapstructure:"brokers"`
	internalBrokers        []string            `mapstructure:"-"`
	ConsumerGroup          string              `mapstructure:"consumerGroup"`
	ClientID               string              `mapstructure:"clientId"`
	AuthType               string              `mapstructure:"authType"`
	SaslUsername           string              `mapstructure:"saslUsername"`
	SaslPassword           string              `mapstructure:"saslPassword"`
	SaslMechanism          string              `mapstructure:"saslMechanism"`
	InitialOffset          string              `mapstructure:"initialOffset"`
	internalInitialOffset  int64               `mapstructure:"-"`
	MaxMessageBytes        int                 `mapstructure:"maxMessageBytes"`
	OidcTokenEndpoint      string              `mapstructure:"oidcTokenEndpoint"`
	OidcClientID           string              `mapstructure:"oidcClientID"`
	OidcClientSecret       string              `mapstructure:"oidcClientSecret"`
	OidcScopes             string              `mapstructure:"oidcScopes"`
	OidcExtensions         string              `mapstructure:"oidcExtensions"`
	internalOidcScopes     []string            `mapstructure:"-"`
	TLSDisable             bool                `mapstructure:"disableTls"`
	TLSSkipVerify          bool                `mapstructure:"skipVerify"`
	TLSCaCert              string              `mapstructure:"caCert"`
	TLSClientCert          string              `mapstructure:"clientCert"`
	TLSClientKey           string              `mapstructure:"clientKey"`
	ConsumeRetryEnabled    bool                `mapstructure:"consumeRetryEnabled"`
	ConsumeRetryInterval   time.Duration       `mapstructure:"consumeRetryInterval"`
	HeartbeatInterval      time.Duration       `mapstructure:"heartbeatInterval"`
	SessionTimeout         time.Duration       `mapstructure:"sessionTimeout"`
	Version                string              `mapstructure:"version"`
	EscapeHeaders          bool                `mapstructure:"escapeHeaders"`
	internalVersion        sarama.KafkaVersion `mapstructure:"-"`
	internalOidcExtensions map[string]string   `mapstructure:"-"`

	// configs for kafka client
	ClientConnectionTopicMetadataRefreshInterval time.Duration `mapstructure:"clientConnectionTopicMetadataRefreshInterval"`
	ClientConnectionKeepAliveInterval            time.Duration `mapstructure:"clientConnectionKeepAliveInterval"`

	// aws iam auth profile
	AWSAccessKey      string `mapstructure:"awsAccessKey"`
	AWSSecretKey      string `mapstructure:"awsSecretKey"`
	AWSSessionToken   string `mapstructure:"awsSessionToken"`
	AWSIamRoleArn     string `mapstructure:"awsIamRoleArn"`
	AWSStsSessionName string `mapstructure:"awsStsSessionName"`
	AWSRegion         string `mapstructure:"awsRegion"`
	channelBufferSize int    `mapstructure:"-"`

	consumerFetchMin     int32 `mapstructure:"-"`
	consumerFetchDefault int32 `mapstructure:"-"`

	// schema registry
	SchemaRegistryURL           string        `mapstructure:"schemaRegistryURL"`
	SchemaRegistryAPIKey        string        `mapstructure:"schemaRegistryAPIKey"`
	SchemaRegistryAPISecret     string        `mapstructure:"schemaRegistryAPISecret"`
	SchemaCachingEnabled        bool          `mapstructure:"schemaCachingEnabled"`
	SchemaLatestVersionCacheTTL time.Duration `mapstructure:"schemaLatestVersionCacheTTL"`
}

// upgradeMetadata updates metadata properties based on deprecated usage.
func (k *Kafka) upgradeMetadata(meta map[string]string) (map[string]string, error) {
	authTypeKey, authTypeVal, authTypeOk := metadata.GetMetadataPropertyWithMatchedKey(meta, authType)
	if authTypeKey == "" {
		authTypeKey = "authType"
	}
	authReqVal, authReqOk := metadata.GetMetadataProperty(meta, "authRequired")
	saslPassVal, saslPassOk := metadata.GetMetadataProperty(meta, "saslPassword")

	// If authType is not set, derive it from authRequired.
	if (!authTypeOk || authTypeVal == "") && authReqOk && authReqVal != "" {
		k.logger.Warn("AuthRequired is deprecated, use AuthType instead.")
		validAuthRequired, err := strconv.ParseBool(authReqVal)
		if err == nil {
			if validAuthRequired {
				// If legacy authRequired was used, either SASL username or mtls is the method.
				if saslPassOk && saslPassVal != "" {
					// User has specified saslPassword, so intend for password auth.
					meta[authTypeKey] = passwordAuthType
				} else {
					meta[authTypeKey] = mtlsAuthType
				}
			} else {
				meta[authTypeKey] = noAuthType
			}
		} else {
			return meta, errors.New("kafka error: invalid value for 'authRequired' attribute")
		}
	}

	return meta, nil
}

// getKafkaMetadata returns new Kafka metadata.
func (k *Kafka) getKafkaMetadata(meta map[string]string) (*KafkaMetadata, error) {
	m := KafkaMetadata{
		ConsumeRetryEnabled:                          k.DefaultConsumeRetryEnabled,
		ConsumeRetryInterval:                         100 * time.Millisecond,
		internalVersion:                              sarama.V2_0_0_0, //nolint:nosnakecase
		channelBufferSize:                            256,
		consumerFetchMin:                             1,
		consumerFetchDefault:                         1024 * 1024,
		ClientConnectionTopicMetadataRefreshInterval: defaultClientConnectionTopicMetadataRefreshInterval,
		ClientConnectionKeepAliveInterval:            defaultClientConnectionKeepAliveInterval,
		HeartbeatInterval:                            3 * time.Second,
		SessionTimeout:                               10 * time.Second,
		EscapeHeaders:                                false,
	}

	err := metadata.DecodeMetadata(meta, &m)
	if err != nil {
		return nil, err
	}

	// If ConsumerGroup is not set, use the 'consumerID' (which can be set by the runtime) as the consumer group so that each dapr runtime creates its own consumergroup
	if m.ConsumerGroup == "" {
		for k, v := range meta {
			if strings.ToLower(k) == "consumerid" { //nolint:gocritic
				m.ConsumerGroup = v
				break
			}
		}
	}

	k.logger.Debugf("ConsumerGroup='%s', ClientID='%s', saslMechanism='%s'", m.ConsumerGroup, m.ClientID, m.SaslMechanism)

	initialOffset, err := parseInitialOffset(meta["initialOffset"])
	if err != nil {
		return nil, err
	}
	m.internalInitialOffset = initialOffset

	if m.Brokers != "" {
		m.internalBrokers = strings.Split(m.Brokers, ",")
	} else {
		return nil, errors.New("kafka error: missing 'brokers' attribute")
	}

	k.logger.Debugf("Found brokers: %v", m.internalBrokers)

	if val, ok := meta[caCert]; ok && val != "" {
		if !isValidPEM(val) {
			return nil, errors.New("kafka error: invalid ca certificate")
		}
		m.TLSCaCert = val
	}

	if m.AuthType == "" {
		return nil, errors.New("kafka error: 'authType' attribute was missing or empty")
	}

	switch strings.ToLower(m.AuthType) {
	case passwordAuthType:
		if m.SaslUsername == "" {
			return nil, errors.New("kafka error: missing SASL Username for authType 'password'")
		}

		if m.SaslPassword == "" {
			return nil, errors.New("kafka error: missing SASL Password for authType 'password'")
		}
		k.logger.Debug("Configuring SASL password authentication.")
	case oidcAuthType:
		if m.OidcTokenEndpoint == "" {
			return nil, errors.New("kafka error: missing OIDC Token Endpoint for authType 'oidc'")
		}
		if m.OidcClientID == "" {
			return nil, errors.New("kafka error: missing OIDC Client ID for authType 'oidc'")
		}
		if m.OidcClientSecret == "" {
			return nil, errors.New("kafka error: missing OIDC Client Secret for authType 'oidc'")
		}
		if m.OidcScopes != "" {
			m.internalOidcScopes = strings.Split(m.OidcScopes, ",")
		} else {
			k.logger.Warn("Warning: no OIDC scopes specified, using default 'openid' scope only. This is a security risk for token reuse.")
			m.internalOidcScopes = []string{"openid"}
		}
		if m.OidcExtensions != "" {
			err = json.Unmarshal([]byte(m.OidcExtensions), &m.internalOidcExtensions)
			if err != nil || len(m.internalOidcExtensions) < 1 {
				return nil, errors.New("kafka error: improper OIDC Extensions format for authType 'oidc'")
			}
		}
		k.logger.Debug("Configuring SASL token authentication via OIDC.")
	case mtlsAuthType:
		if m.TLSClientCert != "" {
			if !isValidPEM(m.TLSClientCert) {
				return nil, errors.New("kafka error: invalid client certificate")
			}
		}
		if m.TLSClientKey != "" {
			if !isValidPEM(m.TLSClientKey) {
				return nil, errors.New("kafka error: invalid client key")
			}
		}
		// clientKey and clientCert need to be all specified or all not specified.
		if (m.TLSClientKey == "") != (m.TLSClientCert == "") {
			return nil, errors.New("kafka error: clientKey or clientCert is missing")
		}
		k.logger.Debug("Configuring mTLS authentication.")
	case noAuthType:
		k.logger.Debug("No authentication configured.")
	case certificateAuthType:
		if m.TLSCaCert == "" {
			return nil, errors.New("missing CA certificate property 'caCert' for authType 'certificate'")
		}
		k.logger.Debug("Configuring root certificate authentication.")
	case awsIAMAuthType:
		if m.AWSRegion == "" {
			return nil, errors.New("missing AWS region property 'awsRegion' for authType 'awsiam'")
		}
		k.logger.Debug("Configuring AWS IAM authentication.")
	default:
		return nil, errors.New("kafka error: invalid value for 'authType' attribute")
	}

	if m.TLSDisable {
		k.logger.Info("kafka: TLS connectivity to broker disabled")
	}

	if m.TLSSkipVerify {
		k.logger.Infof("kafka: you are using 'skipVerify' to skip server config verify which is unsafe!")
	}

	if val, ok := meta[consumeRetryInterval]; ok && val != "" {
		// if the string is a number, parse it as a duration in milliseconds - this is necessary because DecodeMetadata would parse a numeric string in seconds instead.
		// if the string is a duration, it was already correctly parsed in DecodeMetadata
		_, err := strconv.Atoi(val)
		if err == nil {
			intVal, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("kafka error: invalid value for '%s' attribute: %w", consumeRetryInterval, err)
			}
			m.ConsumeRetryInterval = time.Duration(intVal) * time.Millisecond //nolint:gosec
		}
	}

	if m.Version != "" {
		version, err := sarama.ParseKafkaVersion(m.Version)
		if err != nil {
			return nil, errors.New("kafka error: invalid kafka version")
		}
		m.internalVersion = version
	}

	if val, ok := meta[channelBufferSize]; ok && val != "" {
		v, err := strconv.Atoi(val)
		if err != nil {
			return nil, err
		}

		m.channelBufferSize = v
	}

	if val, ok := meta[consumerFetchDefault]; ok && val != "" {
		v, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return nil, err
		}

		m.consumerFetchDefault = int32(v)
	}

	if val, ok := meta[consumerFetchMin]; ok && val != "" {
		v, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return nil, err
		}

		m.consumerFetchMin = int32(v)
	}

	// confirm client connection fields are valid
	if m.ClientConnectionTopicMetadataRefreshInterval <= 0 {
		m.ClientConnectionTopicMetadataRefreshInterval = defaultClientConnectionTopicMetadataRefreshInterval
	}

	if m.ClientConnectionKeepAliveInterval < 0 {
		m.ClientConnectionKeepAliveInterval = defaultClientConnectionKeepAliveInterval
	}

	return &m, nil
}
