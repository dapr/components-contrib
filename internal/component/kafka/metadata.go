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

	"github.com/dapr/components-contrib/metadata"
)

const (
	key                  = "partitionKey"
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
	noAuthType           = "none"
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
	Version                string              `mapstructure:"version"`
	internalVersion        sarama.KafkaVersion `mapstructure:"-"`
	internalOidcExtensions map[string]string   `mapstructure:"-"`
}

// upgradeMetadata updates metadata properties based on deprecated usage.
func (k *Kafka) upgradeMetadata(metadata map[string]string) (map[string]string, error) {
	authTypeVal, authTypePres := metadata[authType]
	authReqVal, authReqPres := metadata["authRequired"]
	saslPassVal, saslPassPres := metadata["saslPassword"]

	// If authType is not set, derive it from authRequired.
	if (!authTypePres || authTypeVal == "") && authReqPres && authReqVal != "" {
		k.logger.Warn("AuthRequired is deprecated, use AuthType instead.")
		validAuthRequired, err := strconv.ParseBool(authReqVal)
		if err == nil {
			if validAuthRequired {
				// If legacy authRequired was used, either SASL username or mtls is the method.
				if saslPassPres && saslPassVal != "" {
					// User has specified saslPassword, so intend for password auth.
					metadata[authType] = passwordAuthType
				} else {
					metadata[authType] = mtlsAuthType
				}
			} else {
				metadata[authType] = noAuthType
			}
		} else {
			return metadata, errors.New("kafka error: invalid value for 'authRequired' attribute")
		}
	}

	// if consumeRetryEnabled is not present, use component default value
	consumeRetryEnabledVal, consumeRetryEnabledPres := metadata[consumeRetryEnabled]
	if !consumeRetryEnabledPres || consumeRetryEnabledVal == "" {
		metadata[consumeRetryEnabled] = strconv.FormatBool(k.DefaultConsumeRetryEnabled)
	}

	return metadata, nil
}

// getKafkaMetadata returns new Kafka metadata.
func (k *Kafka) getKafkaMetadata(meta map[string]string) (*KafkaMetadata, error) {
	m := KafkaMetadata{
		ConsumeRetryInterval: 100 * time.Millisecond,
		internalVersion:      sarama.V2_0_0_0, //nolint:nosnakecase
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
			m.ConsumeRetryInterval = time.Duration(intVal) * time.Millisecond
		}
	}

	if m.Version != "" {
		version, err := sarama.ParseKafkaVersion(m.Version)
		if err != nil {
			return nil, errors.New("kafka error: invalid kafka version")
		}
		m.internalVersion = version
	}

	return &m, nil
}
