/*
Copyright 2023 The Dapr Authors
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

package mqtt

import (
	"encoding/pem"
	"errors"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// Keys.
	mqttURL               = "url"
	mqttTopic             = "topic"
	mqttQOS               = "qos" // This key is deprecated
	mqttRetain            = "retain"
	mqttClientID          = "consumerID"
	mqttCleanSession      = "cleanSession"
	mqttCACert            = "caCert"
	mqttClientCert        = "clientCert"
	mqttClientKey         = "clientKey"
	mqttBackOffMaxRetries = "backOffMaxRetries"

	// Defaults.
	defaultQOS          = 1
	defaultRetain       = false
	defaultWait         = 10 * time.Second
	defaultCleanSession = false
)

//nolint:stylecheck
type mqtt3Metadata struct {
	tlsCfg            `mapstructure:",squash"`
	Url               string `mapstructure:"url"`
	ClientID          string `mapstructure:"consumerID"`
	Qos               byte   `mapstructure:"-"`
	Retain            bool   `mapstructure:"retain"`
	CleanSession      bool   `mapstructure:"cleanSession"`
	BackOffMaxRetries int    `mapstructure:"backOffMaxRetries"`
	Topic             string `mapstructure:"topic"`
}

type tlsCfg struct {
	CaCert     string `mapstructure:"caCert"`
	ClientCert string `mapstructure:"clientCert"`
	ClientKey  string `mapstructure:"clientKey"`
}

func parseMQTTMetaData(md bindings.Metadata, log logger.Logger) (mqtt3Metadata, error) {
	m := mqtt3Metadata{
		Retain:       defaultRetain,
		CleanSession: defaultCleanSession,
	}

	err := metadata.DecodeMetadata(md.Properties, &m)
	if err != nil {
		return m, err
	}

	// required configuration settings
	if m.Url == "" {
		return m, errors.New("missing url")
	}

	if m.Topic == "" {
		return m, errors.New("missing topic")
	}

	if m.ClientID == "" {
		return m, errors.New("missing consumerID")
	}

	if m.CaCert != "" {
		if !isValidPEM(m.CaCert) {
			return m, errors.New("invalid ca certificate")
		}
	}
	if m.ClientCert != "" {
		if !isValidPEM(m.ClientCert) {
			return m, errors.New("invalid client certificate")
		}
	}
	if m.ClientKey != "" {
		if !isValidPEM(m.ClientKey) {
			return m, errors.New("invalid client certificate key")
		}
	}

	// Deprecated options
	m.Qos = defaultQOS
	if val, ok := md.Properties[mqttQOS]; ok && val != "" {
		log.Warn("Metadata property 'qos' has been deprecated and is ignored; qos is set to 1")
	}

	return m, nil
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}
