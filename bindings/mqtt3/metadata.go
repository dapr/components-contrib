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

package mqtt

import (
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/utils"
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

type metadata struct {
	tlsCfg
	url               string
	clientID          string
	qos               byte
	retain            bool
	cleanSession      bool
	backOffMaxRetries int
	topic             string
}

type tlsCfg struct {
	caCert     string
	clientCert string
	clientKey  string
}

func parseMQTTMetaData(md bindings.Metadata, log logger.Logger) (*metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, errors.New("missing url")
	}

	if val, ok := md.Properties[mqttTopic]; ok && val != "" {
		m.topic = val
	} else {
		return &m, errors.New("missing topic")
	}

	// optional configuration settings
	m.retain = defaultRetain
	if val, ok := md.Properties[mqttRetain]; ok && val != "" {
		m.retain = utils.IsTruthy(val)
	}

	if val, ok := md.Properties[mqttClientID]; ok && val != "" {
		m.clientID = val
	} else {
		return &m, errors.New("missing consumerID")
	}

	m.cleanSession = defaultCleanSession
	if val, ok := md.Properties[mqttCleanSession]; ok && val != "" {
		m.cleanSession = utils.IsTruthy(val)
	}

	if val, ok := md.Properties[mqttCACert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, errors.New("invalid ca certificate")
		}
		m.tlsCfg.caCert = val
	}
	if val, ok := md.Properties[mqttClientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, errors.New("invalid client certificate")
		}
		m.tlsCfg.clientCert = val
	}
	if val, ok := md.Properties[mqttClientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, errors.New("invalid client certificate key")
		}
		m.tlsCfg.clientKey = val
	}

	if val, ok := md.Properties[mqttBackOffMaxRetries]; ok && val != "" {
		backOffMaxRetriesInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("invalid backOffMaxRetries %s: %v", val, err)
		}
		m.backOffMaxRetries = backOffMaxRetriesInt
	}

	// Deprecated options
	m.qos = defaultQOS
	if val, ok := md.Properties[mqttQOS]; ok && val != "" {
		log.Warn("Metadata property 'qos' has been deprecated and is ignored; qos is set to 1")
	}

	return &m, nil
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}
