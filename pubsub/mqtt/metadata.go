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
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type metadata struct {
	tlsCfg
	url                      string
	consumerID               string
	producerID               string
	qos                      byte
	retain                   bool
	cleanSession             bool
	maxRetriableErrorsPerSec int
}

type tlsCfg struct {
	caCert     string
	clientCert string
	clientKey  string
}

const (
	// Keys
	mqttURL                      = "url"
	mqttQOS                      = "qos"
	mqttRetain                   = "retain"
	mqttConsumerID               = "consumerID"
	mqttProducerID               = "producerID"
	mqttCleanSession             = "cleanSession"
	mqttCACert                   = "caCert"
	mqttClientCert               = "clientCert"
	mqttClientKey                = "clientKey"
	mqttMaxRetriableErrorsPerSec = "maxRetriableErrorsPerSec"

	// Defaults
	defaultQOS                      = 1
	defaultRetain                   = false
	defaultWait                     = 30 * time.Second
	defaultCleanSession             = false
	defaultMaxRetriableErrorsPerSec = 10
)

func parseMQTTMetaData(md pubsub.Metadata, log logger.Logger) (*metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	// optional configuration settings
	m.qos = defaultQOS
	if val, ok := md.Properties[mqttQOS]; ok && val != "" {
		qosInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid qos %s, %s", errorMsgPrefix, val, err)
		}
		m.qos = byte(qosInt)
	}

	m.retain = defaultRetain
	if val, ok := md.Properties[mqttRetain]; ok && val != "" {
		var err error
		m.retain, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid retain %s, %s", errorMsgPrefix, val, err)
		}
	}

	// Note: the runtime sets the default value to the Dapr app ID if empty
	if val, ok := md.Properties[mqttConsumerID]; ok && val != "" {
		m.consumerID = val
	} else {
		return &m, fmt.Errorf("%s missing consumerID", errorMsgPrefix)
	}

	if val, ok := md.Properties[mqttProducerID]; ok && val != "" {
		m.producerID = val
	}

	m.cleanSession = defaultCleanSession
	if val, ok := md.Properties[mqttCleanSession]; ok && val != "" {
		var err error
		m.cleanSession, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid cleanSession %s, %s", errorMsgPrefix, val, err)
		}
	}

	m.maxRetriableErrorsPerSec = defaultMaxRetriableErrorsPerSec
	if val, ok := md.Properties[mqttMaxRetriableErrorsPerSec]; ok && val != "" {
		var err error
		m.maxRetriableErrorsPerSec, err = strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid maxRetriableErrorsPerSec %s, %s", errorMsgPrefix, val, err)
		}
	}

	if val, ok := md.Properties[mqttCACert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid caCert", errorMsgPrefix)
		}
		m.tlsCfg.caCert = val
	}
	if val, ok := md.Properties[mqttClientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid clientCert", errorMsgPrefix)
		}
		m.tlsCfg.clientCert = val
	}
	if val, ok := md.Properties[mqttClientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid clientKey", errorMsgPrefix)
		}
		m.tlsCfg.clientKey = val
	}

	// Deprecated config option
	// TODO: Remove in the future
	if _, ok := md.Properties["backOffMaxRetries"]; ok {
		log.Warnf("Metadata property 'backOffMaxRetries' for component pubsub.mqtt has been deprecated and will be ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-mqtt/")
	}

	return &m, nil
}
