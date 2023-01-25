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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type metadata struct {
	pubsub.TLSProperties
	url          string
	consumerID   string
	qos          byte
	retain       bool
	cleanSession bool
}

const (
	// Keys
	mqttURL          = "url"
	mqttQOS          = "qos"
	mqttRetain       = "retain"
	mqttConsumerID   = "consumerID"
	mqttCleanSession = "cleanSession"

	// Defaults
	defaultQOS          = 1
	defaultRetain       = false
	defaultWait         = 20 * time.Second
	defaultCleanSession = false
)

func parseMQTTMetaData(md pubsub.Metadata, log logger.Logger) (*metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, errors.New("missing url")
	}

	// optional configuration settings
	m.qos = defaultQOS
	if val, ok := md.Properties[mqttQOS]; ok && val != "" {
		qosInt, err := strconv.Atoi(val)
		if err != nil || qosInt < 0 || qosInt > 7 {
			return &m, fmt.Errorf("invalid qos %s: %w", val, err)
		}
		m.qos = byte(qosInt)
	}

	m.retain = defaultRetain
	if val, ok := md.Properties[mqttRetain]; ok && val != "" {
		m.retain = utils.IsTruthy(val)
	}

	// Note: the runtime sets the default value to the Dapr app ID if empty
	if val, ok := md.Properties[mqttConsumerID]; ok && val != "" {
		m.consumerID = val
	} else {
		return &m, errors.New("missing consumerID")
	}

	m.cleanSession = defaultCleanSession
	if val, ok := md.Properties[mqttCleanSession]; ok && val != "" {
		m.cleanSession = utils.IsTruthy(val)
	}

	var err error
	m.TLSProperties, err = pubsub.TLS(md.Properties)
	if err != nil {
		return &m, fmt.Errorf("invalid TLS configuration: %w", err)
	}

	return &m, nil
}
