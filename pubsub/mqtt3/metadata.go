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
	"time"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type mqttMetadata struct {
	pubsub.TLSProperties `mapstructure:",squash"`
	URL                  string `mapstructure:"url"`
	ConsumerID           string `mapstructure:"consumerID" mdignore:"true"`
	Qos                  byte   `mapstructure:"qos"`
	Retain               bool   `mapstructure:"retain"`
	CleanSession         bool   `mapstructure:"cleanSession"`
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

func parseMQTTMetaData(md pubsub.Metadata, log logger.Logger) (*mqttMetadata, error) {
	m := mqttMetadata{
		Qos:          defaultQOS,
		CleanSession: defaultCleanSession,
	}

	err := metadata.DecodeMetadata(md.Properties, &m)
	if err != nil {
		return &m, fmt.Errorf("mqtt pubsub error: %w", err)
	}

	// required configuration settings
	if m.URL == "" {
		return &m, errors.New("missing url")
	}

	// optional configuration settings
	if m.Qos > 7 { // bytes cannot be less than 0
		return &m, fmt.Errorf("invalid qos %d: %w", m.Qos, err)
	}

	// Note: the runtime sets the default value to the Dapr app ID if empty
	if m.ConsumerID == "" {
		return &m, errors.New("missing consumerID")
	}

	m.TLSProperties, err = pubsub.TLS(md.Properties)
	if err != nil {
		return &m, fmt.Errorf("invalid TLS configuration: %w", err)
	}

	return &m, nil
}
