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

package amqp

import (
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	// errors.
	errorMsgPrefix = "amqp pub sub error:"
)

type metadata struct {
	tlsCfg    `mapstructure:",squash"`
	URL       string
	Username  string
	Password  string
	Anonymous bool

	// TopicAddressPrefix and QueueAddressPrefix are prepended to the AMQP
	// address of every link opened by this component, for topics and queues
	// respectively. Their defaults depend on the component type: pubsub.amqp
	// applies no prefix, and pubsub.solace.amqp applies the Solace convention.
	// Set them to the prefixes the broker is configured with, for example the
	// anycastPrefix and multicastPrefix of an ActiveMQ Artemis acceptor.
	TopicAddressPrefix string
	QueueAddressPrefix string
}

type tlsCfg struct {
	CaCert     string
	ClientCert string
	ClientKey  string
}

const (
	// Keys
	amqpURL        = "url"
	anonymous      = "anonymous"
	username       = "username"
	password       = "password"
	amqpCACert     = "caCert"
	amqpClientCert = "clientCert"
	amqpClientKey  = "clientKey"
	defaultWait    = 30 * time.Second

	// Address prefixes of the Solace addressing convention. They are the
	// defaults of pubsub.solace.amqp only, so that existing Solace
	// configurations are unaffected. pubsub.amqp defaults to no prefix.
	solaceTopicAddressPrefix = "topic://"
	solaceQueueAddressPrefix = "queue://"

	// genericAddressPrefix is the default of pubsub.amqp: the Dapr topic name
	// is used as the AMQP address unchanged, which is what brokers that
	// address destinations by name expect.
	genericAddressPrefix = ""

	// Optional scheme of a topic name, selecting which prefix is applied.
	topicScheme = "topic:"
	queueScheme = "queue:"
)

// addressFor returns the AMQP address a link is opened on for the given Dapr
// topic name.
//
// A topic name may carry an optional "topic:" or "queue:" scheme, selecting
// which of the two configured prefixes is applied; a bare topic name is
// addressed as a topic. A topic name that already carries one of the configured
// prefixes is used as the address as-is.
func (m *metadata) addressFor(topic string) string {
	// An empty topic has no address. Without this, a configured prefix would
	// make the result the bare prefix, which is not empty and so passes the
	// callers' emptiness check.
	if topic == "" {
		return ""
	}

	if hasPrefix(topic, m.TopicAddressPrefix) || hasPrefix(topic, m.QueueAddressPrefix) {
		return topic
	}

	switch {
	case strings.HasPrefix(topic, queueScheme):
		return m.QueueAddressPrefix + trimScheme(topic, queueScheme)
	case strings.HasPrefix(topic, topicScheme):
		return m.TopicAddressPrefix + trimScheme(topic, topicScheme)
	default:
		return m.TopicAddressPrefix + topic
	}
}

// trimScheme removes a scheme, and the "//" that follows it in a fully
// qualified address, from the front of a topic name.
func trimScheme(topic string, scheme string) string {
	return strings.TrimPrefix(strings.TrimPrefix(topic, scheme), "//")
}

// hasPrefix reports whether s begins with a non-empty prefix.
func hasPrefix(s, prefix string) bool {
	return prefix != "" && strings.HasPrefix(s, prefix)
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}

// parseAMQPMetaData builds the component metadata. defaultTopicPrefix and
// defaultQueuePrefix are supplied by the constructor rather than read from a
// package constant, because the two registered component types start from
// different addressing conventions: pubsub.amqp addresses destinations by name,
// while pubsub.solace.amqp keeps the Solace prefixes.
func parseAMQPMetaData(md pubsub.Metadata, log logger.Logger, defaultTopicPrefix, defaultQueuePrefix string) (*metadata, error) {
	m := metadata{
		Anonymous:          false,
		TopicAddressPrefix: defaultTopicPrefix,
		QueueAddressPrefix: defaultQueuePrefix,
	}

	err := kitmd.DecodeMetadata(md.Properties, &m)
	if err != nil {
		return &m, fmt.Errorf("%s %s", errorMsgPrefix, err)
	}

	// required configuration settings
	if m.URL == "" {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	// optional configuration settings
	if !m.Anonymous {
		if m.Username == "" {
			return &m, fmt.Errorf("%s missing username", errorMsgPrefix)
		}

		if m.Password == "" {
			return &m, fmt.Errorf("%s missing username", errorMsgPrefix)
		}
	}

	if m.CaCert != "" {
		if !isValidPEM(m.CaCert) {
			return &m, fmt.Errorf("%s invalid caCert", errorMsgPrefix)
		}
	}
	if m.ClientCert != "" {
		if !isValidPEM(m.ClientCert) {
			return &m, fmt.Errorf("%s invalid clientCert", errorMsgPrefix)
		}
	}
	if m.ClientKey != "" {
		if !isValidPEM(m.ClientKey) {
			return &m, fmt.Errorf("%s invalid clientKey", errorMsgPrefix)
		}
	}

	return &m, nil
}
