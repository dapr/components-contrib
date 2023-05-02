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
	"time"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
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
)

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}

func parseAMQPMetaData(md pubsub.Metadata, log logger.Logger) (*metadata, error) {
	m := metadata{Anonymous: false}

	err := contribMetadata.DecodeMetadata(md.Properties, &m)
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
