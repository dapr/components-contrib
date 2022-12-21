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
	"strconv"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	// errors.
	errorMsgPrefix = "amqp pub sub error:"
)

type metadata struct {
	tlsCfg
	url       string
	username  string
	password  string
	anonymous bool
}

type tlsCfg struct {
	caCert     string
	clientCert string
	clientKey  string
}

const (
	// Keys
	amqpUrl        = "url"
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
	m := metadata{anonymous: false}

	// required configuration settings
	if val, ok := md.Properties[amqpUrl]; ok && val != "" {
		m.url = val
	} else {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	// optional configuration settings
	if val, ok := md.Properties[anonymous]; ok && val != "" {
		var err error
		m.anonymous, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid anonymous %s, %s", errorMsgPrefix, val, err)
		}
	}

	if !m.anonymous {

		if val, ok := md.Properties[username]; ok && val != "" {
			m.username = val
		} else {
			return &m, fmt.Errorf("%s missing username", errorMsgPrefix)
		}

		if val, ok := md.Properties[password]; ok && val != "" {
			m.password = val
		} else {
			return &m, fmt.Errorf("%s missing username", errorMsgPrefix)
		}
	}

	if val, ok := md.Properties[amqpCACert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid caCert", errorMsgPrefix)
		}
		m.tlsCfg.caCert = val
	}
	if val, ok := md.Properties[amqpClientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid clientCert", errorMsgPrefix)
		}
		m.tlsCfg.clientCert = val
	}
	if val, ok := md.Properties[amqpClientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid clientKey", errorMsgPrefix)
		}
		m.tlsCfg.clientKey = val
	}

	return &m, nil
}
