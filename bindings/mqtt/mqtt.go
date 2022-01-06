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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	// Keys.
	mqttURL               = "url"
	mqttTopic             = "topic"
	mqttQOS               = "qos"
	mqttRetain            = "retain"
	mqttClientID          = "consumerID"
	mqttCleanSession      = "cleanSession"
	mqttCACert            = "caCert"
	mqttClientCert        = "clientCert"
	mqttClientKey         = "clientKey"
	mqttBackOffMaxRetries = "backOffMaxRetries"

	// errors.
	errorMsgPrefix = "mqtt binding error:"

	// Defaults.
	defaultQOS          = 0
	defaultRetain       = false
	defaultWait         = 3 * time.Second
	defaultCleanSession = true
)

// MQTT allows sending and receiving data to/from an MQTT broker.
type MQTT struct {
	producer mqtt.Client
	consumer mqtt.Client
	metadata *metadata
	logger   logger.Logger

	ctx     context.Context
	cancel  context.CancelFunc
	backOff backoff.BackOff
}

// NewMQTT returns a new MQTT instance.
func NewMQTT(logger logger.Logger) *MQTT {
	return &MQTT{logger: logger}
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}

func parseMQTTMetaData(md bindings.Metadata) (*metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	if val, ok := md.Properties[mqttTopic]; ok && val != "" {
		m.topic = val
	} else {
		return &m, fmt.Errorf("%s missing topic", errorMsgPrefix)
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

	if val, ok := md.Properties[mqttClientID]; ok && val != "" {
		m.clientID = val
	} else {
		return &m, fmt.Errorf("%s missing consumerID", errorMsgPrefix)
	}

	m.cleanSession = defaultCleanSession
	if val, ok := md.Properties[mqttCleanSession]; ok && val != "" {
		var err error
		m.cleanSession, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid clean session %s, %s", errorMsgPrefix, val, err)
		}
	}

	if val, ok := md.Properties[mqttCACert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid ca certificate", errorMsgPrefix)
		}
		m.tlsCfg.caCert = val
	}
	if val, ok := md.Properties[mqttClientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid client certificate", errorMsgPrefix)
		}
		m.tlsCfg.clientCert = val
	}
	if val, ok := md.Properties[mqttClientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid client certificate key", errorMsgPrefix)
		}
		m.tlsCfg.clientKey = val
	}

	if val, ok := md.Properties[mqttBackOffMaxRetries]; ok && val != "" {
		backOffMaxRetriesInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid backOffMaxRetries %s, %s", errorMsgPrefix, val, err)
		}
		m.backOffMaxRetries = backOffMaxRetriesInt
	}

	return &m, nil
}

// Init does MQTT connection parsing.
func (m *MQTT) Init(metadata bindings.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta

	// mqtt broker allows only one connection at a given time from a clientID.
	producerClientID := fmt.Sprintf("%s-producer", m.metadata.clientID)
	p, err := m.connect(producerClientID)
	if err != nil {
		return err
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())

	// TODO: Make the backoff configurable for constant or exponential
	b := backoff.NewConstantBackOff(5 * time.Second)
	m.backOff = backoff.WithContext(b, m.ctx)

	m.producer = p

	m.logger.Debug("mqtt message bus initialization complete")

	return nil
}

func (m *MQTT) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (m *MQTT) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// MQTT client Publish() has an internal race condition in the default autoreconnect config.
	// To mitigate sporadic failures on the Dapr side, this implementation retries 3 times at
	// a fixed 200ms interval. This is not configurable to keep this as an implementation detail
	// for this component, as the additional public config metadata required could be replaced
	// by the more general Dapr APIs for resiliency moving forwards.
	cbo := backoff.NewConstantBackOff(200 * time.Millisecond)
	bo := backoff.WithMaxRetries(cbo, 3)
	bo = backoff.WithContext(bo, m.ctx)

	return nil, retry.NotifyRecover(func() error {
		m.logger.Debugf("mqtt publishing topic %s with data: %v", m.metadata.topic, req.Data)
		token := m.producer.Publish(m.metadata.topic, m.metadata.qos, m.metadata.retain, req.Data)
		if !token.WaitTimeout(defaultWait) || token.Error() != nil {
			return fmt.Errorf("mqtt error from publish: %v", token.Error())
		}
		return nil
	}, bo, func(err error, _ time.Duration) {
		m.logger.Debugf("Could not publish MQTT message. Retrying...: %v", err)
	}, func() {
		m.logger.Debug("Successfully published MQTT message after it previously failed")
	})
}

func (m *MQTT) handleMessage(handler func(*bindings.ReadResponse) ([]byte, error), mqttMsg mqtt.Message) error {
	msg := bindings.ReadResponse{Data: mqttMsg.Payload()}

	// paho.mqtt.golang requires that handlers never block or it can deadlock on client.Disconnect.
	// To ensure that the Dapr runtime does not hang on teardown on of the component, run the app's
	// handling code in a goroutine so that this handler function is always cancellable on Close().
	ch := make(chan error)
	go func(m *bindings.ReadResponse) {
		defer close(ch)
		_, err := handler(m)
		ch <- err
	}(&msg)

	select {
	case handlerErr := <-ch:
		if handlerErr != nil {
			return handlerErr
		}
		mqttMsg.Ack()
		return nil
	case <-m.ctx.Done():
		m.logger.Infof("Read context cancelled: %v", m.ctx.Err())
		return m.ctx.Err()
	}
}

func (m *MQTT) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)

	// reset synchronization
	if m.consumer != nil {
		m.logger.Warnf("re-initializing the subscriber")
		m.consumer.Disconnect(0)
		m.consumer = nil
	}

	// mqtt broker allows only one connection at a given time from a clientID.
	consumerClientID := fmt.Sprintf("%s-consumer", m.metadata.clientID)
	c, err := m.connect(consumerClientID)
	if err != nil {
		return err
	}
	m.consumer = c

	m.logger.Debugf("mqtt subscribing to topic %s", m.metadata.topic)
	token := m.consumer.Subscribe(m.metadata.topic, m.metadata.qos, func(client mqtt.Client, mqttMsg mqtt.Message) {
		b := m.backOff
		if m.metadata.backOffMaxRetries >= 0 {
			b = backoff.WithMaxRetries(m.backOff, uint64(m.metadata.backOffMaxRetries))
		}

		if err := retry.NotifyRecover(func() error {
			m.logger.Debugf("Processing MQTT message %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())
			return m.handleMessage(handler, mqttMsg)
		}, b, func(err error, d time.Duration) {
			m.logger.Errorf("Error processing MQTT message: %s/%d. Retrying...", mqttMsg.Topic(), mqttMsg.MessageID())
		}, func() {
			m.logger.Infof("Successfully processed MQTT message after it previously failed: %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())
		}); err != nil {
			m.logger.Errorf("Failed processing MQTT message: %s/%d: %v", mqttMsg.Topic(), mqttMsg.MessageID(), err)
		}
	})
	if err := token.Error(); err != nil {
		m.logger.Errorf("mqtt error from subscribe: %v", err)

		return err
	}
	<-sigterm

	return nil
}

func (m *MQTT) connect(clientID string) (mqtt.Client, error) {
	uri, err := url.Parse(m.metadata.url)
	if err != nil {
		return nil, err
	}
	opts := m.createClientOptions(uri, clientID)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(defaultWait) {
	}
	if err := token.Error(); err != nil {
		return nil, err
	}

	return client, nil
}

func (m *MQTT) newTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)

	if m.metadata.clientCert != "" && m.metadata.clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(m.metadata.clientCert), []byte(m.metadata.clientKey))
		if err != nil {
			m.logger.Warnf("unable to load client certificate and key pair. Err: %v", err)

			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if m.metadata.caCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(m.metadata.caCert)); !ok {
			m.logger.Warnf("unable to load ca certificate.")
		}
	}

	return tlsConfig
}

func (m *MQTT) createClientOptions(uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(clientID)
	opts.SetCleanSession(m.metadata.cleanSession)
	// URL scheme backward compatibility
	scheme := uri.Scheme
	switch scheme {
	case "mqtt":
		scheme = "tcp"
	case "mqtts", "tcps", "tls":
		scheme = "ssl"
	}
	opts.AddBroker(scheme + "://" + uri.Host)
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	opts.SetPassword(password)
	// tls config
	opts.SetTLSConfig(m.newTLSConfig())

	return opts
}

func (m *MQTT) Close() error {
	// Cancel any read callback handlers before Disconnect to prevent deadlocks.
	m.cancel()

	if m.consumer != nil {
		m.consumer.Disconnect(1)
	}
	m.producer.Disconnect(1)

	return nil
}
