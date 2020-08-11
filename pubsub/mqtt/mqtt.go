// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// Keys
	mqttURL          = "url"
	mqttQOS          = "qos"
	mqttRetain       = "retain"
	mqttClientID     = "consumerID"
	mqttCleanSession = "cleanSession"
	mqttCACert       = "caCert"
	mqttClientCert   = "clientCert"
	mqttClientKey    = "clientKey"

	// errors
	errorMsgPrefix = "mqtt pub sub error:"

	// Defaults
	defaultQOS          = 0
	defaultRetain       = false
	defaultWait         = 3 * time.Second
	defaultCleanSession = true
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	client   mqtt.Client
	metadata *metadata
	logger   logger.Logger
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{logger: logger}
}

func parseMQTTMetaData(md pubsub.Metadata) (*metadata, error) {
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

	m.clientID = uuid.New().String()
	if val, ok := md.Properties[mqttClientID]; ok && val != "" {
		m.clientID = val
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
		m.tlsCfg.caCert = val
	}
	if val, ok := md.Properties[mqttClientCert]; ok && val != "" {
		m.tlsCfg.clientCert = val
	}
	if val, ok := md.Properties[mqttClientKey]; ok && val != "" {
		m.tlsCfg.clientKey = val
	}

	return &m, nil
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta

	uri, err := url.Parse(m.metadata.url)
	if err != nil {
		return err
	}
	client, err := m.connect(uri)
	if err != nil {
		return err
	}

	m.client = client

	m.logger.Debug("mqtt message bus initialization complete")
	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(req *pubsub.PublishRequest) error {
	m.logger.Debugf("mqtt publishing topic %s with data: %v", req.Topic, req.Data)

	token := m.client.Publish(req.Topic, m.metadata.qos, m.metadata.retain, req.Data)
	if !token.WaitTimeout(defaultWait) || token.Error() != nil {
		return fmt.Errorf("mqtt error from publish: %v", token.Error())
	}
	return nil
}

// Subscribe to the mqtt pub sub topic.
func (m *mqttPubSub) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	token := m.client.Subscribe(
		req.Topic,
		m.metadata.qos,
		func(client mqtt.Client, mqttMsg mqtt.Message) {
			handler(&pubsub.NewMessage{Topic: req.Topic, Data: mqttMsg.Payload()})
		})
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt error from subscribe: %v", err)
	}
	return nil
}

func (m *mqttPubSub) connect(uri *url.URL) (mqtt.Client, error) {
	opts := m.createClientOptions(uri)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(defaultWait) {
	}
	if err := token.Error(); err != nil {
		return nil, err
	}
	return client, nil
}

func (m *mqttPubSub) NewTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)
	tlsConfig.RootCAs = x509.NewCertPool()

	cert, err := tls.X509KeyPair([]byte(m.metadata.clientCert), []byte(m.metadata.clientKey))
	if err != nil {
		m.logger.Warnf("unable to load client certificate and key pair. Err: %v", err)
		return tlsConfig
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(m.metadata.caCert)); !ok {
		m.logger.Warnf("unable to load ca certificate. Err: %v", err)
	}

	return tlsConfig
}

func (m *mqttPubSub) createClientOptions(uri *url.URL) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(m.metadata.clientID)
	opts.SetCleanSession(m.metadata.cleanSession)
	opts.AddBroker(uri.Host)
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	opts.SetPassword(password)
	// tls config
	opts.SetTLSConfig(m.NewTLSConfig())
	return opts
}
