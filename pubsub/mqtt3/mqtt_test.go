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
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type mqttMessage struct {
	data     []byte
	retained bool
	topic    string
	qos      byte
}

var _ mqtt.Message = (*mqttMessage)(nil)

func (m mqttMessage) Duplicate() bool {
	return false
}

func (m mqttMessage) Qos() byte {
	return m.qos
}

func (m mqttMessage) Retained() bool {
	return m.retained
}

func (m mqttMessage) Topic() string {
	return m.topic
}

func (m mqttMessage) MessageID() uint16 {
	return uint16(rand.Intn(math.MaxUint16 + 1)) //nolint:gosec
}

func (m mqttMessage) Payload() []byte {
	return m.data
}

func (m mqttMessage) Ack() {
	// nop
}

type mockedMQTTToken struct {
	m        sync.RWMutex
	complete chan struct{}
	err      error
}

var _ mqtt.Token = (*mockedMQTTToken)(nil)

func (t *mockedMQTTToken) Wait() bool {
	<-t.complete
	return true
}

func (t *mockedMQTTToken) WaitTimeout(d time.Duration) bool {
	timer := time.NewTimer(d)
	select {
	case <-t.complete:
		if !timer.Stop() {
			<-timer.C
		}
		return true
	case <-timer.C:
	}

	return false
}

func (t *mockedMQTTToken) Done() <-chan struct{} {
	return t.complete
}

func (t *mockedMQTTToken) flowComplete() {
	select {
	case <-t.complete:
	default:
		close(t.complete)
	}
}

func (t *mockedMQTTToken) Error() error {
	t.m.RLock()
	defer t.m.RUnlock()
	return t.err
}

type mockedMQTTClient struct {
	msgCh chan mqttMessage
}

var _ mqtt.Client = (*mockedMQTTClient)(nil)

func newMockedMQTTClient(ch chan mqttMessage) *mockedMQTTClient {
	return &mockedMQTTClient{
		msgCh: ch,
	}
}

func (m mockedMQTTClient) IsConnected() bool {
	return true
}

func (m mockedMQTTClient) IsConnectionOpen() bool {
	return true
}

func (m mockedMQTTClient) Connect() mqtt.Token {
	token := &mockedMQTTToken{complete: make(chan struct{})}
	token.flowComplete()

	return token
}

func (m mockedMQTTClient) Disconnect(quiesce uint) {
	// nop
}

func (m mockedMQTTClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	token := &mockedMQTTToken{complete: make(chan struct{})}

	msg := mqttMessage{
		data:     payload.([]byte),
		retained: retained,
		topic:    topic,
		qos:      qos,
	}
	m.msgCh <- msg

	token.flowComplete()

	return token
}

func (m mockedMQTTClient) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	token := &mockedMQTTToken{complete: make(chan struct{})}
	token.flowComplete()

	go func() {
		for msg := range m.msgCh {
			callback(m, msg)
		}
	}()

	return token
}

func (m mockedMQTTClient) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	token := &mockedMQTTToken{complete: make(chan struct{})}
	token.flowComplete()

	go func() {
		for msg := range m.msgCh {
			callback(m, msg)
		}
	}()

	return token
}

func (m mockedMQTTClient) Unsubscribe(topics ...string) mqtt.Token {
	token := &mockedMQTTToken{complete: make(chan struct{})}
	token.flowComplete()

	return token
}

func (m mockedMQTTClient) AddRoute(topic string, callback mqtt.MessageHandler) {
	// nop
}

func (m mockedMQTTClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

func getFakeProperties() map[string]string {
	return map[string]string{
		"consumerID":     "client",
		mqttURL:          "tcp://fakeUser:fakePassword@fake.mqtt.host:1883",
		mqttQOS:          "1",
		mqttRetain:       "true",
		mqttCleanSession: "false",
	}
}

func TestParseMetadata(t *testing.T) {
	log := logger.NewLogger("test")
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.URL)
		assert.Equal(t, byte(1), m.Qos)
		assert.Equal(t, true, m.Retain)
		assert.Equal(t, false, m.CleanSession)
	})

	t.Run("missing consumerID", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties["consumerID"] = ""
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "missing consumerID")
	})

	t.Run("url is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[mqttURL] = ""

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.ErrorContains(t, err, "missing url")
		assert.Equal(t, fakeProperties[mqttURL], m.URL)
	})

	t.Run("qos and retain is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		delete(fakeMetaData.Properties, mqttQOS)
		delete(fakeMetaData.Properties, mqttRetain)

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.URL)
		assert.Equal(t, byte(1), m.Qos)
		assert.Equal(t, false, m.Retain)
	})

	t.Run("invalid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[pubsub.CACert] = "randomNonPEMBlockCA"
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid caCert")
	})

	t.Run("valid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[pubsub.CACert] = "-----BEGIN CERTIFICATE-----\nMIICyDCCAbACCQDb8BtgvbqW5jANBgkqhkiG9w0BAQsFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1MzU4WhcNMjUw\nODEyMDY1MzU4WjAmMQswCQYDVQQGEwJJTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0\nQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDEXte1GBxFJaygsEnK\nHV2AxazZW6Vppv+i50AuURHcaGo0i8G5CTfHzSKrYtTFfBskUspl+2N8GPV5c8Eb\ng+PP6YFn1wiHVz+wRSk3BD35DcGOT2o4XsJw5tiAzJkbpAOYCYl7KAM+BtOf41uC\nd6TdqmawhRGtv1ND2WtyJOT6A3KcUfjhL4TFEhWoljPJVay4TQoJcZMAImD/Xcxw\n6urv6wmUJby3/RJ3I46ZNH3zxEw5vSq1TuzuXxQmfPJG0ZPKJtQZ2nkZ3PNZe4bd\nNUa83YgQap7nBhYdYMMsQyLES2qy3mPcemBVoBWRGODel4PMEcsQiOhAyloAF2d3\nhd+LAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAK13X5JYBy78vHYoP0Oq9fe5XBbL\nuRM8YLnet9b/bXTGG4SnCCOGqWz99swYK7SVyR5l2h8SAoLzeNV61PtaZ6fHrbar\noxSL7BoRXOhMH6LQATadyvwlJ71uqlagqya7soaPK09TtfzeebLT0QkRCWT9b9lQ\nDBvBVCaFidynJL1ts21m5yUdIY4JSu4sGZGb4FRGFdBv/hD3wH8LAkOppsSv3C/Q\nkfkDDSQzYbdMoBuXmafvi3He7Rv+e6Tj9or1rrWdx0MIKlZPzz4DOe5Rh112uRB9\n7xPHJt16c+Ya3DKpchwwdNcki0vFchlpV96HK8sMCoY9kBzPhkEQLdiBGv4=\n-----END CERTIFICATE-----\n"
		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		block, _ := pem.Decode([]byte(m.TLSProperties.CACert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse ca certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprMqttTestCA", cert.Subject.CommonName)
	})

	t.Run("invalid client certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[pubsub.ClientCert] = "randomNonPEMBlockClientCert"
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid clientCert")
	})

	t.Run("valid client certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[pubsub.ClientCert] = "-----BEGIN CERTIFICATE-----\nMIICzDCCAbQCCQDBKDMS3SHsDzANBgkqhkiG9w0BAQUFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1NTE1WhcNMjEw\nODA3MDY1NTE1WjAqMQswCQYDVQQGEwJJTjEbMBkGA1UEAwwSZGFwck1xdHRUZXN0\nQ2xpZW50MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5IDfsGI2pb4W\nt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu++M6yi9ABaBiYChpxbylqIeAn/HT\n3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsMfzO9p1oMpTnRdJCHYinE+oqVced5\nHI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPvui8WzJq9TMkEhEME+5hs3VKyKZr2\nqjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMWwzj2G+Rj8PMcBjoKeCLQL9uQh7f1\nTWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+Ur2b5+7/hoqnTzReJ3XUe1jM3l44f\nl0rOf4hu2QIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAT9yoIeX0LTsvx7/b+8V3a\nkP+j8u97QCc8n5xnMpivcMEk5cfqXX5Llv2EUJ9kBsynrJwT7ujhTJXSA/zb2UdC\nKH8PaSrgIlLwQNZMDofbz6+zPbjStkgne/ZQkTDIxY73sGpJL8LsQVO9p2KjOpdj\nSf9KuJhLzcHolh7ry3ZrkOg+QlMSvseeDRAxNhpkJrGQ6piXoUiEeKKNa0rWTMHx\nIP1Hqj+hh7jgqoQR48NL2jNng7I64HqTl6Mv2fiNfINiw+5xmXTB0QYkGU5NvPBO\naKcCRcGlU7ND89BogQPZsl/P04tAuQqpQWffzT4sEEOyWSVGda4N2Ys3GSQGBv8e\n-----END CERTIFICATE-----\n"
		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		block, _ := pem.Decode([]byte(m.TLSProperties.ClientCert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse client certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprMqttTestClient", cert.Subject.CommonName)
	})

	t.Run("invalid client certificate key", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[pubsub.ClientKey] = "randomNonPEMBlockClientKey"
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid clientKey")
	})

	t.Run("valid client certificate key", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[pubsub.ClientKey] = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA5IDfsGI2pb4Wt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu\n++M6yi9ABaBiYChpxbylqIeAn/HT3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsM\nfzO9p1oMpTnRdJCHYinE+oqVced5HI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPv\nui8WzJq9TMkEhEME+5hs3VKyKZr2qjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMW\nwzj2G+Rj8PMcBjoKeCLQL9uQh7f1TWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+U\nr2b5+7/hoqnTzReJ3XUe1jM3l44fl0rOf4hu2QIDAQABAoIBAQCVMINb4TP20P55\n9IPyqlxjhPT563hijXK+lhMJyiBDPavOOs7qjLikq2bshYPVbm1o2jt6pkXXqAeB\n5t/d20fheQQurYyPfxecNBZuL78duwbcUy28m2aXLlcVRYO4zGhoMgdW4UajoNLV\nT/UIiDONWGyhTHXMHdP+6h9UOmvs3o4b225AuLrw9n6QO5I1Se8lcfOTIqR1fy4O\nGsUWEQPdW0X3Dhgpx7kDIuBTAQzbjD31PCR1U8h2wsCeEe6hPCrsMbo/D019weol\ndi40tbWR1/oNz0+vro2d9YDPJkXN0gmpT51Z4YJoexZBdyzO5z4DMSdn5yczzt6p\nQq8LsXAFAoGBAPYXRbC4OxhtuC+xr8KRkaCCMjtjUWFbFWf6OFgUS9b5uPz9xvdY\nXo7wBP1zp2dS8yFsdIYH5Six4Z5iOuDR4sVixzjabhwedL6bmS1zV5qcCWeASKX1\nURgSkfMmC4Tg3LBgZ9YxySFcVRjikxljkS3eK7Mp7Xmj5afe7qV73TJfAoGBAO20\nTtw2RGe02xnydZmmwf+NpQHOA9S0JsehZA6NRbtPEN/C8bPJIq4VABC5zcH+tfYf\nzndbDlGhuk+qpPA590rG5RSOUjYnQFq7njdSfFyok9dXSZQTjJwFnG2oy0LmgjCe\nROYnbCzD+a+gBKV4xlo2M80OLakQ3zOwPT0xNRnHAoGATLEj/tbrU8mdxP9TDwfe\nom7wyKFDE1wXZ7gLJyfsGqrog69y+lKH5XPXmkUYvpKTQq9SARMkz3HgJkPmpXnD\nelA2Vfl8pza2m1BShF+VxZErPR41hcLV6vKemXAZ1udc33qr4YzSaZskygSSYy8s\nZ2b9p3BBmc8CGzbWmKvpW3ECgYEAn7sFLxdMWj/+5221Nr4HKPn+wrq0ek9gq884\n1Ep8bETSOvrdvolPQ5mbBKJGsLC/h5eR/0Rx18sMzpIF6eOZ2GbU8z474mX36cCf\nrd9A8Gbbid3+9IE6gHGIz2uYwujw3UjNVbdyCpbahvjJhoQlDePUZVu8tRpAUpSA\nYklZvGsCgYBuIlOFTNGMVUnwfzrcS9a/31LSvWTZa8w2QFjsRPMYFezo2l4yWs4D\nPEpeuoJm+Gp6F6ayjoeyOw9mvMBH5hAZr4WjbiU6UodzEHREAsLAzCzcRyIpnDE6\nPW1c3j60r8AHVufkWTA+8B9WoLC5MqcYTV3beMGnNGGqS2PeBom63Q==\n-----END RSA PRIVATE KEY-----\n"
		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, m.TLSProperties.ClientKey, "failed to parse valid client certificate key")
	})
}

func Test_buildRegexForTopic(t *testing.T) {
	type args struct {
		topicName string
	}
	tests := []struct {
		name       string
		args       args
		regex      string
		tryMatches map[string]bool
	}{
		{
			name:  "no wildcard",
			args:  args{topicName: "hello world"},
			regex: "",
		},
		{
			name:  "#",
			args:  args{topicName: "#"},
			regex: "^(.*)$",
			tryMatches: map[string]bool{
				"helloworld":     true,
				"helloworld/":    true,
				"helloworld/22":  true,
				"/helloworld":    true,
				"/helloworld/":   true,
				"/helloworld/22": true,
				"Ei fu. Siccome immobile, dato il mortal sospiro, stette la spoglia immemore.": true,
				"🐶":         true,
				"🐶/foo":     true,
				"🐶/foo/bar": true,
			},
		},
		{
			// This should be forbidden by the specs, but apparently it works in brokers
			name:  "#/foo",
			args:  args{topicName: "#/foo"},
			regex: "^(.*)/foo$",
			tryMatches: map[string]bool{
				"helloworld":          false,
				"helloworld/":         false,
				"helloworld/22":       false,
				"helloworld/foo":      true,
				"hello/world/foo":     true,
				"helloworld/foo/bar":  false,
				"/helloworld":         false,
				"/helloworld/":        false,
				"/helloworld/22":      false,
				"/helloworld/foo":     true,
				"/hello/world/foo":    true,
				"/helloworld/foo/bar": false,
				"🐶":                   false,
				"🐶/foo":               true,
				"🐶/😄/foo":             true,
				"🐶/foo/bar":           false,
				"🐶/😄":                 false,
			},
		},
		{
			name:  "+",
			args:  args{topicName: "+"},
			regex: `^([^\/]*)$`,
			tryMatches: map[string]bool{
				"helloworld":     true,
				"helloworld/":    false,
				"helloworld/22":  false,
				"/helloworld":    false,
				"/helloworld/":   false,
				"/helloworld/22": false,
				"Ei fu. Siccome immobile, dato il mortal sospiro, stette la spoglia immemore.": true,
				"🐶":         true,
				"🐶/foo":     false,
				"🐶/foo/bar": false,
			},
		},
		{
			name:  "+/foo",
			args:  args{topicName: "+/foo"},
			regex: `^([^\/]*)/foo$`,
			tryMatches: map[string]bool{
				"helloworld":          false,
				"helloworld/":         false,
				"helloworld/22":       false,
				"helloworld/foo":      true,
				"hello/world/foo":     false,
				"helloworld/foo/bar":  false,
				"/helloworld":         false,
				"/helloworld/":        false,
				"/helloworld/22":      false,
				"/helloworld/foo":     false,
				"/hello/world/foo":    false,
				"/helloworld/foo/bar": false,
				"🐶":                   false,
				"🐶/foo":               true,
				"🐶/😄/foo":             false,
				"🐶/foo/bar":           false,
				"🐶/😄":                 false,
			},
		},
		{
			name:  "foo# (invalid)",
			args:  args{topicName: "foo#"},
			regex: "",
		},
		{
			name:  "foo+ (invalid)",
			args:  args{topicName: "foo+"},
			regex: "",
		},
		{
			name:  "foo/#",
			args:  args{topicName: "foo/#"},
			regex: "^foo(.*)$",
			tryMatches: map[string]bool{
				"helloworld":      false,
				"foo":             true,
				"foo/":            true,
				"foo/bar":         true,
				"/helloworld":     false,
				"foo/helloworld":  true,
				"foo/hello/world": true,
				"hello/world":     false,
				"🐶":               false,
				"foo/🐶":           true,
				"🐶/foo/bar":       false,
				"foo/🐶/bar":       true,
			},
		},
		{
			// This should be forbidden by the specs, but apparently it works in brokers
			name:  "foo/#/bar",
			args:  args{topicName: "foo/#/bar"},
			regex: "^foo/(.*)/bar$",
			tryMatches: map[string]bool{
				"helloworld":       false,
				"foo/":             false,
				"foo/bar":          false,
				"foo/hi/bar":       true,
				"foo/hi/hi/hi/bar": true,
				"foo/hi/world":     false,
			},
		},
		{
			name:  "foo/+",
			args:  args{topicName: "foo/+"},
			regex: `^foo((\/|)[^\/]*)$`,
			tryMatches: map[string]bool{
				"helloworld":      false,
				"foo":             true,
				"foo/":            true,
				"foo/bar":         true,
				"/helloworld":     false,
				"foo/helloworld":  true,
				"foo/hello/world": false,
				"hello/world":     false,
				"🐶":               false,
				"foo/🐶":           true,
				"🐶/foo/bar":       false,
				"foo/🐶/bar":       false,
			},
		},
		{
			name:  "foo/+/bar",
			args:  args{topicName: "foo/+/bar"},
			regex: `^foo/([^\/]*)/bar$`,
			tryMatches: map[string]bool{
				"helloworld":       false,
				"foo/":             false,
				"foo/bar":          false,
				"foo/hi/bar":       true,
				"foo/hi/hi/hi/bar": false,
				"foo/hi/world":     false,
			},
		},
		{
			// https://github.com/dapr/components-contrib/issues/1881#issuecomment-1191571216
			name:  "event/data/+/+/+/1/1",
			args:  args{topicName: "event/data/+/+/+/1/1"},
			regex: `^event/data/([^\/]*)/([^\/]*)/([^\/]*)/1/1$`,
			tryMatches: map[string]bool{
				"helloworld":               false,
				"event/data":               false,
				"event/data/a/b/c/1/1":     true,
				"event/data/a/b/c/1/2":     false,
				"event/data/a/b/1/1":       false,
				"event/data/a/bbb/ccc/1/1": true,
			},
		},
		{
			name:  "+/+/+/1/1",
			args:  args{topicName: "+/+/+/1/1"},
			regex: `^([^\/]*)/([^\/]*)/([^\/]*)/1/1$`,
			tryMatches: map[string]bool{
				"helloworld":    false,
				"a/b/c/":        false,
				"a/b/c/1":       false,
				"a/b/c/1/1":     true,
				"a/b/c/1/2":     false,
				"a/b/1/1":       false,
				"a/bbb/ccc/1/1": true,
			},
		},
		{
			name:  "+/#/1/1",
			args:  args{topicName: "+/#/1/1"},
			regex: `^([^\/]*)/(.*)/1/1$`,
			tryMatches: map[string]bool{
				"helloworld":         false,
				"a/b/c/":             false,
				"a/b/c/1":            false,
				"a/b/c/1/1":          true,
				"a/b/c/1/2":          false,
				"a/b/1/1":            true,
				"a/bbb/ccc/1/1":      true,
				"aa/bbb/ccc/ddd/1/1": true,
			},
		},
		{
			name:  "foo/+/bar/+",
			args:  args{topicName: "foo/+/bar/+"},
			regex: `^foo/([^\/]*)/bar((\/|)[^\/]*)$`,
			tryMatches: map[string]bool{
				"helloworld":         false,
				"foo/":               false,
				"foo/bar":            false,
				"foo/hi/bar":         true,
				"foo/hi/bar/foo":     true,
				"foo/hi/bar/foo/hi":  false,
				"foo/hi/bar/foo/bar": false,
				"foo/hi/hi/hi/bar":   false,
				"foo/hi/world":       false,
			},
		},
		{
			name:  "foo/#/bar/+",
			args:  args{topicName: "foo/#/bar/+"},
			regex: `^foo/(.*)/bar((\/|)[^\/]*)$`,
			tryMatches: map[string]bool{
				"helloworld":          false,
				"foo/":                false,
				"foo/bar":             false,
				"foo/hi/bar":          true,
				"foo/hi/bar/foo":      true,
				"foo/h/i/bar/foo":     true,
				"foo/h/i/0/bar/foo":   true,
				"foo/hi/bar/foo/hi":   false,
				"foo/hi/bar/foo/bar":  true,
				"foo/h/i/bar/foo/bar": true,
				"foo/hi/hi/hi/bar":    true,
				"foo/hi/world":        false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildRegexForTopic(tt.args.topicName)
			if got != tt.regex {
				t.Errorf("buildRegexForTopic(%v) = %v, want %v", tt.args.topicName, got, tt.regex)
				return
			}
			if len(tt.tryMatches) > 0 {
				re := regexp.MustCompile(got)
				for topic, match := range tt.tryMatches {
					if matched := re.MatchString(topic); matched != match {
						t.Errorf("buildRegexForTopic(%v) - match(%v) returned %v but expected %v", tt.args.topicName, topic, matched, match)
					}
				}
			}
		})
	}
}

func Test_mqttPubSub_Publish(t *testing.T) {
	type fields struct {
		logger   logger.Logger
		metadata *mqttMetadata
		ctx      context.Context
	}
	type args struct {
		req *pubsub.PublishRequest
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   assert.ErrorAssertionFunc
		wantedMsg mqttMessage
	}{
		{
			name: "publish request does not contain retain metadata",
			fields: fields{
				logger: logger.NewLogger("mqtt-test"),
				ctx:    context.Background(),
				metadata: &mqttMetadata{
					Retain: true,
				},
			},
			args: args{
				req: &pubsub.PublishRequest{
					Data:        []byte("test"),
					PubsubName:  "mqtt",
					Metadata:    map[string]string{},
					Topic:       "test",
					ContentType: nil,
				},
			},
			wantErr: assert.NoError,
			wantedMsg: mqttMessage{
				data:     []byte("test"),
				retained: true,
				topic:    "test",
				qos:      0,
			},
		},
		{
			name: "publish request contains retain metadata",
			fields: fields{
				logger: logger.NewLogger("mqtt-test"),
				ctx:    context.Background(),
				metadata: &mqttMetadata{
					Retain: true,
				},
			},
			args: args{
				req: &pubsub.PublishRequest{
					Data:        []byte("test"),
					PubsubName:  "mqtt",
					Metadata:    map[string]string{"retain": "false"},
					Topic:       "test",
					ContentType: nil,
				},
			},
			wantErr: assert.NoError,
			wantedMsg: mqttMessage{
				data:     []byte("test"),
				retained: false,
				topic:    "test",
				qos:      0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgCh := make(chan mqttMessage, 1)

			m := &mqttPubSub{
				conn:     newMockedMQTTClient(msgCh),
				logger:   tt.fields.logger,
				metadata: tt.fields.metadata,
			}

			ctx := context.Background()
			tt.wantErr(t, m.Publish(ctx, tt.args.req), fmt.Sprintf("Publish(%v, %v)", ctx, tt.args.req))
			close(msgCh)

			for msg := range msgCh {
				if !reflect.DeepEqual(msg, tt.wantedMsg) {
					t.Errorf("received different message than expected, got = %v, want %v", m, tt.wantedMsg)
				}
			}
		})
	}
}
