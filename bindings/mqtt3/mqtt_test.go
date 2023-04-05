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
	"errors"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

var log = logger.NewLogger("testlogger")

func getFakeProperties() map[string]string {
	return map[string]string{
		"consumerID":     "client",
		mqttURL:          "tcp://fakeUser:fakePassword@fake.mqtt.host:1883",
		mqttTopic:        "fake/topic",
		mqttQOS:          "1",
		mqttRetain:       "true",
		mqttCleanSession: "false",
	}
}

func TestParseMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := bindings.Metadata{Base: mdata.Base{
			Properties: fakeProperties,
		}}

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.Url)
		assert.Equal(t, byte(1), m.Qos)
		assert.Equal(t, true, m.Retain)
		assert.Equal(t, false, m.CleanSession)
	})

	t.Run("missing topic", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties["topic"] = ""
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "missing topic")
	})

	t.Run("missing consumerID", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties["consumerID"] = ""
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "missing consumerID")
	})

	t.Run("url is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttURL] = ""

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.EqualError(t, err, errors.New("missing url").Error())
		assert.Equal(t, fakeProperties[mqttURL], m.Url)
	})

	t.Run("qos and retain is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttQOS] = ""
		fakeMetaData.Properties[mqttRetain] = ""

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[mqttURL], m.Url)
		assert.Equal(t, byte(1), m.Qos)
		assert.Equal(t, false, m.Retain)
	})

	t.Run("invalid clean session field", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttCleanSession] = "randomString"

		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, m.CleanSession, false)
		assert.Equal(t, fakeProperties[mqttURL], m.Url)
	})

	t.Run("invalid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttCACert] = "randomNonPEMBlockCA"
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid ca certificate")
	})

	t.Run("valid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttCACert] = "-----BEGIN CERTIFICATE-----\nMIICyDCCAbACCQDb8BtgvbqW5jANBgkqhkiG9w0BAQsFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1MzU4WhcNMjUw\nODEyMDY1MzU4WjAmMQswCQYDVQQGEwJJTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0\nQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDEXte1GBxFJaygsEnK\nHV2AxazZW6Vppv+i50AuURHcaGo0i8G5CTfHzSKrYtTFfBskUspl+2N8GPV5c8Eb\ng+PP6YFn1wiHVz+wRSk3BD35DcGOT2o4XsJw5tiAzJkbpAOYCYl7KAM+BtOf41uC\nd6TdqmawhRGtv1ND2WtyJOT6A3KcUfjhL4TFEhWoljPJVay4TQoJcZMAImD/Xcxw\n6urv6wmUJby3/RJ3I46ZNH3zxEw5vSq1TuzuXxQmfPJG0ZPKJtQZ2nkZ3PNZe4bd\nNUa83YgQap7nBhYdYMMsQyLES2qy3mPcemBVoBWRGODel4PMEcsQiOhAyloAF2d3\nhd+LAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAK13X5JYBy78vHYoP0Oq9fe5XBbL\nuRM8YLnet9b/bXTGG4SnCCOGqWz99swYK7SVyR5l2h8SAoLzeNV61PtaZ6fHrbar\noxSL7BoRXOhMH6LQATadyvwlJ71uqlagqya7soaPK09TtfzeebLT0QkRCWT9b9lQ\nDBvBVCaFidynJL1ts21m5yUdIY4JSu4sGZGb4FRGFdBv/hD3wH8LAkOppsSv3C/Q\nkfkDDSQzYbdMoBuXmafvi3He7Rv+e6Tj9or1rrWdx0MIKlZPzz4DOe5Rh112uRB9\n7xPHJt16c+Ya3DKpchwwdNcki0vFchlpV96HK8sMCoY9kBzPhkEQLdiBGv4=\n-----END CERTIFICATE-----\n"
		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		block, _ := pem.Decode([]byte(m.tlsCfg.CaCert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse ca certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprMqttTestCA", cert.Subject.CommonName)
	})

	t.Run("invalid client certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttClientCert] = "randomNonPEMBlockClientCert"
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid client certificate")
	})

	t.Run("valid client certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttClientCert] = "-----BEGIN CERTIFICATE-----\nMIICzDCCAbQCCQDBKDMS3SHsDzANBgkqhkiG9w0BAQUFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1NTE1WhcNMjEw\nODA3MDY1NTE1WjAqMQswCQYDVQQGEwJJTjEbMBkGA1UEAwwSZGFwck1xdHRUZXN0\nQ2xpZW50MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5IDfsGI2pb4W\nt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu++M6yi9ABaBiYChpxbylqIeAn/HT\n3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsMfzO9p1oMpTnRdJCHYinE+oqVced5\nHI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPvui8WzJq9TMkEhEME+5hs3VKyKZr2\nqjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMWwzj2G+Rj8PMcBjoKeCLQL9uQh7f1\nTWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+Ur2b5+7/hoqnTzReJ3XUe1jM3l44f\nl0rOf4hu2QIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAT9yoIeX0LTsvx7/b+8V3a\nkP+j8u97QCc8n5xnMpivcMEk5cfqXX5Llv2EUJ9kBsynrJwT7ujhTJXSA/zb2UdC\nKH8PaSrgIlLwQNZMDofbz6+zPbjStkgne/ZQkTDIxY73sGpJL8LsQVO9p2KjOpdj\nSf9KuJhLzcHolh7ry3ZrkOg+QlMSvseeDRAxNhpkJrGQ6piXoUiEeKKNa0rWTMHx\nIP1Hqj+hh7jgqoQR48NL2jNng7I64HqTl6Mv2fiNfINiw+5xmXTB0QYkGU5NvPBO\naKcCRcGlU7ND89BogQPZsl/P04tAuQqpQWffzT4sEEOyWSVGda4N2Ys3GSQGBv8e\n-----END CERTIFICATE-----\n"
		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		block, _ := pem.Decode([]byte(m.tlsCfg.ClientCert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse client certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprMqttTestClient", cert.Subject.CommonName)
	})

	t.Run("invalid client certificate key", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttClientKey] = "randomNonPEMBlockClientKey"
		_, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid client certificate key")
	})

	t.Run("valid client certificate key", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := bindings.Metadata{Base: mdata.Base{Name: "binging-test", Properties: fakeProperties}}
		fakeMetaData.Properties[mqttClientKey] = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA5IDfsGI2pb4Wt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu\n++M6yi9ABaBiYChpxbylqIeAn/HT3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsM\nfzO9p1oMpTnRdJCHYinE+oqVced5HI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPv\nui8WzJq9TMkEhEME+5hs3VKyKZr2qjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMW\nwzj2G+Rj8PMcBjoKeCLQL9uQh7f1TWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+U\nr2b5+7/hoqnTzReJ3XUe1jM3l44fl0rOf4hu2QIDAQABAoIBAQCVMINb4TP20P55\n9IPyqlxjhPT563hijXK+lhMJyiBDPavOOs7qjLikq2bshYPVbm1o2jt6pkXXqAeB\n5t/d20fheQQurYyPfxecNBZuL78duwbcUy28m2aXLlcVRYO4zGhoMgdW4UajoNLV\nT/UIiDONWGyhTHXMHdP+6h9UOmvs3o4b225AuLrw9n6QO5I1Se8lcfOTIqR1fy4O\nGsUWEQPdW0X3Dhgpx7kDIuBTAQzbjD31PCR1U8h2wsCeEe6hPCrsMbo/D019weol\ndi40tbWR1/oNz0+vro2d9YDPJkXN0gmpT51Z4YJoexZBdyzO5z4DMSdn5yczzt6p\nQq8LsXAFAoGBAPYXRbC4OxhtuC+xr8KRkaCCMjtjUWFbFWf6OFgUS9b5uPz9xvdY\nXo7wBP1zp2dS8yFsdIYH5Six4Z5iOuDR4sVixzjabhwedL6bmS1zV5qcCWeASKX1\nURgSkfMmC4Tg3LBgZ9YxySFcVRjikxljkS3eK7Mp7Xmj5afe7qV73TJfAoGBAO20\nTtw2RGe02xnydZmmwf+NpQHOA9S0JsehZA6NRbtPEN/C8bPJIq4VABC5zcH+tfYf\nzndbDlGhuk+qpPA590rG5RSOUjYnQFq7njdSfFyok9dXSZQTjJwFnG2oy0LmgjCe\nROYnbCzD+a+gBKV4xlo2M80OLakQ3zOwPT0xNRnHAoGATLEj/tbrU8mdxP9TDwfe\nom7wyKFDE1wXZ7gLJyfsGqrog69y+lKH5XPXmkUYvpKTQq9SARMkz3HgJkPmpXnD\nelA2Vfl8pza2m1BShF+VxZErPR41hcLV6vKemXAZ1udc33qr4YzSaZskygSSYy8s\nZ2b9p3BBmc8CGzbWmKvpW3ECgYEAn7sFLxdMWj/+5221Nr4HKPn+wrq0ek9gq884\n1Ep8bETSOvrdvolPQ5mbBKJGsLC/h5eR/0Rx18sMzpIF6eOZ2GbU8z474mX36cCf\nrd9A8Gbbid3+9IE6gHGIz2uYwujw3UjNVbdyCpbahvjJhoQlDePUZVu8tRpAUpSA\nYklZvGsCgYBuIlOFTNGMVUnwfzrcS9a/31LSvWTZa8w2QFjsRPMYFezo2l4yWs4D\nPEpeuoJm+Gp6F6ayjoeyOw9mvMBH5hAZr4WjbiU6UodzEHREAsLAzCzcRyIpnDE6\nPW1c3j60r8AHVufkWTA+8B9WoLC5MqcYTV3beMGnNGGqS2PeBom63Q==\n-----END RSA PRIVATE KEY-----\n"
		m, err := parseMQTTMetaData(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, m.tlsCfg.ClientKey, "failed to parse valid client certificate key")
	})

	t.Run("Response returns the topic that the subscribed data is from.", func(t *testing.T) {
		const msg = "hello world"

		payload := []byte(msg)
		topic := "/topic/where/the/data/is/from"

		logger := logger.NewLogger("test")
		m := NewMQTT(logger).(*MQTT)
		m.backOff = backoff.NewConstantBackOff(5 * time.Second)
		m.readHandler = func(ctx context.Context, r *bindings.ReadResponse) ([]byte, error) {
			assert.Equal(t, payload, r.Data)
			metadata := r.Metadata
			responseTopic, ok := metadata[mqttTopic]
			assert.True(t, ok)
			assert.Equal(t, topic, responseTopic)
			return r.Data, nil
		}

		m.handleMessage()(nil, &mqttMockMessage{
			topic:   topic,
			payload: payload,
		})
	})
}

type mqttMockMessage struct {
	duplicate         bool
	qos               byte
	retained          bool
	topic             string
	mqttMockMessageID uint16
	payload           []byte
	ackCalled         bool
	noautoack         bool
}

func (m *mqttMockMessage) Duplicate() bool {
	return m.duplicate
}

func (m *mqttMockMessage) Qos() byte {
	return m.qos
}

func (m *mqttMockMessage) Retained() bool {
	return m.retained
}

func (m *mqttMockMessage) Topic() string {
	return m.topic
}

func (m *mqttMockMessage) MessageID() uint16 {
	return m.mqttMockMessageID
}

func (m *mqttMockMessage) Payload() []byte {
	return m.payload
}

func (m *mqttMockMessage) Ack() {
	m.ackCalled = true
}

func (m *mqttMockMessage) NoAutoAck() bool {
	return m.noautoack
}

func (m *mqttMockMessage) AutoAckOff() {
	m.noautoack = true
}
