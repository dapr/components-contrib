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
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mdata "github.com/dapr/components-contrib/metadata"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		"consumerID": "client",
		amqpURL:      "tcp://fakeUser:fakePassword@fake.mqtt.host:1883",
		anonymous:    "false",
		username:     "default",
		password:     "default",
	}
}

// TestAddressFor verifies how a Dapr topic name is translated into the AMQP
// address used for the sender/receiver link, for each prefix configuration.
func TestAddressFor(t *testing.T) {
	tests := []struct {
		name        string
		topicPrefix string
		queuePrefix string
		topic       string
		want        string
	}{
		// Solace defaults.
		{
			name:        "bare topic uses the topic prefix",
			topicPrefix: defaultTopicAddressPrefix,
			queuePrefix: defaultQueueAddressPrefix,
			topic:       "orders",
			want:        "topic://orders",
		},
		{
			name:        "topic scheme uses the topic prefix",
			topicPrefix: defaultTopicAddressPrefix,
			queuePrefix: defaultQueueAddressPrefix,
			topic:       "topic:orders",
			want:        "topic://orders",
		},
		{
			name:        "queue scheme uses the queue prefix",
			topicPrefix: defaultTopicAddressPrefix,
			queuePrefix: defaultQueueAddressPrefix,
			topic:       "queue:orders",
			want:        "queue://orders",
		},
		{
			name:        "an already prefixed topic address is left untouched",
			topicPrefix: defaultTopicAddressPrefix,
			queuePrefix: defaultQueueAddressPrefix,
			topic:       "topic://orders",
			want:        "topic://orders",
		},
		{
			name:        "an already prefixed queue address is left untouched",
			topicPrefix: defaultTopicAddressPrefix,
			queuePrefix: defaultQueueAddressPrefix,
			topic:       "queue://orders",
			want:        "queue://orders",
		},
		// Brokers that address topics and queues by name, such as ActiveMQ Artemis.
		{
			name:        "empty prefixes pass a bare topic through",
			topicPrefix: "",
			queuePrefix: "",
			topic:       "orders",
			want:        "orders",
		},
		{
			name:        "empty prefixes strip the topic scheme",
			topicPrefix: "",
			queuePrefix: "",
			topic:       "topic:orders",
			want:        "orders",
		},
		{
			name:        "empty prefixes strip the queue scheme",
			topicPrefix: "",
			queuePrefix: "",
			topic:       "queue:orders",
			want:        "orders",
		},
		{
			name:        "empty prefixes strip a fully qualified topic address",
			topicPrefix: "",
			queuePrefix: "",
			topic:       "topic://orders",
			want:        "orders",
		},
		{
			name:        "empty prefixes strip a fully qualified queue address",
			topicPrefix: "",
			queuePrefix: "",
			topic:       "queue://orders",
			want:        "orders",
		},
		{
			name:        "one empty prefix strips only its own fully qualified address",
			topicPrefix: defaultTopicAddressPrefix,
			queuePrefix: "",
			topic:       "queue://orders",
			want:        "orders",
		},
		// Brokers configured with their own routing prefixes.
		{
			name:        "custom topic prefix is applied",
			topicPrefix: "multicast::",
			queuePrefix: "anycast::",
			topic:       "orders",
			want:        "multicast::orders",
		},
		{
			name:        "custom queue prefix is applied",
			topicPrefix: "multicast::",
			queuePrefix: "anycast::",
			topic:       "queue:orders",
			want:        "anycast::orders",
		},
		{
			name:        "custom prefixes replace a fully qualified topic address",
			topicPrefix: "multicast::",
			queuePrefix: "anycast::",
			topic:       "topic://orders",
			want:        "multicast::orders",
		},
		{
			name:        "custom prefixes replace a fully qualified queue address",
			topicPrefix: "multicast::",
			queuePrefix: "anycast::",
			topic:       "queue://orders",
			want:        "anycast::orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &metadata{TopicAddressPrefix: tt.topicPrefix, QueueAddressPrefix: tt.queuePrefix}
			assert.Equal(t, tt.want, m.addressFor(tt.topic))
		})
	}
}

// TestNewPubsubMessage verifies the topic and the payload a received AMQP
// message is delivered with.
func TestNewPubsubMessage(t *testing.T) {
	t.Run("message is delivered under the subscribed topic", func(t *testing.T) {
		msg := newPubsubMessage("orders", amqp.NewMessage([]byte("hello")))

		assert.Equal(t, "orders", msg.Topic)
		assert.Equal(t, []byte("hello"), msg.Data)
	})

	t.Run("the value field is used when the message carries no data", func(t *testing.T) {
		msg := newPubsubMessage("orders", &amqp.Message{Value: "hello"})

		assert.Equal(t, "orders", msg.Topic)
		assert.Equal(t, []byte("hello"), msg.Data)
	})
}

// TestPublishErrorClassification verifies that terminal Publish error paths
// reachable without a live broker are classified as codes.FailedPrecondition.
func TestPublishErrorClassification(t *testing.T) {
	t.Run("closed component is terminal", func(t *testing.T) {
		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		a.closed.Store(true)

		err := a.Publish(context.Background(), &pubsub.PublishRequest{Topic: "some-topic"})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("empty topic is terminal", func(t *testing.T) {
		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)

		err := a.Publish(context.Background(), &pubsub.PublishRequest{Topic: ""})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("a topic that maps to an empty address is terminal", func(t *testing.T) {
		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		// Both prefixes empty, so the scheme alone maps to an empty address.
		a.metadata = &metadata{}

		err := a.Publish(context.Background(), &pubsub.PublishRequest{Topic: topicScheme})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty AMQP address")

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})
}

// TestSubscribeEmptyAddress verifies that a subscription whose topic maps to an
// empty AMQP address is rejected instead of attaching a link to the anonymous
// relay.
func TestSubscribeEmptyAddress(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	// Both prefixes empty, so the scheme alone maps to an empty address.
	a.metadata = &metadata{}

	err := a.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: queueScheme}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty AMQP address")
}

// TestInitUnreachableBroker verifies that a broker which cannot be reached is
// reported as an error from Init. A failed dial used to be passed to
// logger.Fatal, which terminated the process, so this case could not be
// exercised at all before.
func TestInitUnreachableBroker(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := a.Init(ctx, pubsub.Metadata{Base: mdata.Base{Properties: map[string]string{
		// Port 1 is reserved, so nothing is listening on it.
		amqpURL:   "amqp://127.0.0.1:1",
		anonymous: "true",
	}}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "dialing AMQP server")
}

// TestCloseWithoutSession verifies that a component whose Init never
// established a session can still be closed.
func TestCloseWithoutSession(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test"))

	require.NoError(t, a.Close())
}

func TestParseMetadata(t *testing.T) {
	log := logger.NewLogger("test")
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}

		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		assert.Equal(t, fakeProperties[amqpURL], m.URL)
	})

	t.Run("address prefixes default to the Solace convention", func(t *testing.T) {
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: getFakeProperties()}}

		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		assert.Equal(t, defaultTopicAddressPrefix, m.TopicAddressPrefix)
		assert.Equal(t, defaultQueueAddressPrefix, m.QueueAddressPrefix)
		assert.Equal(t, "topic://orders", m.addressFor("orders"))
	})

	t.Run("address prefixes are overridden", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties["topicAddressPrefix"] = "multicast::"
		fakeProperties["queueAddressPrefix"] = "anycast::"
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}

		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		assert.Equal(t, "multicast::", m.TopicAddressPrefix)
		assert.Equal(t, "anycast::", m.QueueAddressPrefix)
	})

	t.Run("address prefixes are disabled when set to an empty value", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties["topicAddressPrefix"] = ""
		fakeProperties["queueAddressPrefix"] = ""
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}

		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		assert.Empty(t, m.TopicAddressPrefix)
		assert.Empty(t, m.QueueAddressPrefix)
		assert.Equal(t, "orders", m.addressFor("orders"))
	})

	t.Run("url is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[amqpURL] = ""

		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.EqualError(t, err, errors.New(errorMsgPrefix+" missing url").Error())
		assert.Equal(t, fakeProperties[amqpURL], m.URL)
	})

	t.Run("invalid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpCACert] = "randomNonPEMBlockCA"
		_, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid caCert")
	})

	t.Run("valid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpCACert] = "-----BEGIN CERTIFICATE-----\nMIICyDCCAbACCQDb8BtgvbqW5jANBgkqhkiG9w0BAQsFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1MzU4WhcNMjUw\nODEyMDY1MzU4WjAmMQswCQYDVQQGEwJJTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0\nQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDEXte1GBxFJaygsEnK\nHV2AxazZW6Vppv+i50AuURHcaGo0i8G5CTfHzSKrYtTFfBskUspl+2N8GPV5c8Eb\ng+PP6YFn1wiHVz+wRSk3BD35DcGOT2o4XsJw5tiAzJkbpAOYCYl7KAM+BtOf41uC\nd6TdqmawhRGtv1ND2WtyJOT6A3KcUfjhL4TFEhWoljPJVay4TQoJcZMAImD/Xcxw\n6urv6wmUJby3/RJ3I46ZNH3zxEw5vSq1TuzuXxQmfPJG0ZPKJtQZ2nkZ3PNZe4bd\nNUa83YgQap7nBhYdYMMsQyLES2qy3mPcemBVoBWRGODel4PMEcsQiOhAyloAF2d3\nhd+LAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAK13X5JYBy78vHYoP0Oq9fe5XBbL\nuRM8YLnet9b/bXTGG4SnCCOGqWz99swYK7SVyR5l2h8SAoLzeNV61PtaZ6fHrbar\noxSL7BoRXOhMH6LQATadyvwlJ71uqlagqya7soaPK09TtfzeebLT0QkRCWT9b9lQ\nDBvBVCaFidynJL1ts21m5yUdIY4JSu4sGZGb4FRGFdBv/hD3wH8LAkOppsSv3C/Q\nkfkDDSQzYbdMoBuXmafvi3He7Rv+e6Tj9or1rrWdx0MIKlZPzz4DOe5Rh112uRB9\n7xPHJt16c+Ya3DKpchwwdNcki0vFchlpV96HK8sMCoY9kBzPhkEQLdiBGv4=\n-----END CERTIFICATE-----\n"
		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		block, _ := pem.Decode([]byte(m.CaCert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse ca certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprMqttTestCA", cert.Subject.CommonName)
	})

	t.Run("invalid client certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpClientCert] = "randomNonPEMBlockClientCert"
		_, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid clientCert")
	})

	t.Run("valid client certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpClientCert] = "-----BEGIN CERTIFICATE-----\nMIICzDCCAbQCCQDBKDMS3SHsDzANBgkqhkiG9w0BAQUFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1NTE1WhcNMjEw\nODA3MDY1NTE1WjAqMQswCQYDVQQGEwJJTjEbMBkGA1UEAwwSZGFwck1xdHRUZXN0\nQ2xpZW50MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5IDfsGI2pb4W\nt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu++M6yi9ABaBiYChpxbylqIeAn/HT\n3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsMfzO9p1oMpTnRdJCHYinE+oqVced5\nHI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPvui8WzJq9TMkEhEME+5hs3VKyKZr2\nqjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMWwzj2G+Rj8PMcBjoKeCLQL9uQh7f1\nTWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+Ur2b5+7/hoqnTzReJ3XUe1jM3l44f\nl0rOf4hu2QIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAT9yoIeX0LTsvx7/b+8V3a\nkP+j8u97QCc8n5xnMpivcMEk5cfqXX5Llv2EUJ9kBsynrJwT7ujhTJXSA/zb2UdC\nKH8PaSrgIlLwQNZMDofbz6+zPbjStkgne/ZQkTDIxY73sGpJL8LsQVO9p2KjOpdj\nSf9KuJhLzcHolh7ry3ZrkOg+QlMSvseeDRAxNhpkJrGQ6piXoUiEeKKNa0rWTMHx\nIP1Hqj+hh7jgqoQR48NL2jNng7I64HqTl6Mv2fiNfINiw+5xmXTB0QYkGU5NvPBO\naKcCRcGlU7ND89BogQPZsl/P04tAuQqpQWffzT4sEEOyWSVGda4N2Ys3GSQGBv8e\n-----END CERTIFICATE-----\n"
		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		block, _ := pem.Decode([]byte(m.ClientCert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse client certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprMqttTestClient", cert.Subject.CommonName)
	})

	t.Run("invalid client certificate key", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpClientKey] = "randomNonPEMBlockClientKey"
		_, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		assert.Contains(t, err.Error(), "invalid clientKey")
	})

	t.Run("valid client certificate key", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpClientKey] = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA5IDfsGI2pb4Wt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu\n++M6yi9ABaBiYChpxbylqIeAn/HT3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsM\nfzO9p1oMpTnRdJCHYinE+oqVced5HI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPv\nui8WzJq9TMkEhEME+5hs3VKyKZr2qjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMW\nwzj2G+Rj8PMcBjoKeCLQL9uQh7f1TWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+U\nr2b5+7/hoqnTzReJ3XUe1jM3l44fl0rOf4hu2QIDAQABAoIBAQCVMINb4TP20P55\n9IPyqlxjhPT563hijXK+lhMJyiBDPavOOs7qjLikq2bshYPVbm1o2jt6pkXXqAeB\n5t/d20fheQQurYyPfxecNBZuL78duwbcUy28m2aXLlcVRYO4zGhoMgdW4UajoNLV\nT/UIiDONWGyhTHXMHdP+6h9UOmvs3o4b225AuLrw9n6QO5I1Se8lcfOTIqR1fy4O\nGsUWEQPdW0X3Dhgpx7kDIuBTAQzbjD31PCR1U8h2wsCeEe6hPCrsMbo/D019weol\ndi40tbWR1/oNz0+vro2d9YDPJkXN0gmpT51Z4YJoexZBdyzO5z4DMSdn5yczzt6p\nQq8LsXAFAoGBAPYXRbC4OxhtuC+xr8KRkaCCMjtjUWFbFWf6OFgUS9b5uPz9xvdY\nXo7wBP1zp2dS8yFsdIYH5Six4Z5iOuDR4sVixzjabhwedL6bmS1zV5qcCWeASKX1\nURgSkfMmC4Tg3LBgZ9YxySFcVRjikxljkS3eK7Mp7Xmj5afe7qV73TJfAoGBAO20\nTtw2RGe02xnydZmmwf+NpQHOA9S0JsehZA6NRbtPEN/C8bPJIq4VABC5zcH+tfYf\nzndbDlGhuk+qpPA590rG5RSOUjYnQFq7njdSfFyok9dXSZQTjJwFnG2oy0LmgjCe\nROYnbCzD+a+gBKV4xlo2M80OLakQ3zOwPT0xNRnHAoGATLEj/tbrU8mdxP9TDwfe\nom7wyKFDE1wXZ7gLJyfsGqrog69y+lKH5XPXmkUYvpKTQq9SARMkz3HgJkPmpXnD\nelA2Vfl8pza2m1BShF+VxZErPR41hcLV6vKemXAZ1udc33qr4YzSaZskygSSYy8s\nZ2b9p3BBmc8CGzbWmKvpW3ECgYEAn7sFLxdMWj/+5221Nr4HKPn+wrq0ek9gq884\n1Ep8bETSOvrdvolPQ5mbBKJGsLC/h5eR/0Rx18sMzpIF6eOZ2GbU8z474mX36cCf\nrd9A8Gbbid3+9IE6gHGIz2uYwujw3UjNVbdyCpbahvjJhoQlDePUZVu8tRpAUpSA\nYklZvGsCgYBuIlOFTNGMVUnwfzrcS9a/31LSvWTZa8w2QFjsRPMYFezo2l4yWs4D\nPEpeuoJm+Gp6F6ayjoeyOw9mvMBH5hAZr4WjbiU6UodzEHREAsLAzCzcRyIpnDE6\nPW1c3j60r8AHVufkWTA+8B9WoLC5MqcYTV3beMGnNGGqS2PeBom63Q==\n-----END RSA PRIVATE KEY-----\n"
		m, err := parseAMQPMetaData(fakeMetaData, log)

		// assert
		require.NoError(t, err)
		assert.NotNil(t, m.ClientKey, "failed to parse valid client certificate key")
	})
}
