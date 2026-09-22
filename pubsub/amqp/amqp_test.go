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
	"fmt"
	"math"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mdata "github.com/dapr/components-contrib/metadata"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		"consumerID": "client",
		amqpURL:      "amqps://fakeUser:fakePassword@fake.amqp.host:5671",
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
			topicPrefix: solaceTopicAddressPrefix,
			queuePrefix: solaceQueueAddressPrefix,
			topic:       "orders",
			want:        "topic://orders",
		},
		{
			name:        "topic scheme uses the topic prefix",
			topicPrefix: solaceTopicAddressPrefix,
			queuePrefix: solaceQueueAddressPrefix,
			topic:       "topic:orders",
			want:        "topic://orders",
		},
		{
			name:        "queue scheme uses the queue prefix",
			topicPrefix: solaceTopicAddressPrefix,
			queuePrefix: solaceQueueAddressPrefix,
			topic:       "queue:orders",
			want:        "queue://orders",
		},
		{
			name:        "an already prefixed topic address is left untouched",
			topicPrefix: solaceTopicAddressPrefix,
			queuePrefix: solaceQueueAddressPrefix,
			topic:       "topic://orders",
			want:        "topic://orders",
		},
		{
			name:        "an already prefixed queue address is left untouched",
			topicPrefix: solaceTopicAddressPrefix,
			queuePrefix: solaceQueueAddressPrefix,
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
			topicPrefix: solaceTopicAddressPrefix,
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
		a := NewSolaceAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		a.closed.Store(true)

		err := a.Publish(context.Background(), &pubsub.PublishRequest{Topic: "some-topic"})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("empty topic is terminal", func(t *testing.T) {
		a := NewSolaceAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)

		err := a.Publish(context.Background(), &pubsub.PublishRequest{Topic: ""})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("a topic that maps to an empty address is terminal", func(t *testing.T) {
		a := NewSolaceAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
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
	a := NewSolaceAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
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
	a := NewSolaceAMQPPubsub(logger.NewLogger("test"))

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
	a := NewSolaceAMQPPubsub(logger.NewLogger("test"))

	require.NoError(t, a.Close())
}

func TestParseMetadata(t *testing.T) {
	log := logger.NewLogger("test")
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}

		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

		// assert
		require.NoError(t, err)
		assert.Equal(t, fakeProperties[amqpURL], m.URL)
	})

	t.Run("address prefixes default to the Solace convention", func(t *testing.T) {
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: getFakeProperties()}}

		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

		// assert
		require.NoError(t, err)
		assert.Equal(t, solaceTopicAddressPrefix, m.TopicAddressPrefix)
		assert.Equal(t, solaceQueueAddressPrefix, m.QueueAddressPrefix)
		assert.Equal(t, "topic://orders", m.addressFor("orders"))
	})

	t.Run("address prefixes are overridden", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties["topicAddressPrefix"] = "multicast::"
		fakeProperties["queueAddressPrefix"] = "anycast::"
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}

		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

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

		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

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

		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

		// assert
		require.EqualError(t, err, errors.New(errorMsgPrefix+" missing url").Error())
		assert.Equal(t, fakeProperties[amqpURL], m.URL)
	})

	t.Run("invalid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpCACert] = "randomNonPEMBlockCA"
		_, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

		// assert
		assert.Contains(t, err.Error(), "invalid caCert")
	})

	t.Run("valid ca certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpCACert] = "-----BEGIN CERTIFICATE-----\nMIICyDCCAbACCQDb8BtgvbqW5jANBgkqhkiG9w0BAQsFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1MzU4WhcNMjUw\nODEyMDY1MzU4WjAmMQswCQYDVQQGEwJJTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0\nQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDEXte1GBxFJaygsEnK\nHV2AxazZW6Vppv+i50AuURHcaGo0i8G5CTfHzSKrYtTFfBskUspl+2N8GPV5c8Eb\ng+PP6YFn1wiHVz+wRSk3BD35DcGOT2o4XsJw5tiAzJkbpAOYCYl7KAM+BtOf41uC\nd6TdqmawhRGtv1ND2WtyJOT6A3KcUfjhL4TFEhWoljPJVay4TQoJcZMAImD/Xcxw\n6urv6wmUJby3/RJ3I46ZNH3zxEw5vSq1TuzuXxQmfPJG0ZPKJtQZ2nkZ3PNZe4bd\nNUa83YgQap7nBhYdYMMsQyLES2qy3mPcemBVoBWRGODel4PMEcsQiOhAyloAF2d3\nhd+LAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAK13X5JYBy78vHYoP0Oq9fe5XBbL\nuRM8YLnet9b/bXTGG4SnCCOGqWz99swYK7SVyR5l2h8SAoLzeNV61PtaZ6fHrbar\noxSL7BoRXOhMH6LQATadyvwlJ71uqlagqya7soaPK09TtfzeebLT0QkRCWT9b9lQ\nDBvBVCaFidynJL1ts21m5yUdIY4JSu4sGZGb4FRGFdBv/hD3wH8LAkOppsSv3C/Q\nkfkDDSQzYbdMoBuXmafvi3He7Rv+e6Tj9or1rrWdx0MIKlZPzz4DOe5Rh112uRB9\n7xPHJt16c+Ya3DKpchwwdNcki0vFchlpV96HK8sMCoY9kBzPhkEQLdiBGv4=\n-----END CERTIFICATE-----\n"
		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

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
		_, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

		// assert
		assert.Contains(t, err.Error(), "invalid clientCert")
	})

	t.Run("valid client certificate", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpClientCert] = "-----BEGIN CERTIFICATE-----\nMIICzDCCAbQCCQDBKDMS3SHsDzANBgkqhkiG9w0BAQUFADAmMQswCQYDVQQGEwJJ\nTjEXMBUGA1UEAwwOZGFwck1xdHRUZXN0Q0EwHhcNMjAwODEyMDY1NTE1WhcNMjEw\nODA3MDY1NTE1WjAqMQswCQYDVQQGEwJJTjEbMBkGA1UEAwwSZGFwck1xdHRUZXN0\nQ2xpZW50MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5IDfsGI2pb4W\nt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu++M6yi9ABaBiYChpxbylqIeAn/HT\n3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsMfzO9p1oMpTnRdJCHYinE+oqVced5\nHI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPvui8WzJq9TMkEhEME+5hs3VKyKZr2\nqjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMWwzj2G+Rj8PMcBjoKeCLQL9uQh7f1\nTWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+Ur2b5+7/hoqnTzReJ3XUe1jM3l44f\nl0rOf4hu2QIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAT9yoIeX0LTsvx7/b+8V3a\nkP+j8u97QCc8n5xnMpivcMEk5cfqXX5Llv2EUJ9kBsynrJwT7ujhTJXSA/zb2UdC\nKH8PaSrgIlLwQNZMDofbz6+zPbjStkgne/ZQkTDIxY73sGpJL8LsQVO9p2KjOpdj\nSf9KuJhLzcHolh7ry3ZrkOg+QlMSvseeDRAxNhpkJrGQ6piXoUiEeKKNa0rWTMHx\nIP1Hqj+hh7jgqoQR48NL2jNng7I64HqTl6Mv2fiNfINiw+5xmXTB0QYkGU5NvPBO\naKcCRcGlU7ND89BogQPZsl/P04tAuQqpQWffzT4sEEOyWSVGda4N2Ys3GSQGBv8e\n-----END CERTIFICATE-----\n"
		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

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
		_, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

		// assert
		assert.Contains(t, err.Error(), "invalid clientKey")
	})

	t.Run("valid client certificate key", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeMetaData := pubsub.Metadata{Base: mdata.Base{Properties: fakeProperties}}
		fakeMetaData.Properties[amqpClientKey] = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA5IDfsGI2pb4Wt3CjckrKuNeTrgmla3sXxSI5wfDgLGd/XkNu\n++M6yi9ABaBiYChpxbylqIeAn/HT3r/nhcb+bldMtEkU9tODHy/QDhvN2UGFjRsM\nfzO9p1oMpTnRdJCHYinE+oqVced5HI+UEofAU+1eiIXqJGKrdfn4gvaHst4QfVPv\nui8WzJq9TMkEhEME+5hs3VKyKZr2qjIxzr7nLVod3DBf482VjxRI06Ip3fPvNuMW\nwzj2G+Rj8PMcBjoKeCLQL9uQh7f1TWHuACqNIrmFEUQWdGETnRjHWIvw0NEL40+U\nr2b5+7/hoqnTzReJ3XUe1jM3l44fl0rOf4hu2QIDAQABAoIBAQCVMINb4TP20P55\n9IPyqlxjhPT563hijXK+lhMJyiBDPavOOs7qjLikq2bshYPVbm1o2jt6pkXXqAeB\n5t/d20fheQQurYyPfxecNBZuL78duwbcUy28m2aXLlcVRYO4zGhoMgdW4UajoNLV\nT/UIiDONWGyhTHXMHdP+6h9UOmvs3o4b225AuLrw9n6QO5I1Se8lcfOTIqR1fy4O\nGsUWEQPdW0X3Dhgpx7kDIuBTAQzbjD31PCR1U8h2wsCeEe6hPCrsMbo/D019weol\ndi40tbWR1/oNz0+vro2d9YDPJkXN0gmpT51Z4YJoexZBdyzO5z4DMSdn5yczzt6p\nQq8LsXAFAoGBAPYXRbC4OxhtuC+xr8KRkaCCMjtjUWFbFWf6OFgUS9b5uPz9xvdY\nXo7wBP1zp2dS8yFsdIYH5Six4Z5iOuDR4sVixzjabhwedL6bmS1zV5qcCWeASKX1\nURgSkfMmC4Tg3LBgZ9YxySFcVRjikxljkS3eK7Mp7Xmj5afe7qV73TJfAoGBAO20\nTtw2RGe02xnydZmmwf+NpQHOA9S0JsehZA6NRbtPEN/C8bPJIq4VABC5zcH+tfYf\nzndbDlGhuk+qpPA590rG5RSOUjYnQFq7njdSfFyok9dXSZQTjJwFnG2oy0LmgjCe\nROYnbCzD+a+gBKV4xlo2M80OLakQ3zOwPT0xNRnHAoGATLEj/tbrU8mdxP9TDwfe\nom7wyKFDE1wXZ7gLJyfsGqrog69y+lKH5XPXmkUYvpKTQq9SARMkz3HgJkPmpXnD\nelA2Vfl8pza2m1BShF+VxZErPR41hcLV6vKemXAZ1udc33qr4YzSaZskygSSYy8s\nZ2b9p3BBmc8CGzbWmKvpW3ECgYEAn7sFLxdMWj/+5221Nr4HKPn+wrq0ek9gq884\n1Ep8bETSOvrdvolPQ5mbBKJGsLC/h5eR/0Rx18sMzpIF6eOZ2GbU8z474mX36cCf\nrd9A8Gbbid3+9IE6gHGIz2uYwujw3UjNVbdyCpbahvjJhoQlDePUZVu8tRpAUpSA\nYklZvGsCgYBuIlOFTNGMVUnwfzrcS9a/31LSvWTZa8w2QFjsRPMYFezo2l4yWs4D\nPEpeuoJm+Gp6F6ayjoeyOw9mvMBH5hAZr4WjbiU6UodzEHREAsLAzCzcRyIpnDE6\nPW1c3j60r8AHVufkWTA+8B9WoLC5MqcYTV3beMGnNGGqS2PeBom63Q==\n-----END RSA PRIVATE KEY-----\n"
		m, err := parseAMQPMetaData(fakeMetaData, log, solaceTopicAddressPrefix, solaceQueueAddressPrefix)

		// assert
		require.NoError(t, err)
		assert.NotNil(t, m.ClientKey, "failed to parse valid client certificate key")
	})
}

// TestConstructorDefaults pins the addressing defaults of the two registered
// component types. They cannot be derived at runtime: pubsub.Metadata carries
// the component instance name and its properties, never the component type, so
// the constructor is the only place the distinction can be made.
func TestConstructorDefaults(t *testing.T) {
	tests := []struct {
		name            string
		newPubSub       func(logger.Logger) pubsub.PubSub
		wantTopicPrefix string
		wantQueuePrefix string
	}{
		{
			name:            "pubsub.amqp addresses destinations by name",
			newPubSub:       NewAMQPPubsub,
			wantTopicPrefix: "",
			wantQueuePrefix: "",
		},
		{
			name:            "pubsub.solace.amqp keeps the Solace convention",
			newPubSub:       NewSolaceAMQPPubsub,
			wantTopicPrefix: "topic://",
			wantQueuePrefix: "queue://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.newPubSub(logger.NewLogger("test")).(*amqpPubSub)
			assert.Equal(t, tt.wantTopicPrefix, a.defaultTopicPrefix)
			assert.Equal(t, tt.wantQueuePrefix, a.defaultQueuePrefix)

			m, err := parseAMQPMetaData(pubsub.Metadata{Base: mdata.Base{
				Properties: map[string]string{
					"url":       "amqp://localhost:5672",
					"anonymous": "true",
				},
			}}, logger.NewLogger("test"), a.defaultTopicPrefix, a.defaultQueuePrefix)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTopicPrefix, m.TopicAddressPrefix)
			assert.Equal(t, tt.wantQueuePrefix, m.QueueAddressPrefix)
		})
	}
}

// TestAddressForGenericDefaults documents how a pubsub.amqp component addresses
// destinations out of the box, and records one consequence that matters to
// users: with no prefixes configured, the queue: scheme cannot select a
// different address from a bare topic name. A broker that distinguishes the two
// needs its own prefixes configured, for example an ActiveMQ Artemis acceptor's
// anycastPrefix and multicastPrefix.
func TestAddressForGenericDefaults(t *testing.T) {
	m := &metadata{
		TopicAddressPrefix: genericAddressPrefix,
		QueueAddressPrefix: genericAddressPrefix,
	}

	assert.Equal(t, "orders", m.addressFor("orders"))
	assert.Equal(t, "orders", m.addressFor("topic:orders"))
	assert.Equal(t, "orders", m.addressFor("queue:orders"),
		"with no prefixes configured the queue scheme collapses onto the bare topic name")
}

// TestAddressForBrokerPrefixes covers the configuration an ActiveMQ Artemis
// user applies to keep ANYCAST and MULTICAST selection working.
func TestAddressForBrokerPrefixes(t *testing.T) {
	m := &metadata{
		TopicAddressPrefix: "multicast://",
		QueueAddressPrefix: "anycast://",
	}

	assert.Equal(t, "multicast://orders", m.addressFor("orders"))
	assert.Equal(t, "multicast://orders", m.addressFor("topic:orders"))
	assert.Equal(t, "anycast://orders", m.addressFor("queue:orders"))
	assert.Equal(t, "anycast://orders", m.addressFor("anycast://orders"),
		"an address that already carries a configured prefix is used as-is")
}

// TestFeaturesDoesNotClaimWildcards pins the capability list. Wildcard syntax
// is broker-specific and pubsub.Feature carries no syntax dimension, so a
// component pointed at any AMQP 1.0 broker must not declare it. metadata.yaml
// declares ttl only, and the two have to agree.
func TestFeaturesDoesNotClaimWildcards(t *testing.T) {
	features := NewAMQPPubsub(logger.NewLogger("test")).Features()

	assert.Equal(t, []pubsub.Feature{pubsub.FeatureMessageTTL}, features)
	assert.NotContains(t, features, pubsub.FeatureSubscribeWildcards)
}

// TestRenewSessionIsSingleFlight covers the guard that stops a broker restart
// costing one dial per in-flight operation. A caller handing back a session
// that is no longer current gets the replacement, and no dial is attempted.
func TestRenewSessionIsSingleFlight(t *testing.T) {
	newComponent := func() *amqpPubSub {
		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		// Dialling this address fails fast, so any test that reaches a dial
		// reports an error rather than hanging.
		a.metadata = &metadata{URL: "amqp://127.0.0.1:1"}

		return a
	}

	t.Run("a superseded caller gets the live session and does not dial", func(t *testing.T) {
		a := newComponent()
		current := &amqp.Session{}
		a.session = current

		got, err := a.renewSession(t.Context(), &amqp.Session{})
		require.NoError(t, err)
		assert.Same(t, current, got)
		assert.Same(t, current, a.currentSession())
	})

	// This is the case the single-flight guard originally got wrong. A failed
	// reconnect leaves a.session nil, which is also "different from stale". The
	// guard must not report that as success, or the caller dereferences nil.
	t.Run("a caller racing a failed reconnect never gets a nil session and a nil error", func(t *testing.T) {
		a := newComponent()
		a.session = nil

		got, err := a.renewSession(t.Context(), &amqp.Session{})

		require.Error(t, err, "with no live session the caller must dial, and this dial fails")
		assert.Nil(t, got)
		if err == nil && got == nil {
			t.Fatal("renewSession returned a nil session with a nil error")
		}
	})
}

// TestRenewSessionRefusesWhenClosed stops a reconnect racing a shutdown and
// re-opening a connection that nothing will ever close.
func TestRenewSessionRefusesWhenClosed(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	a.metadata = &metadata{URL: "amqp://127.0.0.1:1"}
	a.closed.Store(true)

	_, err := a.renewSession(t.Context(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// TestPublishRetriesOutsideTheConnectionLock guards the throughput fix. Publish
// used to hold an exclusive lock for its whole body, including the sleep
// between retries, which serialised every publisher on the component and held
// up reconnects for the duration.
//
// The probe takes the lock for writing. A reader would get through whether
// Publish held nothing or held the lock shared, so only a writer tells the two
// apart. Publish does take the write lock to dial, but a dial to a closed local
// port is refused within milliseconds, so a writer that waits as long as a
// retry interval is waiting behind the retry loop itself.
func TestPublishRetriesOutsideTheConnectionLock(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	a.metadata = &metadata{URL: "amqp://127.0.0.1:1"}

	// Long enough for at least one retry sleep, short enough to keep the test
	// quick. Publish returns when the context ends.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		_ = a.Publish(ctx, &pubsub.PublishRequest{Topic: "orders", Data: []byte("hello")})
	}()

	// Probe for the whole life of the publish, so that at least one attempt
	// overlaps whatever Publish holds the lock across.
	deadline := time.After(60 * time.Second)
	var longest time.Duration
	for {
		select {
		case <-publishDone:
			if longest >= time.Second {
				t.Fatalf("a writer waited %v for the connection lock, so Publish holds it across its retries", longest)
			}

			return
		case <-deadline:
			t.Fatal("Publish did not return")
		default:
		}

		start := time.Now()
		a.connMu.Lock()
		waited := time.Since(start)
		a.connMu.Unlock()

		if waited > longest {
			longest = waited
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// TestShouldRenew pins the error classifier that keeps one refused link from
// tearing down the shared connection. The certification suite only exercises
// whole-broker and network failures, so a change in the shape of go-amqp's
// errors would otherwise reintroduce the shared-connection failure silently.
func TestShouldRenew(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"a dead connection", t.Context(), &amqp.ConnError{}, true},
		{"a dead session", t.Context(), &amqp.SessionError{}, true},
		{"a dead connection inside a wrapped error", t.Context(), fmt.Errorf("attach: %w", &amqp.ConnError{}), true},
		{"a connection-level condition", t.Context(), &amqp.Error{Condition: amqp.ErrCondConnectionForced}, true},
		{"a session-level condition", t.Context(), &amqp.Error{Condition: amqp.ErrCondWindowViolation}, true},
		{"a refused link, address not found", t.Context(), &amqp.Error{Condition: amqp.ErrCondNotFound}, false},
		{"a refused link, unauthorized", t.Context(), &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}, false},
		{"a link-level condition", t.Context(), &amqp.Error{Condition: amqp.ErrCondDetachForced}, false},
		{"a detached link", t.Context(), &amqp.LinkError{}, false},
		{"an error of no known shape", t.Context(), errors.New("boom"), false},
		{"a caller that gave up, whatever the error", cancelled, &amqp.ConnError{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldRenew(tt.ctx, tt.err))
		})
	}
}

// TestInitDoesNotStoreAConnectionAfterClose covers the window between Init's
// dial returning and the connection being stored. Close can run to completion
// inside it and find nothing to close, so a connection stored afterwards would
// leak for the life of the process.
//
// A live *amqp.Conn cannot be built without a broker, so this drives the guard
// with nil handles: it must refuse them and leave the component uninitialised.
func TestInitDoesNotStoreAConnectionAfterClose(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	require.NoError(t, a.Close())

	err := a.adoptConnection(nil, nil, &metadata{URL: "amqp://127.0.0.1:1"}, retry.DefaultConfig())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
	assert.Nil(t, a.currentMetadata(), "a closed component must not become initialised")
	assert.Nil(t, a.currentSession())
}

// TestCloseWithoutInitIsSafe covers Close running after Init failed, when
// neither a connection nor a session was ever established.
func TestCloseWithoutInitIsSafe(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test"))

	require.NoError(t, a.Close())
	require.NoError(t, a.Close(), "Close must be idempotent")
}

// TestAddressForEmptyTopic covers the guard that an empty topic has no address.
//
// Without it, a configured prefix makes the result the bare prefix, which is
// not empty and so passes the emptiness check both callers rely on. The
// component would then open a link on the literal address "topic://".
func TestAddressForEmptyTopic(t *testing.T) {
	tests := []struct {
		name string
		md   *metadata
	}{
		{
			name: "no prefixes",
			md:   &metadata{TopicAddressPrefix: "", QueueAddressPrefix: ""},
		},
		{
			name: "Solace prefixes",
			md:   &metadata{TopicAddressPrefix: solaceTopicAddressPrefix, QueueAddressPrefix: solaceQueueAddressPrefix},
		},
		{
			name: "broker prefixes",
			md:   &metadata{TopicAddressPrefix: "multicast://", QueueAddressPrefix: "anycast://"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Empty(t, tt.md.addressFor(""),
				"an empty topic must have no address, whatever the prefixes are")
		})
	}
}

// TestPublishWithTTLDoesNotPanic covers the one capability this component
// advertises. go-amqp's NewMessage leaves Message.Header nil, and Header is a
// pointer, so writing a TTL to it dereferenced nil and panicked the publishing
// goroutine. Features() and metadata.yaml both declare the capability, so
// nothing else would have caught it.
func TestPublishWithTTLDoesNotPanic(t *testing.T) {
	tests := []struct{ name, ttl string }{
		{name: "valid ttl", ttl: "30"},
		{name: "zero ttl", ttl: "0"},
		{name: "negative ttl", ttl: "-1"},
		{name: "unparseable ttl", ttl: "soon"},
		{name: "ttl above what the header can carry", ttl: "4294968"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
			a.metadata = &metadata{URL: "amqp://127.0.0.1:1"}

			// The message is built before the publish loop, so a cancelled
			// context exercises the TTL path and then returns at once instead
			// of sitting through the retry backoff.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			// The publish fails, because nothing is listening. What matters is
			// that building the message does not panic first.
			require.NotPanics(t, func() {
				_ = a.Publish(ctx, &pubsub.PublishRequest{
					Topic:    "orders",
					Data:     []byte("hello"),
					Metadata: map[string]string{"ttlInSeconds": tt.ttl},
				})
			})
		})
	}
}

// TestMessageTTL pins the bounds on ttlInSeconds. The AMQP 1.0 header carries
// the TTL as an unsigned 32-bit count of milliseconds, and go-amqp truncates a
// longer duration with a plain cast, so a TTL above that has to be refused
// rather than sent and silently shortened.
func TestMessageTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     string
		want    time.Duration
		wantErr bool
	}{
		{name: "absent", ttl: "", want: 0},
		{name: "zero", ttl: "0", want: 0},
		{name: "thirty seconds", ttl: "30", want: 30 * time.Second},
		{name: "the longest the header can carry", ttl: "4294967", want: 4294967 * time.Second},
		{name: "one second above the limit", ttl: "4294968", wantErr: true},
		{name: "large enough to overflow a duration", ttl: "99999999999", wantErr: true},
		{name: "negative", ttl: "-1", wantErr: true},
		{name: "not an integer", ttl: "soon", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := messageTTL(tt.ttl)
			if tt.wantErr {
				require.Error(t, err)
				assert.Zero(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	// The limit is derived from the wire format, so pin the derivation: the
	// longest accepted TTL must fit the uint32 milliseconds go-amqp writes.
	assert.LessOrEqual(t, (time.Duration(maxTTLSeconds) * time.Second).Milliseconds(), int64(math.MaxUint32))
	assert.Greater(t, (time.Duration(maxTTLSeconds+1) * time.Second).Milliseconds(), int64(math.MaxUint32))
}

// TestGenericTypeAddressesByName is the outcome this whole change exists for.
// A pubsub.amqp component must address a destination by its bare name, and a
// pubsub.solace.amqp component must keep the Solace convention, both through
// the public constructors rather than a hand-built metadata struct.
func TestGenericTypeAddressesByName(t *testing.T) {
	props := map[string]string{
		"url":       "amqp://127.0.0.1:1",
		"anonymous": "true",
	}

	tests := []struct {
		name      string
		newPubSub func(logger.Logger) pubsub.PubSub
		topic     string
		want      string
	}{
		{"generic addresses a topic by name", NewAMQPPubsub, "orders", "orders"},
		{"generic addresses a queue by name", NewAMQPPubsub, "queue:orders", "orders"},
		{"solace keeps the topic convention", NewSolaceAMQPPubsub, "orders", "topic://orders"},
		{"solace keeps the queue convention", NewSolaceAMQPPubsub, "queue:orders", "queue://orders"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.newPubSub(logger.NewLogger("test")).(*amqpPubSub)

			md, err := parseAMQPMetaData(
				pubsub.Metadata{Base: mdata.Base{Properties: props}},
				logger.NewLogger("test"),
				a.defaultTopicPrefix,
				a.defaultQueuePrefix,
			)
			require.NoError(t, err)

			assert.Equal(t, tt.want, md.addressFor(tt.topic))
		})
	}
}

// TestTLSMaterialRequiresTLSScheme stops the component reporting success for
// certificates it then discards. createClientOptions only applies TLS to an
// amqps:// url, so accepting caCert on a plaintext url connected in the clear.
func TestTLSMaterialRequiresTLSScheme(t *testing.T) {
	const cert = `-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAJ3sTTWjHa5UMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl
c3RjYTAeFw0yMDA1MDcxNzE5NTdaFw0zMDA1MDUxNzE5NTdaMBExDzANBgNVBAMM
BnRlc3RjYTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAvZ7Ec6nnKVqLnHDN
-----END CERTIFICATE-----`

	_, err := parseAMQPMetaData(pubsub.Metadata{Base: mdata.Base{Properties: map[string]string{
		"url":       "amqp://localhost:5672",
		"anonymous": "true",
		"caCert":    cert,
	}}}, logger.NewLogger("test"), "", "")

	require.Error(t, err, "TLS material on a plaintext url must be rejected, not silently ignored")
	assert.Contains(t, err.Error(), "amqps")
}
