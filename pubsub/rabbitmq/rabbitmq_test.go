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

package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func newBroker() *rabbitMQInMemoryBroker {
	return &rabbitMQInMemoryBroker{
		buffer: make(chan amqp.Delivery, 2),
	}
}

func newRabbitMQTest(broker *rabbitMQInMemoryBroker) pubsub.PubSub {
	return &rabbitMQ{
		declaredExchanges: make(map[string]bool),
		logger:            logger.NewLogger("test"),
		connectionDial: func(protocol, uri string, tlsCfg *tls.Config, externalSasl bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
			broker.connectCount.Add(1)
			return broker, broker, nil
		},
		closeCh: make(chan struct{}),
	}
}

func TestNoConsumer(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataHostnameKey: "anyhost",
		},
	}}
	err := pubsubRabbitMQ.Init(context.Background(), metadata)
	assert.NoError(t, err)
	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{}, nil)
	assert.Contains(t, err.Error(), "consumerID is required for subscriptions")
}

func TestPublishAndSubscribeWithPriorityQueue(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataHostnameKey:   "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}}
	err := pubsubRabbitMQ.Init(context.Background(), metadata)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), broker.connectCount.Load())
	assert.Equal(t, int32(0), broker.closeCount.Load())

	topic := "mytopic"

	messageCount := 0
	lastMessage := ""
	processed := make(chan bool)
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		messageCount++
		lastMessage = string(msg.Data)
		processed <- true

		return nil
	}

	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: topic, Metadata: map[string]string{metadataMaxPriority: "5"}}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("hello world"), Metadata: map[string]string{metadataMaxPriority: "5"}})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: topic, Metadata: map[string]string{metadataQueueType: "classic"}}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("hey there"), Metadata: map[string]string{metadataQueueType: "classic"}})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 2, messageCount)
	assert.Equal(t, "hey there", lastMessage)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("foo bar")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 3, messageCount)
	assert.Equal(t, "foo bar", lastMessage)
}

func TestConcurrencyMode(t *testing.T) {
	t.Run("parallel", func(t *testing.T) {
		broker := newBroker()
		pubsubRabbitMQ := newRabbitMQTest(broker)
		metadata := pubsub.Metadata{Base: mdata.Base{
			Properties: map[string]string{
				metadataHostnameKey:   "anyhost",
				metadataConsumerIDKey: "consumer",
				pubsub.ConcurrencyKey: string(pubsub.Parallel),
			},
		}}
		err := pubsubRabbitMQ.Init(context.Background(), metadata)
		assert.Nil(t, err)
		assert.Equal(t, pubsub.Parallel, pubsubRabbitMQ.(*rabbitMQ).metadata.Concurrency)
	})

	t.Run("single", func(t *testing.T) {
		broker := newBroker()
		pubsubRabbitMQ := newRabbitMQTest(broker)
		metadata := pubsub.Metadata{Base: mdata.Base{
			Properties: map[string]string{
				metadataHostnameKey:   "anyhost",
				metadataConsumerIDKey: "consumer",
				pubsub.ConcurrencyKey: string(pubsub.Single),
			},
		}}
		err := pubsubRabbitMQ.Init(context.Background(), metadata)
		assert.Nil(t, err)
		assert.Equal(t, pubsub.Single, pubsubRabbitMQ.(*rabbitMQ).metadata.Concurrency)
	})

	t.Run("default", func(t *testing.T) {
		broker := newBroker()
		pubsubRabbitMQ := newRabbitMQTest(broker)
		metadata := pubsub.Metadata{Base: mdata.Base{
			Properties: map[string]string{
				metadataHostnameKey:   "anyhost",
				metadataConsumerIDKey: "consumer",
			},
		}}
		err := pubsubRabbitMQ.Init(context.Background(), metadata)
		assert.Nil(t, err)
		assert.Equal(t, pubsub.Parallel, pubsubRabbitMQ.(*rabbitMQ).metadata.Concurrency)
	})
}

func TestPublishAndSubscribe(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataHostnameKey:   "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}}
	err := pubsubRabbitMQ.Init(context.Background(), metadata)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), broker.connectCount.Load())
	assert.Equal(t, int32(0), broker.closeCount.Load())

	topic := "mytopic"

	messageCount := 0
	lastMessage := ""
	processed := make(chan bool)
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		messageCount++
		lastMessage = string(msg.Data)
		processed <- true

		return nil
	}

	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("foo bar")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 2, messageCount)
	assert.Equal(t, "foo bar", lastMessage)
}

func TestPublishReconnect(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataHostnameKey:   "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}}
	err := pubsubRabbitMQ.Init(context.Background(), metadata)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), broker.connectCount.Load())
	assert.Equal(t, int32(0), broker.closeCount.Load())

	topic := "othertopic"

	messageCount := 0
	lastMessage := ""
	processed := make(chan bool)
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		messageCount++
		lastMessage = string(msg.Data)
		processed <- true

		return nil
	}

	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte(errorChannelConnection)})
	assert.NotNil(t, err)
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)
	// Check that reconnection happened
	assert.Equal(t, int32(3), broker.connectCount.Load()) // three counts - one initial connection plus 2 reconnect attempts
	assert.Equal(t, int32(4), broker.closeCount.Load())   // four counts - one for connection, one for channel , times 2 reconnect attempts

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("foo bar")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 2, messageCount)
	assert.Equal(t, "foo bar", lastMessage)
}

func TestPublishReconnectAfterClose(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataHostnameKey:   "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}}
	err := pubsubRabbitMQ.Init(context.Background(), metadata)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), broker.connectCount.Load())
	assert.Equal(t, int32(0), broker.closeCount.Load())

	topic := "mytopic2"

	messageCount := 0
	lastMessage := ""
	processed := make(chan bool)
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		messageCount++
		lastMessage = string(msg.Data)
		processed <- true

		return nil
	}

	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	// Close PubSub
	err = pubsubRabbitMQ.Close()
	assert.Nil(t, err)
	assert.Equal(t, int32(2), broker.closeCount.Load()) // two counts - one for connection, one for channel

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte(errorChannelConnection)})
	assert.NotNil(t, err)
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)
	// Check that reconnection did not happened
	assert.Equal(t, int32(1), broker.connectCount.Load())
	assert.Equal(t, int32(2), broker.closeCount.Load()) // two counts - one for connection, one for channel
}

func TestSubscribeBindRoutingKeys(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataHostnameKey:   "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}}
	err := pubsubRabbitMQ.Init(context.Background(), metadata)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), broker.connectCount.Load())
	assert.Equal(t, int32(0), broker.closeCount.Load())

	topic := "mytopic_routingkeys"

	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		return nil
	}

	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: topic, Metadata: map[string]string{"routingKey": "keya,keyb,"}}, handler)
	assert.Nil(t, err)
}

func TestSubscribeReconnect(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataHostnameKey:             "anyhost",
			metadataConsumerIDKey:           "consumer",
			metadataAutoAckKey:              "true",
			metadataReconnectWaitSecondsKey: "0",
			pubsub.ConcurrencyKey:           string(pubsub.Single),
		},
	}}
	err := pubsubRabbitMQ.Init(context.Background(), metadata)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), broker.connectCount.Load())
	assert.Equal(t, int32(0), broker.closeCount.Load())

	topic := "thetopic"

	messageCount := 0
	lastMessage := ""
	processed := make(chan bool)
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		messageCount++
		lastMessage = string(msg.Data)
		processed <- true

		return errors.New(errorChannelConnection)
	}

	err = pubsubRabbitMQ.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for message")
	}
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	err = pubsubRabbitMQ.Publish(context.Background(), &pubsub.PublishRequest{Topic: topic, Data: []byte("foo bar")})
	assert.Nil(t, err)
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for message")
	}
	assert.Equal(t, 2, messageCount)
	assert.Equal(t, "foo bar", lastMessage)

	// allow last reconnect completion
	time.Sleep(time.Second)

	// Check that reconnection happened
	assert.Equal(t, int32(3), broker.connectCount.Load()) // initial connect + 2 reconnects
	assert.Equal(t, int32(4), broker.closeCount.Load())   // two counts for each connection closure - one for connection, one for channel
}

func createAMQPMessage(body []byte) amqp.Delivery {
	return amqp.Delivery{Body: body}
}

type rabbitMQInMemoryBroker struct {
	buffer chan amqp.Delivery

	connectCount atomic.Int32
	closeCount   atomic.Int32
}

func (r *rabbitMQInMemoryBroker) Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}

func (r *rabbitMQInMemoryBroker) PublishWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	// This is actually how the SDK implements it
	_, err := r.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	return err
}

func (r *rabbitMQInMemoryBroker) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	if string(msg.Body) == errorChannelConnection {
		return nil, errors.New(errorChannelConnection)
	}

	r.buffer <- createAMQPMessage(msg.Body)

	return nil, nil
}

func (r *rabbitMQInMemoryBroker) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

func (r *rabbitMQInMemoryBroker) QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error {
	return nil
}

func (r *rabbitMQInMemoryBroker) Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return r.buffer, nil
}

func (r *rabbitMQInMemoryBroker) Nack(tag uint64, multiple bool, requeue bool) error {
	return nil
}

func (r *rabbitMQInMemoryBroker) Ack(tag uint64, multiple bool) error {
	return nil
}

func (r *rabbitMQInMemoryBroker) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	return nil
}

func (r *rabbitMQInMemoryBroker) Confirm(noWait bool) error {
	return nil
}

func (r *rabbitMQInMemoryBroker) Close() error {
	r.closeCount.Add(1)

	return nil
}

func (r *rabbitMQInMemoryBroker) IsClosed() bool {
	return r.connectCount.Load() <= r.closeCount.Load()
}
