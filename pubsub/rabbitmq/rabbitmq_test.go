package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func newBroker() *rabbitMQInMemoryBroker {
	return &rabbitMQInMemoryBroker{
		buffer: make(chan amqp.Delivery),
	}
}

func newRabbitMQTest(broker *rabbitMQInMemoryBroker) pubsub.PubSub {
	return &rabbitMQ{
		declaredExchanges: make(map[string]bool),
		stopped:           false,
		logger:            logger.NewLogger("test"),
		connectionDial: func(host string) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
			broker.connectCount++

			return broker, broker, nil
		},
	}
}

func TestNoHost(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	err := pubsubRabbitMQ.Init(pubsub.Metadata{})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "missing RabbitMQ host")
}

func TestNoConsumer(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{
		Properties: map[string]string{
			metadataHostKey: "anyhost",
		},
	}
	err := pubsubRabbitMQ.Init(metadata)
	assert.NoError(t, err)
	err = pubsubRabbitMQ.Subscribe(pubsub.SubscribeRequest{}, nil)
	assert.Contains(t, err.Error(), "consumerID is required for subscriptions")
}

func TestConcurrencyMode(t *testing.T) {
	t.Run("parallel", func(t *testing.T) {
		broker := newBroker()
		pubsubRabbitMQ := newRabbitMQTest(broker)
		metadata := pubsub.Metadata{
			Properties: map[string]string{
				metadataHostKey:       "anyhost",
				metadataConsumerIDKey: "consumer",
				pubsub.ConcurrencyKey: string(pubsub.Parallel),
			},
		}
		err := pubsubRabbitMQ.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, pubsub.Parallel, pubsubRabbitMQ.(*rabbitMQ).metadata.concurrency)
	})

	t.Run("single", func(t *testing.T) {
		broker := newBroker()
		pubsubRabbitMQ := newRabbitMQTest(broker)
		metadata := pubsub.Metadata{
			Properties: map[string]string{
				metadataHostKey:       "anyhost",
				metadataConsumerIDKey: "consumer",
				pubsub.ConcurrencyKey: string(pubsub.Single),
			},
		}
		err := pubsubRabbitMQ.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, pubsub.Single, pubsubRabbitMQ.(*rabbitMQ).metadata.concurrency)
	})

	t.Run("default", func(t *testing.T) {
		broker := newBroker()
		pubsubRabbitMQ := newRabbitMQTest(broker)
		metadata := pubsub.Metadata{
			Properties: map[string]string{
				metadataHostKey:       "anyhost",
				metadataConsumerIDKey: "consumer",
			},
		}
		err := pubsubRabbitMQ.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, pubsub.Parallel, pubsubRabbitMQ.(*rabbitMQ).metadata.concurrency)
	})
}

func TestPublishAndSubscribe(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{
		Properties: map[string]string{
			metadataHostKey:       "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}
	err := pubsubRabbitMQ.Init(metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, broker.connectCount)
	assert.Equal(t, 0, broker.closeCount)

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

	err = pubsubRabbitMQ.Subscribe(pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte("foo bar")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 2, messageCount)
	assert.Equal(t, "foo bar", lastMessage)
}

func TestPublishReconnect(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{
		Properties: map[string]string{
			metadataHostKey:       "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}
	err := pubsubRabbitMQ.Init(metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, broker.connectCount)
	assert.Equal(t, 0, broker.closeCount)

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

	err = pubsubRabbitMQ.Subscribe(pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte(errorChannelConnection)})
	assert.NotNil(t, err)
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)
	// Check that reconnection happened
	assert.Equal(t, 2, broker.connectCount)
	assert.Equal(t, 1, broker.closeCount)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte("foo bar")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 2, messageCount)
	assert.Equal(t, "foo bar", lastMessage)
}

func TestPublishReconnectAfterClose(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{
		Properties: map[string]string{
			metadataHostKey:       "anyhost",
			metadataConsumerIDKey: "consumer",
		},
	}
	err := pubsubRabbitMQ.Init(metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, broker.connectCount)
	assert.Equal(t, 0, broker.closeCount)

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

	err = pubsubRabbitMQ.Subscribe(pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	// Close PubSub
	err = pubsubRabbitMQ.Close()
	assert.Nil(t, err)
	assert.Equal(t, 1, broker.closeCount)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte(errorChannelConnection)})
	assert.NotNil(t, err)
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)
	// Check that reconnection did not happened
	assert.Equal(t, 1, broker.connectCount)
	assert.Equal(t, 1, broker.closeCount)
}

func TestSubscribeReconnect(t *testing.T) {
	broker := newBroker()
	pubsubRabbitMQ := newRabbitMQTest(broker)
	metadata := pubsub.Metadata{
		Properties: map[string]string{
			metadataHostKey:              "anyhost",
			metadataConsumerIDKey:        "consumer",
			metadataAutoAckKey:           "true",
			metadataReconnectWaitSeconds: "0",
			pubsub.ConcurrencyKey:        string(pubsub.Single),
		},
	}
	err := pubsubRabbitMQ.Init(metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, broker.connectCount)
	assert.Equal(t, 0, broker.closeCount)

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

	err = pubsubRabbitMQ.Subscribe(pubsub.SubscribeRequest{Topic: topic}, handler)
	assert.Nil(t, err)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte("hello world")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 1, messageCount)
	assert.Equal(t, "hello world", lastMessage)

	err = pubsubRabbitMQ.Publish(&pubsub.PublishRequest{Topic: topic, Data: []byte("foo bar")})
	assert.Nil(t, err)
	<-processed
	assert.Equal(t, 2, messageCount)
	assert.Equal(t, "foo bar", lastMessage)

	// Check that reconnection happened
	assert.Equal(t, 2, broker.connectCount)
	assert.Equal(t, 1, broker.closeCount)
}

func createAMQPMessage(body []byte) amqp.Delivery {
	return amqp.Delivery{Body: body}
}

type rabbitMQInMemoryBroker struct {
	buffer chan amqp.Delivery

	connectCount int
	closeCount   int
}

func (r *rabbitMQInMemoryBroker) Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}

func (r *rabbitMQInMemoryBroker) Publish(exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	if string(msg.Body) == errorChannelConnection {
		return errors.New(errorChannelConnection)
	}

	r.buffer <- createAMQPMessage(msg.Body)

	return nil
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

func (r *rabbitMQInMemoryBroker) Close() error {
	r.closeCount++

	return nil
}
