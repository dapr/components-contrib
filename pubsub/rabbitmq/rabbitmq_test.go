package rabbitmq

import (
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func createAmqpMessage(body string) amqp.Delivery {
	return amqp.Delivery{Body: []byte(body)}
}

func TestProcessSubscriberMessage(t *testing.T) {
	testMetadata := &metadata{autoAck: true}
	testRabbitMQSubscriber := &rabbitMQ{
		declaredExchanges: make(map[string]bool),
		logger:            logger.NewLogger("test"),
	}
	testRabbitMQSubscriber.metadata = testMetadata

	const topic = "testTopic"

	ch := make(chan amqp.Delivery)
	defer close(ch)

	messageCount := 0

	fakeHandler := func(msg *pubsub.NewMessage) error {
		messageCount++

		assert.Equal(t, topic, msg.Topic)
		assert.NotNil(t, msg.Data)

		return nil
	}

	go testRabbitMQSubscriber.listenMessages(ch, topic, fakeHandler)
	assert.Equal(t, messageCount, 0)
	ch <- createAmqpMessage("{ \"msg\": \"1\"}")
	ch <- createAmqpMessage("{ \"msg\": \"2\"}")
	assert.GreaterOrEqual(t, messageCount, 1)
	assert.LessOrEqual(t, messageCount, 2)
	ch <- createAmqpMessage("{ \"msg\": \"3\"}")
	assert.GreaterOrEqual(t, messageCount, 2)
	assert.LessOrEqual(t, messageCount, 3)
}
