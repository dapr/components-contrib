package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestNewInMemoryBus(t *testing.T) {
	bus := New(logger.NewLogger("test"))
	bus.Init(pubsub.Metadata{})

	ch := make(chan []byte)
	bus.Subscribe(pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return publish(ch, msg)
	})

	bus.Publish(&pubsub.PublishRequest{Data: []byte("ABCD"), Topic: "demo"})
	assert.Equal(t, "ABCD", string(<-ch))
}

func TestMultipleSubscribers(t *testing.T) {
	bus := New(logger.NewLogger("test"))
	bus.Init(pubsub.Metadata{})

	ch1 := make(chan []byte)
	ch2 := make(chan []byte)
	bus.Subscribe(pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return publish(ch1, msg)
	})

	bus.Subscribe(pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return publish(ch2, msg)
	})

	bus.Publish(&pubsub.PublishRequest{Data: []byte("ABCD"), Topic: "demo"})

	assert.Equal(t, "ABCD", string(<-ch1))
	assert.Equal(t, "ABCD", string(<-ch2))
}

func TestRetry(t *testing.T) {
	bus := New(logger.NewLogger("test"))
	bus.Init(pubsub.Metadata{})

	ch := make(chan []byte)
	i := -1

	bus.Subscribe(pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		i++
		if i < 5 {
			return errors.New("if at first you don't succeed")
		}

		return publish(ch, msg)
	})

	bus.Publish(&pubsub.PublishRequest{Data: []byte("ABCD"), Topic: "demo"})
	assert.Equal(t, "ABCD", string(<-ch))
	assert.Equal(t, 5, i)
}

func publish(ch chan []byte, msg *pubsub.NewMessage) error {
	go func() { ch <- msg.Data }()

	return nil
}
