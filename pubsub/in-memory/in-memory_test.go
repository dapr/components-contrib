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

package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func TestNewInMemoryBus(t *testing.T) {
	bus := New(logger.NewLogger("test"))
	bus.Init(pubsub.Metadata{})

	ch := make(chan []byte)
	bus.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
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
	bus.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return publish(ch1, msg)
	})

	bus.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return publish(ch2, msg)
	})

	bus.Publish(&pubsub.PublishRequest{Data: []byte("ABCD"), Topic: "demo"})

	assert.Equal(t, "ABCD", string(<-ch1))
	assert.Equal(t, "ABCD", string(<-ch2))
}

func TestWildcards(t *testing.T) {
	bus := New(logger.NewLogger("test"))
	bus.Init(pubsub.Metadata{})

	ch1 := make(chan []byte)
	ch2 := make(chan []byte)
	bus.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: "mytopic"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return publish(ch1, msg)
	})

	bus.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: "topic*"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		return publish(ch2, msg)
	})

	bus.Publish(&pubsub.PublishRequest{Data: []byte("1"), Topic: "mytopic"})
	assert.Equal(t, "1", string(<-ch1))

	bus.Publish(&pubsub.PublishRequest{Data: []byte("2"), Topic: "topic1"})
	assert.Equal(t, "2", string(<-ch2))

	bus.Publish(&pubsub.PublishRequest{Data: []byte("3"), Topic: "topicX"})
	assert.Equal(t, "3", string(<-ch2))
}

func TestRetry(t *testing.T) {
	bus := New(logger.NewLogger("test"))
	bus.Init(pubsub.Metadata{})

	ch := make(chan []byte)
	i := -1

	bus.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: "demo"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
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
