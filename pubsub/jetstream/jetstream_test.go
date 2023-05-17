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

package jetstream

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func setupServerAndStream(t *testing.T) (*server.Server, *nats.Conn) {
	// Create a new server with JetStream enabled.
	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	assert.NoError(t, err)
	go ns.Start()
	ns.ReadyForConnections(time.Second)

	// Create the stream for the test.
	nc, err := nats.Connect(ns.ClientURL())
	assert.NoError(t, err)

	js, err := nc.JetStream()
	assert.NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "test",
		Subjects: []string{"test"},
		Storage:  nats.MemoryStorage,
	})
	assert.NoError(t, err)

	return ns, nc
}

func TestNewJetStream_EmphemeralPushConsumer(t *testing.T) {
	ns, nc := setupServerAndStream(t)
	defer ns.Shutdown()
	defer nc.Drain()

	bus := NewJetStream(logger.NewLogger("test"))
	defer bus.Close()

	err := bus.Init(context.Background(), pubsub.Metadata{
		Base: mdata.Base{
			Properties: map[string]string{
				"natsURL": ns.ClientURL(),
			},
		},
	})
	assert.NoError(t, err)

	ctx := context.Background()
	ch := make(chan []byte, 1)

	err = bus.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "test"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		ch <- msg.Data
		return nil
	})
	assert.NoError(t, err)

	// Use minimal cloud event payload with `id` for NATS de-dupe.
	payload := []byte(`{"id": "ABCD", "data": "test"}`)
	err = bus.Publish(ctx, &pubsub.PublishRequest{
		Data:  payload,
		Topic: "test",
	})
	assert.NoError(t, err)

	// Ensure the output is received.
	select {
	case output := <-ch:
		assert.Equal(t, payload, output)
	case <-time.After(time.Second):
		t.Fatal("receive timeout")
	}
}

func TestNewJetStream_DurableQueuePushConsumer(t *testing.T) {
	ns, nc := setupServerAndStream(t)
	defer ns.Shutdown()
	defer nc.Drain()

	bus := NewJetStream(logger.NewLogger("test"))
	defer bus.Close()

	err := bus.Init(context.Background(), pubsub.Metadata{
		Base: mdata.Base{
			Properties: map[string]string{
				"natsURL":        ns.ClientURL(),
				"durableName":    "test",
				"queueGroupName": "test",
			},
		},
	})
	assert.NoError(t, err)

	ctx := context.Background()
	ch := make(chan []byte, 2)

	// Two subscriptions to the same queue group. If there is a config
	// error, the second subscription would fail.
	err = bus.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "test"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		ch <- msg.Data
		return nil
	})
	assert.NoError(t, err)

	err = bus.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "test"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		ch <- msg.Data
		return nil
	})
	assert.NoError(t, err)

	js, _ := nc.JetStream()
	ci, err := js.ConsumerInfo("test", "test")
	assert.NoError(t, err)
	assert.Equal(t, ci.Config.Durable, "test")
	assert.Equal(t, ci.Config.DeliverGroup, "test")

	// Use minimal cloud event payload with `id` for NATS de-dupe.
	payload := []byte(`{"id": "ABCD-1", "data": "test"}`)
	err = bus.Publish(ctx, &pubsub.PublishRequest{
		Data:  payload,
		Topic: "test",
	})
	assert.NoError(t, err)

	// Ensure the output is received.
	select {
	case output := <-ch:
		assert.Equal(t, payload, output)
	case <-time.After(time.Second):
		t.Fatal("receive timeout")
	}

	// This confirms only one of the subs received the message.
	select {
	case output := <-ch:
		t.Fatalf("unexpected message received: %s", string(output))
	case <-time.After(10 * time.Millisecond):
	}
}
