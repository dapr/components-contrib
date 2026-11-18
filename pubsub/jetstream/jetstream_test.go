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
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// TestJetStream_Publish_ClosedIsTerminal asserts the cheaply-reachable
// closed-component guard in Publish is classified as terminal
// (codes.FailedPrecondition) so the runtime can stop retrying it.
func TestJetStream_Publish_ClosedIsTerminal(t *testing.T) {
	js := &jetstreamPubSub{
		l: logger.NewLogger("test"),
	}
	js.closed.Store(true)

	err := js.Publish(t.Context(), &pubsub.PublishRequest{Topic: "test", Data: []byte("hi")})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func setupServerAndStream(t *testing.T) (*server.Server, *nats.Conn) {
	// Create a new server with JetStream enabled.
	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	require.NoError(t, err)
	go ns.Start()
	ns.ReadyForConnections(time.Second)

	// Create the stream for the test.
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)

	js, err := nc.JetStream()
	require.NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "test",
		Subjects: []string{"test"},
		Storage:  nats.MemoryStorage,
	})
	require.NoError(t, err)

	return ns, nc
}

func TestNewJetStream_EmphemeralPushConsumer(t *testing.T) {
	ns, nc := setupServerAndStream(t)
	defer ns.Shutdown()
	defer nc.Drain()

	bus := NewJetStream(logger.NewLogger("test"))
	defer bus.Close()

	err := bus.Init(t.Context(), pubsub.Metadata{
		Base: mdata.Base{
			Properties: map[string]string{
				"natsURL": ns.ClientURL(),
			},
		},
	})
	require.NoError(t, err)

	ctx := t.Context()
	ch := make(chan []byte, 1)

	err = bus.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "test"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		ch <- msg.Data
		return nil
	})
	require.NoError(t, err)

	// Use minimal cloud event payload with `id` for NATS de-dupe.
	payload := []byte(`{"id": "ABCD", "data": "test"}`)
	err = bus.Publish(ctx, &pubsub.PublishRequest{
		Data:  payload,
		Topic: "test",
	})
	require.NoError(t, err)

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

	err := bus.Init(t.Context(), pubsub.Metadata{
		Base: mdata.Base{
			Properties: map[string]string{
				"natsURL":        ns.ClientURL(),
				"durableName":    "test",
				"queueGroupName": "test",
			},
		},
	})
	require.NoError(t, err)

	ctx := t.Context()
	ch := make(chan []byte, 2)

	// Two subscriptions to the same queue group. If there is a config
	// error, the second subscription would fail.
	err = bus.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "test"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		ch <- msg.Data
		return nil
	})
	require.NoError(t, err)

	err = bus.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "test"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		ch <- msg.Data
		return nil
	})
	require.NoError(t, err)

	js, _ := nc.JetStream()
	ci, err := js.ConsumerInfo("test", "test")
	require.NoError(t, err)
	assert.Equal(t, "test", ci.Config.Durable)
	assert.Equal(t, "test", ci.Config.DeliverGroup)

	// Use minimal cloud event payload with `id` for NATS de-dupe.
	payload := []byte(`{"id": "ABCD-1", "data": "test"}`)
	err = bus.Publish(ctx, &pubsub.PublishRequest{
		Data:  payload,
		Topic: "test",
	})
	require.NoError(t, err)

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

func TestJetStream_Init_ReconnectsIndefinitely(t *testing.T) {
	ns, nc := setupServerAndStream(t)
	defer ns.Shutdown()
	defer nc.Drain()

	bus := NewJetStream(logger.NewLogger("test")).(*jetstreamPubSub)
	defer bus.Close()

	err := bus.Init(t.Context(), pubsub.Metadata{
		Base: mdata.Base{
			Properties: map[string]string{
				"natsURL": ns.ClientURL(),
			},
		},
	})
	require.NoError(t, err)

	// nats.go closes the connection for good once MaxReconnect attempts are
	// used up (60 by default), and nothing re-creates the component after
	// that. A negative value keeps reconnecting.
	assert.Equal(t, -1, bus.nc.Opts.MaxReconnect)
}

// syncBuffer is a bytes.Buffer that the nats.go callback goroutine and the
// test goroutine can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startFileStoreServer(t *testing.T, port int, storeDir string) *server.Server {
	t.Helper()

	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: true,
		StoreDir:  storeDir,
	})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server not ready")

	return ns
}

// TestJetStream_RecoversAfterLongOutage stops the server for longer than
// nats.go's default reconnect budget would allow, then starts it again on the
// same port with the same JetStream state. Publishing and the existing
// subscription must both work again without re-initializing the component.
func TestJetStream_RecoversAfterLongOutage(t *testing.T) {
	const (
		reconnectWait = 10 * time.Millisecond
		// With nats.go's default MaxReconnect of 60, the connection is
		// closed after roughly 60*reconnectWait. Stay down well past that.
		outage = 60 * reconnectWait * 3
	)

	tests := []struct {
		name  string
		props map[string]string
	}{
		// The outage stays under the server's 5 s InactiveThreshold for ephemeral consumers: one idle longer than that is deleted server-side and not re-created, while durable consumers survive.
		{name: "ephemeral", props: map[string]string{}},
		{name: "durable", props: map[string]string{"durableName": "test"}},
		{name: "durable queue", props: map[string]string{"durableName": "test", "queueGroupName": "test"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeDir := t.TempDir()
			ns := startFileStoreServer(t, -1, storeDir)
			port := ns.Addr().(*net.TCPAddr).Port
			t.Cleanup(func() { ns.Shutdown() })

			nc, err := nats.Connect(ns.ClientURL())
			require.NoError(t, err)
			jsc, err := nc.JetStream()
			require.NoError(t, err)
			_, err = jsc.AddStream(&nats.StreamConfig{
				Name:     "test",
				Subjects: []string{"test"},
				Storage:  nats.FileStorage,
			})
			require.NoError(t, err)
			nc.Close()

			logs := &syncBuffer{}
			log := logger.NewLogger("jetstream-outage-" + t.Name())
			log.SetOutput(logs)

			bus := NewJetStream(log).(*jetstreamPubSub)
			bus.natsOpts = []nats.Option{
				nats.ReconnectWait(reconnectWait),
				nats.ReconnectJitter(0, 0),
			}
			t.Cleanup(func() { bus.Close() })

			props := map[string]string{"natsURL": ns.ClientURL()}
			for k, v := range tt.props {
				props[k] = v
			}
			require.NoError(t, bus.Init(t.Context(), pubsub.Metadata{
				Base: mdata.Base{Properties: props},
			}))

			ch := make(chan []byte, 10)
			require.NoError(t, bus.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "test"}, func(ctx context.Context, msg *pubsub.NewMessage) error {
				ch <- msg.Data
				return nil
			}))

			publishAndReceive := func(t *testing.T, payload []byte) {
				t.Helper()
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					assert.NoError(c, bus.Publish(t.Context(), &pubsub.PublishRequest{Topic: "test", Data: payload}))
				}, 10*time.Second, 50*time.Millisecond)
				select {
				case got := <-ch:
					assert.Equal(t, payload, got)
				case <-time.After(5 * time.Second):
					t.Fatalf("subscriber did not receive %s", payload)
				}
			}

			publishAndReceive(t, []byte(`{"id": "before-outage", "data": "test"}`))

			ns.Shutdown()
			ns.WaitForShutdown()
			time.Sleep(outage)

			assert.False(t, bus.nc.IsClosed(), "connection was closed during the outage")

			ns = startFileStoreServer(t, port, storeDir)
			t.Cleanup(func() { ns.Shutdown() })

			publishAndReceive(t, []byte(`{"id": "after-outage", "data": "test"}`))

			out := logs.String()
			assert.Contains(t, out, "disconnected from nats")
			assert.Contains(t, out, "reconnected to nats")
		})
	}
}
