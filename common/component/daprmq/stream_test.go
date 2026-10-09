/*
Copyright 2026 The Dapr Authors
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

package daprmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/olitomlinson/dapr-mq/sdks/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// fakeStream stands in for a DaprMQ Consume stream: tests push deliveries into it and read back
// how each was settled.
type fakeStream struct {
	deliveries chan *fakeDelivery
	closed     chan struct{}
	closeOnce  sync.Once
	endErr     error // what Receive returns once deliveries is closed
	settled    chan string
}

func newFakeStream() *fakeStream {
	return &fakeStream{deliveries: make(chan *fakeDelivery, 100), closed: make(chan struct{}), settled: make(chan string, 100)}
}

func (s *fakeStream) Receive() (streamDelivery, error) {
	select {
	case d, ok := <-s.deliveries:
		if !ok {
			if s.endErr != nil {
				return nil, s.endErr
			}
			return nil, io.EOF
		}
		return d, nil
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *fakeStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *fakeStream) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// deliver queues a delivery whose item is the envelope for data.
func (s *fakeStream) deliver(lockID, data string, deliveryCount int) {
	item, _ := json.Marshal(wrap([]byte(data), nil, nil))
	s.deliveries <- &fakeDelivery{lockID: lockID, item: item, deliveryCount: deliveryCount, stream: s}
}

type fakeDelivery struct {
	lockID        string
	item          json.RawMessage
	deliveryCount int
	stream        *fakeStream
}

func (d *fakeDelivery) LockID() string        { return d.lockID }
func (d *fakeDelivery) Item() json.RawMessage { return d.item }
func (d *fakeDelivery) DeliveryCount() int    { return d.deliveryCount }
func (d *fakeDelivery) Ack() error            { return d.settle("ack") }
func (d *fakeDelivery) Nack() error           { return d.settle("nack") }
func (d *fakeDelivery) DeadLetter() error     { return d.settle("deadletter") }
func (d *fakeDelivery) settle(how string) error {
	if d.stream.isClosed() {
		return errors.New("stream is closed")
	}
	d.stream.settled <- how + ":" + d.lockID
	return nil
}

// streamOpens records every stream the component opens and hands out the given fake streams in turn.
type streamOpens struct {
	mu      sync.Mutex
	streams []*fakeStream
	options []*mq.ConsumeOptions
	queues  []string
}

func (o *streamOpens) open(_ context.Context, queueID string, options *mq.ConsumeOptions) (queueStream, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := len(o.options)
	o.options = append(o.options, options)
	o.queues = append(o.queues, queueID)
	if n >= len(o.streams) {
		return nil, errors.New("no more streams")
	}
	return o.streams[n], nil
}

func (o *streamOpens) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.options)
}

func newStreamComponent(t *testing.T, extra map[string]string, streams ...*fakeStream) (*PubSub, *streamOpens) {
	t.Helper()
	props := map[string]string{"httpEndpoint": "http://localhost:1", "grpcEndpoint": "localhost:1", "consumerID": "app"}
	for k, v := range extra {
		props[k] = v
	}
	ps := New(logger.NewLogger("test"), queueEntity{})
	require.NoError(t, ps.Init(t.Context(), pubsub.Metadata{Base: metadata.Base{Properties: props}}))
	opens := &streamOpens{streams: streams}
	ps.openStream = opens.open
	t.Cleanup(func() { _ = ps.Close() })
	return ps, opens
}

func nextSettled(t *testing.T, s *fakeStream) string {
	t.Helper()
	select {
	case got := <-s.settled:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("nothing settled")
		return ""
	}
}

func TestStreamModeOpensTheStreamWithTheSettings(t *testing.T) {
	stream := newFakeStream()
	ps, opens := newStreamComponent(t, map[string]string{"maxActiveMessages": "250", "lockTTL": "45s"}, stream)

	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error { return nil }))

	require.Eventually(t, func() bool { return opens.count() == 1 }, 5*time.Second, 10*time.Millisecond)
	opens.mu.Lock()
	defer opens.mu.Unlock()
	assert.Equal(t, "orders", opens.queues[0])
	assert.Equal(t, 250, opens.options[0].PrefetchCount)
	assert.Equal(t, 45*time.Second, opens.options[0].LockTTL)
	assert.True(t, opens.options[0].AllowCompetingConsumers)
}

func TestStreamModeAcksHandledMessagesAndNacksFailedOnes(t *testing.T) {
	stream := newFakeStream()
	ps, _ := newStreamComponent(t, nil, stream)
	stream.deliver("L1", `"ok"`, 1)
	stream.deliver("L2", `"fail"`, 3)

	deliveryCounts := make(chan string, 2)
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(_ context.Context, msg *pubsub.NewMessage) error {
		deliveryCounts <- string(msg.Data) + "=" + msg.Metadata["metadata.DeliveryCount"]
		if string(msg.Data) == `"fail"` {
			return errors.New("boom")
		}
		return nil
	}))

	assert.ElementsMatch(t, []string{"ack:L1", "nack:L2"}, []string{nextSettled(t, stream), nextSettled(t, stream)})
	assert.ElementsMatch(t, []string{`"ok"=1`, `"fail"=3`}, []string{<-deliveryCounts, <-deliveryCounts})
}

func TestStreamModeDeadLettersAnUndecodableItem(t *testing.T) {
	stream := newFakeStream()
	ps, _ := newStreamComponent(t, nil, stream)
	stream.deliveries <- &fakeDelivery{lockID: "L1", item: json.RawMessage(`"not an envelope"`), stream: stream}

	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		t.Error("handler must not be called")
		return nil
	}))

	assert.Equal(t, "deadletter:L1", nextSettled(t, stream))
}

func TestStreamModeStrictOrderUsesAWindowOfOneAndHandlesInTurn(t *testing.T) {
	for name, extra := range map[string]map[string]string{
		"competing consumers off": {"allowCompetingConsumers": "false"},
		"single concurrency":      {"concurrencyMode": "single"},
	} {
		t.Run(name, func(t *testing.T) {
			stream := newFakeStream()
			ps, opens := newStreamComponent(t, extra, stream)
			for i := range 4 {
				stream.deliver(fmt.Sprintf("L%d", i), strconv.Itoa(i), 1)
			}

			var active, maxActive atomic.Int32
			var mu sync.Mutex
			var order []string
			require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(_ context.Context, msg *pubsub.NewMessage) error {
				n := active.Add(1)
				if n > maxActive.Load() {
					maxActive.Store(n)
				}
				mu.Lock()
				order = append(order, string(msg.Data))
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
				active.Add(-1)
				return nil
			}))

			for range 4 {
				nextSettled(t, stream)
			}
			assert.Equal(t, int32(1), maxActive.Load())
			mu.Lock()
			assert.Equal(t, []string{"0", "1", "2", "3"}, order)
			mu.Unlock()
			opens.mu.Lock()
			assert.Equal(t, 1, opens.options[0].PrefetchCount)
			opens.mu.Unlock()
		})
	}
}

func TestStreamModeMaxConcurrentHandlersCapsHandlersRunningAtOnce(t *testing.T) {
	stream := newFakeStream()
	ps, _ := newStreamComponent(t, map[string]string{"maxConcurrentHandlers": "2"}, stream)
	for i := range 6 {
		stream.deliver(fmt.Sprintf("L%d", i), "1", 1)
	}

	var active, maxActive atomic.Int32
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		n := active.Add(1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		return nil
	}))

	for range 6 {
		nextSettled(t, stream)
	}
	assert.Equal(t, int32(2), maxActive.Load())
}

func TestStreamModeUnsubscribeLetsRunningHandlersSettleThenClosesTheStream(t *testing.T) {
	stream := newFakeStream()
	ps, _ := newStreamComponent(t, nil, stream)
	stream.deliver("L1", "1", 1)

	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	var handled atomic.Int32
	require.NoError(t, ps.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		if handled.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}))
	<-started

	cancel()
	stream.deliver("L2", "2", 1) // arrives after unsubscribe: left for the server to return
	time.Sleep(50 * time.Millisecond)
	assert.False(t, stream.isClosed(), "the stream stays open while a handler is still running")
	close(release)

	assert.Equal(t, "ack:L1", nextSettled(t, stream))
	require.Eventually(t, stream.isClosed, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(1), handled.Load())
}

func TestStreamModeReopensAStreamThatEnds(t *testing.T) {
	first, second := newFakeStream(), newFakeStream()
	first.endErr = errors.New("connection reset")
	close(first.deliveries)
	ps, opens := newStreamComponent(t, nil, first, second)
	second.deliver("L1", "1", 1)

	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error { return nil }))

	assert.Equal(t, "ack:L1", nextSettled(t, second))
	assert.Equal(t, 2, opens.count())
}

func TestFailedMessagesArePacedByMaxRetriableErrorsPerSec(t *testing.T) {
	stream := newFakeStream()
	ps, _ := newStreamComponent(t, map[string]string{"maxRetriableErrorsPerSec": "4", "concurrencyMode": "single"}, stream)
	for i := range 5 {
		stream.deliver(fmt.Sprintf("L%d", i), "1", 1)
	}

	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		return errors.New("boom")
	}))

	start := time.Now()
	for range 5 {
		nextSettled(t, stream)
	}
	// Five failures at four per second take at least a second.
	assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond)
}
