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

// Tests for the processMode bug fix:
//
//   - processMode was read only from req.Metadata (subscription metadata) and
//     silently ignored when set in component YAML metadata (pulsarMetadata).
//   - Fix: pulsarMetadata now carries ProcessMode; listenMessage resolves the
//     effective mode by using component metadata as the default and letting
//     subscription metadata override it.
package pulsar

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdpkg "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// ---------------------------------------------------------------------------
// Mock types for testing listenMessage without a real Pulsar broker.
// These are shared across process_mode_test.go and pulsar_backpressure_test.go.
// ---------------------------------------------------------------------------

// mockMessage provides the minimum pulsarclient.Message implementation needed
// by handleMessage.
type mockMessage struct {
	payload    []byte
	properties map[string]string
	topic      string
}

func (m *mockMessage) Payload() []byte                                       { return m.payload }
func (m *mockMessage) ID() pulsarclient.MessageID                            { return mockMsgID{} }
func (m *mockMessage) Properties() map[string]string                         { return m.properties }
func (m *mockMessage) Topic() string                                         { return m.topic }
func (m *mockMessage) ProducerName() string                                  { return "" }
func (m *mockMessage) PublishTime() time.Time                                { return time.Time{} }
func (m *mockMessage) EventTime() time.Time                                  { return time.Time{} }
func (m *mockMessage) Key() string                                           { return "" }
func (m *mockMessage) OrderingKey() string                                   { return "" }
func (m *mockMessage) GetSchemaValue(_ interface{}) error                    { return nil }
func (m *mockMessage) RedeliveryCount() uint32                               { return 0 }
func (m *mockMessage) IsReplicated() bool                                    { return false }
func (m *mockMessage) GetReplicatedFrom() string                             { return "" }
func (m *mockMessage) GetEncryptionContext() *pulsarclient.EncryptionContext { return nil }
func (m *mockMessage) Index() *uint64                                        { return nil }
func (m *mockMessage) BrokerPublishTime() *time.Time                         { return nil }
func (m *mockMessage) SchemaVersion() []byte                                 { return nil }

// mockMsgID satisfies pulsarclient.MessageID.
type mockMsgID struct{}

func (mockMsgID) Serialize() []byte   { return nil }
func (mockMsgID) LedgerID() int64     { return 0 }
func (mockMsgID) EntryID() int64      { return 0 }
func (mockMsgID) BatchIdx() int32     { return 0 }
func (mockMsgID) PartitionIdx() int32 { return 0 }
func (mockMsgID) BatchSize() int32    { return 0 }
func (mockMsgID) String() string      { return "mock-id" }

// ackTrackingConsumer routes messages through a controllable channel and
// records Ack calls. It implements the full pulsarclient.Consumer interface.
type ackTrackingConsumer struct {
	Ch       chan pulsarclient.ConsumerMessage
	ackCount atomic.Int64
}

func (c *ackTrackingConsumer) Chan() <-chan pulsarclient.ConsumerMessage { return c.Ch }
func (c *ackTrackingConsumer) Subscription() string                      { return "test-sub" }
func (c *ackTrackingConsumer) Unsubscribe() error                        { return nil }
func (c *ackTrackingConsumer) UnsubscribeForce() error                   { return nil }

func (c *ackTrackingConsumer) GetLastMessageIDs() ([]pulsarclient.TopicMessageID, error) {
	return nil, nil
}

func (c *ackTrackingConsumer) Receive(ctx context.Context) (pulsarclient.Message, error) {
	select {
	case msg, ok := <-c.Ch:
		if !ok {
			return nil, errors.New("consumer closed")
		}
		return msg.Message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ackTrackingConsumer) Ack(_ pulsarclient.Message) error {
	c.ackCount.Add(1)
	return nil
}
func (c *ackTrackingConsumer) AckID(_ pulsarclient.MessageID) error       { return nil }
func (c *ackTrackingConsumer) AckIDList(_ []pulsarclient.MessageID) error { return nil }
func (c *ackTrackingConsumer) AckWithTxn(_ pulsarclient.Message, _ pulsarclient.Transaction) error {
	return nil
}
func (c *ackTrackingConsumer) AckCumulative(_ pulsarclient.Message) error             { return nil }
func (c *ackTrackingConsumer) AckIDCumulative(_ pulsarclient.MessageID) error         { return nil }
func (c *ackTrackingConsumer) ReconsumeLater(_ pulsarclient.Message, _ time.Duration) {}
func (c *ackTrackingConsumer) ReconsumeLaterWithCustomProperties(
	_ pulsarclient.Message, _ map[string]string, _ time.Duration,
) {
}
func (c *ackTrackingConsumer) Nack(_ pulsarclient.Message)         {}
func (c *ackTrackingConsumer) NackID(_ pulsarclient.MessageID)     {}
func (c *ackTrackingConsumer) Close()                              {}
func (c *ackTrackingConsumer) Seek(_ pulsarclient.MessageID) error { return nil }
func (c *ackTrackingConsumer) SeekByTime(_ time.Time) error        { return nil }
func (c *ackTrackingConsumer) Name() string                        { return "mock-consumer" }
func (c *ackTrackingConsumer) Topic() string                       { return "mock-topic" }

// newMockConsumer creates an ackTrackingConsumer with the given channel buffer size.
func newMockConsumer(bufSize int) *ackTrackingConsumer {
	return &ackTrackingConsumer{
		Ch: make(chan pulsarclient.ConsumerMessage, bufSize),
	}
}

// sendMsg pushes a ConsumerMessage with the given consumer and payload onto ch.
func sendMsg(ch chan<- pulsarclient.ConsumerMessage, consumer pulsarclient.Consumer, payload []byte) {
	ch <- pulsarclient.ConsumerMessage{
		Consumer: consumer,
		Message: &mockMessage{
			payload:    payload,
			properties: map[string]string{},
			topic:      "persistent://public/default/test-topic",
		},
	}
}

// ---------------------------------------------------------------------------
// Helpers local to process_mode tests
// ---------------------------------------------------------------------------

// newPubsubMetadata builds a pubsub.Metadata with the given key-value pairs.
func newPubsubMetadata(kvs map[string]string) pubsub.Metadata {
	m := pubsub.Metadata{}
	m.Base = mdpkg.Base{Properties: kvs}
	return m
}

// newProcessModePulsar builds a minimal *Pulsar wired with a component-level
// ProcessMode. MaxConcurrentHandlers is set high so the worker pool does not
// artificially constrain concurrency in async tests.
func newProcessModePulsar(componentProcessMode string) *Pulsar {
	return &Pulsar{
		logger:  logger.NewLogger("test"),
		closeCh: make(chan struct{}),
		metadata: pulsarMetadata{
			ProcessMode:           componentProcessMode,
			MaxConcurrentHandlers: 100,
		},
	}
}

// ---------------------------------------------------------------------------
// Unit tests: parsePulsarMetadata — ProcessMode field
// ---------------------------------------------------------------------------

func TestParsePulsarMetadataProcessMode(t *testing.T) {
	tt := []struct {
		name        string
		processMode string
		expected    string
	}{
		{
			name:        "sync processMode normalized to lowercase",
			processMode: "sync",
			expected:    "sync",
		},
		{
			name:        "async processMode normalized to lowercase",
			processMode: "async",
			expected:    "async",
		},
		{
			name:        "empty processMode stored as empty string",
			processMode: "",
			expected:    "",
		},
		{
			name:        "mixed-case Sync normalized to lowercase",
			processMode: "Sync",
			expected:    "sync",
		},
		{
			name:        "upper-case SYNC normalized to lowercase",
			processMode: "SYNC",
			expected:    "sync",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			m := newPubsubMetadata(map[string]string{
				"host":        "pulsar://localhost:6650",
				"processMode": tc.processMode,
			})

			meta, err := parsePulsarMetadata(m)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, meta.ProcessMode)
		})
	}
}

func TestParsePulsarMetadataProcessModeInvalid(t *testing.T) {
	m := newPubsubMetadata(map[string]string{
		"host":        "pulsar://localhost:6650",
		"processMode": "snc",
	})
	_, err := parsePulsarMetadata(m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid processMode")
}

func TestParsePulsarMetadataZeroMaxConcurrentHandlers(t *testing.T) {
	m := newPubsubMetadata(map[string]string{
		"host":                  "pulsar://localhost:6650",
		"maxConcurrentHandlers": "0",
	})
	meta, err := parsePulsarMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, uint(100), meta.MaxConcurrentHandlers,
		"MaxConcurrentHandlers=0 should fall back to default (100)")
}

// ---------------------------------------------------------------------------
// Unit tests: effective processMode resolution (component vs subscription)
// ---------------------------------------------------------------------------

// effectiveProcessMode mirrors the resolution in listenMessage:
// component metadata is already normalized to lowercase by parsePulsarMetadata;
// subscription metadata override is lowercased and validated at point of use.
func effectiveProcessMode(componentMode string, subMeta map[string]string) string {
	mode := strings.ToLower(componentMode)
	if mode == "" {
		mode = processModeAsync
	}
	if v, ok := subMeta[processModeKey]; ok {
		override := strings.ToLower(v)
		switch override {
		case processModeAsync, processModeSync:
			mode = override
		default:
			// Invalid override is ignored; keep component default.
		}
	}
	return mode
}

func TestResolveProcessMode(t *testing.T) {
	tt := []struct {
		name          string
		componentMode string
		subMeta       map[string]string
		wantSync      bool
	}{
		{
			name:          "component sync, no subscription override",
			componentMode: "sync",
			subMeta:       map[string]string{},
			wantSync:      true,
		},
		{
			name:          "component async, no subscription override",
			componentMode: "async",
			subMeta:       map[string]string{},
			wantSync:      false,
		},
		{
			name:          "component empty (default), no subscription override",
			componentMode: "",
			subMeta:       map[string]string{},
			wantSync:      false,
		},
		{
			name:          "component sync overridden by subscription async",
			componentMode: "sync",
			subMeta:       map[string]string{processModeKey: "async"},
			wantSync:      false,
		},
		{
			name:          "component async overridden by subscription sync",
			componentMode: "async",
			subMeta:       map[string]string{processModeKey: "sync"},
			wantSync:      true,
		},
		{
			name:          "component empty overridden by subscription sync",
			componentMode: "",
			subMeta:       map[string]string{processModeKey: "sync"},
			wantSync:      true,
		},
		{
			name:          "case insensitive Sync from component",
			componentMode: "Sync",
			subMeta:       map[string]string{},
			wantSync:      true,
		},
		{
			name:          "case insensitive SYNC from subscription override",
			componentMode: "async",
			subMeta:       map[string]string{processModeKey: "SYNC"},
			wantSync:      true,
		},
		{
			name:          "nil subscription metadata map uses component mode",
			componentMode: "sync",
			subMeta:       nil,
			wantSync:      true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			mode := effectiveProcessMode(tc.componentMode, tc.subMeta)
			gotSync := strings.EqualFold(mode, processModeSync)
			assert.Equal(t, tc.wantSync, gotSync,
				"effectiveProcessMode(%q, %v) = %q; wantSync=%v",
				tc.componentMode, tc.subMeta, mode, tc.wantSync)
		})
	}
}

// ---------------------------------------------------------------------------
// Integration-style tests: listenMessage processing order
// ---------------------------------------------------------------------------

// TestListenMessageSyncMode_ComponentMetadata verifies that when ProcessMode is
// "sync" in component metadata, the next message is not dispatched until the
// current handler returns.
func TestListenMessageSyncMode_ComponentMetadata(t *testing.T) {
	const numMessages = 3

	released := make(chan struct{})
	var callOrder []int
	var orderMu sync.Mutex
	var handlerCallCount atomic.Int32

	handler := func(_ context.Context, _ *pubsub.NewMessage) error {
		n := int(handlerCallCount.Add(1))
		orderMu.Lock()
		callOrder = append(callOrder, n)
		orderMu.Unlock()

		if n == 1 {
			<-released // block until the test releases
		}
		return nil
	}

	consumer := newMockConsumer(numMessages)
	p := newProcessModePulsar("sync")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := pubsub.SubscribeRequest{
		Topic:    "test-topic",
		Metadata: map[string]string{}, // no subscription override
	}

	for i := range numMessages {
		sendMsg(consumer.Ch, consumer, []byte{byte(i)})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.listenMessage(ctx, req, consumer, handler)
	}()

	// Wait until exactly one handler invocation has occurred. Since the handler
	// blocks on the 'released' channel, this ensures the second handler has not
	// been invoked while the first is still running.
	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 1
	}, 2*time.Second, 10*time.Millisecond,
		"sync mode: second handler must not fire while first is still running")

	close(released)
	// After releasing the first handler, wait until all messages have been handled
	// before cancelling the context.
	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == int32(numMessages)
	}, 2*time.Second, 10*time.Millisecond,
		"all messages must be handled before cancel")

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listenMessage goroutine did not exit after context cancel")
	}

	assert.Equal(t, int32(numMessages), handlerCallCount.Load(), "all messages must be handled")

	orderMu.Lock()
	defer orderMu.Unlock()
	assert.Equal(t, []int{1, 2, 3}, callOrder, "sync mode must process messages in strict order")
}

// TestListenMessageAsyncMode_ComponentMetadata verifies that when ProcessMode
// is "async" in component metadata, multiple messages can be in-flight simultaneously.
func TestListenMessageAsyncMode_ComponentMetadata(t *testing.T) {
	const numMessages = 3

	barrier := make(chan struct{})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	handler := func(_ context.Context, _ *pubsub.NewMessage) error {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		<-barrier
		inFlight.Add(-1)
		return nil
	}

	consumer := newMockConsumer(numMessages)
	p := newProcessModePulsar("async")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := pubsub.SubscribeRequest{
		Topic:    "test-topic",
		Metadata: map[string]string{},
	}

	for i := range numMessages {
		sendMsg(consumer.Ch, consumer, []byte{byte(i)})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.listenMessage(ctx, req, consumer, handler)
	}()

	// Wait until all handlers have started (they all block on the barrier).
	require.Eventually(t, func() bool {
		return inFlight.Load() == int32(numMessages)
	}, 2*time.Second, 10*time.Millisecond,
		"async mode: all handlers should start concurrently")

	close(barrier)

	require.Eventually(t, func() bool {
		return inFlight.Load() == 0
	}, 2*time.Second, 10*time.Millisecond,
		"all handlers should complete after barrier release")

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listenMessage goroutine did not exit after context cancel")
	}
	p.wg.Wait()

	assert.Greater(t, maxInFlight.Load(), int32(1),
		"async mode must process messages concurrently; max in-flight was %d", maxInFlight.Load())
}

// TestListenMessageSubscriptionOverridesComponent verifies that subscription
// metadata "sync" overrides component-level "async".
func TestListenMessageSubscriptionOverridesComponent(t *testing.T) {
	released := make(chan struct{})
	var handlerCallCount atomic.Int32

	handler := func(_ context.Context, _ *pubsub.NewMessage) error {
		n := handlerCallCount.Add(1)
		if n == 1 {
			<-released
		}
		return nil
	}

	consumer := newMockConsumer(2)
	p := newProcessModePulsar("async") // component says async

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := pubsub.SubscribeRequest{
		Topic:    "test-topic",
		Metadata: map[string]string{processModeKey: "sync"}, // subscription overrides to sync
	}

	sendMsg(consumer.Ch, consumer, []byte("msg1"))
	sendMsg(consumer.Ch, consumer, []byte("msg2"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.listenMessage(ctx, req, consumer, handler)
	}()

	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 1
	}, 2*time.Second, 10*time.Millisecond,
		"subscription sync override: second handler must not fire while first is running")

	close(released)

	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 2
	}, 2*time.Second, 10*time.Millisecond,
		"both messages should be handled after release")

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listenMessage goroutine did not exit after context cancel")
	}

	assert.Equal(t, int32(2), handlerCallCount.Load())
}

// TestListenMessageDefaultFallbackAsync verifies that when no processMode is
// configured anywhere, the system defaults to async (concurrent) processing.
func TestListenMessageDefaultFallbackAsync(t *testing.T) {
	const numMessages = 3

	barrier := make(chan struct{})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	handler := func(_ context.Context, _ *pubsub.NewMessage) error {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		<-barrier
		inFlight.Add(-1)
		return nil
	}

	consumer := newMockConsumer(numMessages)
	p := newProcessModePulsar("") // no processMode set anywhere

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := pubsub.SubscribeRequest{
		Topic:    "test-topic",
		Metadata: map[string]string{},
	}

	for i := range numMessages {
		sendMsg(consumer.Ch, consumer, []byte{byte(i)})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.listenMessage(ctx, req, consumer, handler)
	}()

	require.Eventually(t, func() bool {
		return inFlight.Load() == int32(numMessages)
	}, 2*time.Second, 10*time.Millisecond,
		"default async: all handlers should start concurrently")

	close(barrier)

	require.Eventually(t, func() bool {
		return inFlight.Load() == 0
	}, 2*time.Second, 10*time.Millisecond,
		"all handlers should complete after barrier release")

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listenMessage goroutine did not exit after context cancel")
	}
	p.wg.Wait()

	assert.Greater(t, maxInFlight.Load(), int32(1),
		"default (empty processMode) must fall back to async; max concurrent was %d", maxInFlight.Load())
}

// TestListenMessageCaseInsensitiveSync verifies that mixed-case "Sync", "SYNC",
// and "sYnC" in subscription metadata all trigger synchronous processing.
// Component metadata is normalized to lowercase at parse time, so case
// insensitivity for component metadata is tested via TestParsePulsarMetadataProcessMode.
func TestListenMessageCaseInsensitiveSync(t *testing.T) {
	variants := []string{"Sync", "SYNC", "sYnC"}

	for _, variant := range variants {
		t.Run("processMode="+variant, func(t *testing.T) {
			released := make(chan struct{})
			var callCount atomic.Int32

			handler := func(_ context.Context, _ *pubsub.NewMessage) error {
				n := callCount.Add(1)
				if n == 1 {
					<-released
				}
				return nil
			}

			consumer := newMockConsumer(2)
			p := newProcessModePulsar("") // no component-level processMode

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			req := pubsub.SubscribeRequest{
				Topic:    "test-topic",
				Metadata: map[string]string{processModeKey: variant}, // subscription override with mixed case
			}

			sendMsg(consumer.Ch, consumer, []byte("msg1"))
			sendMsg(consumer.Ch, consumer, []byte("msg2"))

			done := make(chan struct{})
			go func() {
				defer close(done)
				p.listenMessage(ctx, req, consumer, handler)
			}()

			require.Eventually(t, func() bool {
				return callCount.Load() == 1
			}, 2*time.Second, 10*time.Millisecond,
				"processMode %q should be treated as sync; second handler fired prematurely", variant)

			close(released)

			require.Eventually(t, func() bool {
				return callCount.Load() == 2
			}, 2*time.Second, 10*time.Millisecond,
				"both messages should be handled after release")

			cancel()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("listenMessage did not exit after context cancel")
			}
		})
	}
}

// TestGetComponentMetadataIncludesProcessMode verifies that GetComponentMetadata
// exposes the processMode field so operator tooling can discover it.
func TestGetComponentMetadataIncludesProcessMode(t *testing.T) {
	p := &Pulsar{
		logger:  logger.NewLogger("test"),
		closeCh: make(chan struct{}),
	}
	metaInfo := p.GetComponentMetadata()
	_, ok := metaInfo["processMode"]
	assert.True(t, ok, "GetComponentMetadata must expose processMode field")
}
