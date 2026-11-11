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
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	mq "github.com/olitomlinson/dapr-mq/sdks/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/component/daprmq/daprmqtest"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// queueEntity maps a topic to the queue of the same name; the component types' own tests cover
// their entities.
type queueEntity struct{}

func (queueEntity) Publish(ctx context.Context, client *mq.Client, topic string, items []mq.EnqueueItem) error {
	_, err := client.Enqueue(ctx, topic, items, nil)
	return err
}

func (queueEntity) SubscriberQueue(_ context.Context, _ *mq.Client, _, topic string) (string, error) {
	return topic, nil
}

func (queueEntity) RequiresConsumerID() bool { return true }

func newTestComponent(t *testing.T, fake *daprmqtest.Fake, extra map[string]string) *PubSub {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	props := map[string]string{
		"httpEndpoint": srv.URL,
		"grpcEndpoint": "localhost:1",
		"consumerID":   "app",
		"pollInterval": "10ms",
		"receiveMode":  "poll",
	}
	for k, v := range extra {
		props[k] = v
	}

	ps := New(logger.NewLogger("test"), queueEntity{})
	require.NoError(t, ps.Init(t.Context(), pubsub.Metadata{Base: metadata.Base{Properties: props}}))
	t.Cleanup(func() { _ = ps.Close() })
	return ps
}

func TestEnvelopeRoundTripsJSONAndBinaryPayloads(t *testing.T) {
	for name, data := range map[string][]byte{
		"json":   []byte(`{"specversion":"1.0","data":{"a":1}}`),
		"binary": {0x00, 0xff, 0x10},
		"text":   []byte("hello"),
	} {
		t.Run(name, func(t *testing.T) {
			item := wrap(data, ptr.Of("application/x-test"), map[string]string{"k": "v"})
			raw, err := json.Marshal(item)
			require.NoError(t, err)

			msg, err := unwrap(raw, "orders")
			require.NoError(t, err)

			assert.Equal(t, data, msg.Data)
			assert.Equal(t, "orders", msg.Topic)
			assert.Equal(t, "application/x-test", *msg.ContentType)
			assert.Equal(t, map[string]string{"k": "v"}, msg.Metadata)
		})
	}
}

func TestEnvelopeKeepsJSONPayloadsAsJSON(t *testing.T) {
	raw, err := json.Marshal(wrap([]byte(`{"a":1}`), nil, nil))
	require.NoError(t, err)

	assert.JSONEq(t, `{"data":{"a":1}}`, string(raw))
}

func TestPublishSendsTheWrappedMessageToTheEntity(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, nil)

	err := ps.Publish(t.Context(), &pubsub.PublishRequest{
		Topic: "orders", Data: []byte(`{"id":1}`), ContentType: ptr.Of("application/json"),
		Metadata: map[string]string{"priority": "0"},
	})
	require.NoError(t, err)

	fake.Snapshot(func(f *daprmqtest.Fake) {
		require.Len(t, f.Queues["orders"], 1)
		assert.JSONEq(t, `{"data":{"id":1},"contentType":"application/json","metadata":{"priority":"0"}}`, string(f.Queues["orders"][0]))
		assert.Equal(t, []int{0}, f.Priorities)
	})
}

func TestPublishRejectsAnInvalidPriority(t *testing.T) {
	ps := newTestComponent(t, daprmqtest.New(), nil)

	err := ps.Publish(t.Context(), &pubsub.PublishRequest{Topic: "orders", Data: []byte(`1`), Metadata: map[string]string{"priority": "high"}})

	require.Error(t, err)
}

func TestBulkPublishSendsEveryEntryInOneCall(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, nil)

	res, err := ps.BulkPublish(t.Context(), &pubsub.BulkPublishRequest{
		Topic: "orders",
		Entries: []pubsub.BulkMessageEntry{
			{EntryId: "1", Event: []byte(`1`), ContentType: "application/json"},
			{EntryId: "2", Event: []byte(`2`), ContentType: "application/json"},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, res.FailedEntries)

	fake.Snapshot(func(f *daprmqtest.Fake) {
		assert.Len(t, f.Queues["orders"], 2)
	})
	assert.Contains(t, ps.Features(), pubsub.FeatureBulkPublish)
}

func TestBulkPublishFailureFailsEveryEntry(t *testing.T) {
	ps := newTestComponent(t, daprmqtest.New(), nil)

	res, err := ps.BulkPublish(t.Context(), &pubsub.BulkPublishRequest{
		Topic: "orders",
		Entries: []pubsub.BulkMessageEntry{
			{EntryId: "1", Event: []byte(`1`), Metadata: map[string]string{"priority": "high"}},
			{EntryId: "2", Event: []byte(`2`)},
		},
	})

	require.Error(t, err)
	assert.Len(t, res.FailedEntries, 2)
}

func TestSubscribeAcksHandledMessagesAndNacksFailedOnes(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, nil)
	queueID := "orders"
	for _, data := range []string{`"ok"`, `"fail"`} {
		raw, _ := json.Marshal(wrap([]byte(data), nil, nil))
		fake.Enqueue(queueID, raw)
	}

	received := make(chan string, 2)
	err := ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(_ context.Context, msg *pubsub.NewMessage) error {
		received <- string(msg.Data)
		if string(msg.Data) == `"fail"` {
			return errors.New("boom")
		}
		return nil
	})
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{`"ok"`, `"fail"`}, []string{<-received, <-received})
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		fake.Snapshot(func(f *daprmqtest.Fake) {
			assert.Len(c, f.Acked, 1)
			assert.Len(c, f.Nacked, 1)
		})
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSubscribeDeadLettersAnUndecodableItem(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, nil)
	fake.Enqueue("orders", json.RawMessage(`"not an envelope"`))

	err := ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		t.Error("handler must not be called")
		return nil
	})
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		fake.Snapshot(func(f *daprmqtest.Fake) { assert.Len(c, f.DeadLetters, 1) })
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSingleConcurrencyHandlesOneMessageAtATime(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, map[string]string{"concurrencyMode": "single"})
	for i := range 5 {
		raw, _ := json.Marshal(wrap(fmt.Appendf(nil, "%d", i), nil, nil))
		fake.Enqueue("orders", raw)
	}

	var mu sync.Mutex
	var active, maxActive int
	var order []string
	done := make(chan struct{})
	err := ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(_ context.Context, msg *pubsub.NewMessage) error {
		mu.Lock()
		active++
		maxActive = max(maxActive, active)
		order = append(order, string(msg.Data))
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		active--
		if len(order) == 5 {
			close(done)
		}
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("not all messages delivered")
	}
	assert.Equal(t, 1, maxActive)
	assert.Equal(t, []string{"0", "1", "2", "3", "4"}, order)
}

func TestLocksAreRenewedUntilSettled(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, map[string]string{"lockRenewalInterval": "1s", "lockTTL": "5s"})
	raw, _ := json.Marshal(wrap([]byte(`1`), nil, nil))
	fake.Enqueue("orders", raw)

	handled := make(chan struct{})
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		time.Sleep(2500 * time.Millisecond)
		close(handled)
		return nil
	}))
	<-handled

	var renewals int
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		fake.Snapshot(func(f *daprmqtest.Fake) {
			assert.Len(c, f.Acked, 1)
			renewals = len(f.Extends)
		})
	}, 5*time.Second, 10*time.Millisecond)
	fake.Snapshot(func(f *daprmqtest.Fake) {
		require.NotEmpty(t, f.Extends)
		for _, call := range f.Extends {
			// Each tick catches the lock back up to about lockTTL ahead: one interval (1s), or 2s
			// when the whole-second expiry rounding has cost it part of a second.
			assert.Equal(t, "lock-1", call.LockID)
			assert.InDelta(t, 1.5, call.AdditionalTTLSeconds, 0.5)
		}
	})

	time.Sleep(1500 * time.Millisecond)
	fake.Snapshot(func(f *daprmqtest.Fake) { assert.Len(t, f.Extends, renewals, "a settled lock is not renewed") })
}

func TestALostLockIsNotRenewedAgain(t *testing.T) {
	fake := daprmqtest.New()
	fake.ExtendsLost = true
	ps := newTestComponent(t, fake, map[string]string{"lockRenewalInterval": "1s", "lockTTL": "5s"})
	raw, _ := json.Marshal(wrap([]byte(`1`), nil, nil))
	fake.Enqueue("orders", raw)

	handled := make(chan struct{})
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		time.Sleep(3500 * time.Millisecond)
		close(handled)
		return nil
	}))
	<-handled

	fake.Snapshot(func(f *daprmqtest.Fake) { assert.Len(t, f.Extends, 1) })
}

func TestZeroLockRenewalIntervalNeverRenews(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, map[string]string{"lockRenewalInterval": "0s"})
	raw, _ := json.Marshal(wrap([]byte(`1`), nil, nil))
	fake.Enqueue("orders", raw)

	handled := make(chan struct{})
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		time.Sleep(1500 * time.Millisecond)
		close(handled)
		return nil
	}))
	<-handled

	fake.Snapshot(func(f *daprmqtest.Fake) { assert.Empty(t, f.Extends) })
}

func TestCancellingTheSubscribeContextStopsPolling(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, nil)

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, ps.Subscribe(ctx, pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error { return nil }))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Positive(c, fake.TotalDequeues())
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)
	before := fake.TotalDequeues()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, before, fake.TotalDequeues())
}

func TestCloseWaitsForSubscriptionsAndRejectsFurtherCalls(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake, nil)
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error { return nil }))

	require.NoError(t, ps.Close())

	before := fake.TotalDequeues()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, before, fake.TotalDequeues())
	require.Error(t, ps.Publish(t.Context(), &pubsub.PublishRequest{Topic: "orders", Data: []byte(`1`)}))
	require.Error(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error { return nil }))
}

func TestRenewalExtensionKeepsALockAboutLockTTLAhead(t *testing.T) {
	now := time.Unix(1000, 0)
	ttl := 30 * time.Second

	// On time: the lock is 20s ahead, so it gets the 10s it lost.
	assert.Equal(t, 10*time.Second, renewalExtension(now, now.Add(20*time.Second), ttl))
	// A late tick: only 5s left, so it catches up by 25s rather than adding one interval.
	assert.Equal(t, 25*time.Second, renewalExtension(now, now.Add(5*time.Second), ttl))
	// Never less than a second, the server's smallest extension.
	assert.Equal(t, time.Second, renewalExtension(now, now.Add(30*time.Second), ttl))
}
