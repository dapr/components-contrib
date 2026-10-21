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

package snssqs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// consumerStub stands in for consumeSubscription: it records every context the
// controller starts SQS consumption with and blocks on it, so tests can assert
// how many times the poller (re)started and whether a given run was stopped.
type consumerStub struct {
	mu    sync.Mutex
	ctxs  []context.Context
	calls int
}

func (c *consumerStub) run(ctx context.Context, _ *sqsQueueInfo, _ *sqsQueueInfo) {
	c.mu.Lock()
	c.calls++
	c.ctxs = append(c.ctxs, ctx)
	c.mu.Unlock()
	<-ctx.Done()
}

func (c *consumerStub) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *consumerStub) lastCtx() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ctxs) == 0 {
		return nil
	}
	return c.ctxs[len(c.ctxs)-1]
}

func newTestSubscriptionManager(t *testing.T) (*SubscriptionManager, *consumerStub) {
	t.Helper()
	sm := NewSubscriptionMgmt(logger.NewLogger("test")).(*SubscriptionManager)
	t.Cleanup(sm.Close)

	stub := &consumerStub{}
	sm.Init(nil, nil, stub.run)

	return sm, stub
}

func newTestHandler(topic string) *SubscriptionTopicHandler {
	return &SubscriptionTopicHandler{
		topic:        topic,
		requestTopic: topic,
		handler:      func(context.Context, *pubsub.NewMessage) error { return nil },
		ctx:          context.Background(),
	}
}

func waitForHandler(t *testing.T, sm *SubscriptionManager, topic string, want *SubscriptionTopicHandler) {
	t.Helper()
	require.Eventually(t, func() bool {
		got, ok := sm.GetSubscriptionTopicHandler(topic)
		return ok && got == want
	}, 2*time.Second, time.Millisecond, "topic %q never resolved to the expected handler", topic)
}

func waitForHandlerAbsent(t *testing.T, sm *SubscriptionManager, topic string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, ok := sm.GetSubscriptionTopicHandler(topic)
		return !ok
	}, 2*time.Second, time.Millisecond, "topic %q was never removed", topic)
}

// waitForPollerStart blocks until the stub consumer has actually been invoked at least n times.
// The controller spawns it in its own goroutine when the handler map transitions from empty to
// non-empty, independently of when that transition becomes visible via GetSubscriptionTopicHandler,
// so callers must not assume it has already run just because a Subscribe has been observed.
func waitForPollerStart(t *testing.T, stub *consumerStub, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return stub.callCount() >= n
	}, 2*time.Second, time.Millisecond, "poller was not started %d time(s)", n)
}

// drainChangeQueue blocks until every change event enqueued before this call has been fully
// applied by queueConsumerController, by round-tripping a disposable subscription through it.
// Because topicsChangeCh is unbuffered and has a single serialized consumer, the barrier
// handler can only become visible (and then absent) after everything enqueued ahead of it in
// program order has finished processing.
func drainChangeQueue(t *testing.T, sm *SubscriptionManager) {
	t.Helper()
	barrier := newTestHandler("__drain_barrier__")
	sm.Subscribe(barrier)
	waitForHandler(t, sm, barrier.topic, barrier)
	sm.unsubscribe(barrier)
	waitForHandlerAbsent(t, sm, barrier.topic)
}

// Reproduces dapr/dapr#10766: H1 is registered for topic T, H2 replaces H1 for the same topic,
// and only then does H1's delayed unsubscribe get processed. H2 must remain registered.
func Test_Unsubscribe_StaleHandlerDoesNotDeleteNewerSubscription(t *testing.T) {
	t.Parallel()
	sm, _ := newTestSubscriptionManager(t)

	h1 := newTestHandler("T")
	sm.Subscribe(h1)
	waitForHandler(t, sm, "T", h1)

	h2 := newTestHandler("T")
	sm.Subscribe(h2)
	waitForHandler(t, sm, "T", h2)

	// H1's unsubscribe is delayed and only gets processed now, after H2 already took over.
	sm.unsubscribe(h1)
	drainChangeQueue(t, sm)

	got, ok := sm.GetSubscriptionTopicHandler("T")
	require.True(t, ok, "topic T must still have a handler")
	require.Same(t, h2, got, "the newer handler H2 must not be removed by H1's stale unsubscribe")
}

// A genuine (non-stale) unsubscribe must still remove its matching handler.
func Test_Unsubscribe_RemovesMatchingHandler(t *testing.T) {
	t.Parallel()
	sm, _ := newTestSubscriptionManager(t)

	h1 := newTestHandler("T")
	sm.Subscribe(h1)
	waitForHandler(t, sm, "T", h1)

	sm.unsubscribe(h1)
	waitForHandlerAbsent(t, sm, "T")
}

// Unsubscribing the same, already-removed handler a second time must be a harmless no-op.
func Test_Unsubscribe_IsIdempotent(t *testing.T) {
	t.Parallel()
	sm, _ := newTestSubscriptionManager(t)

	h1 := newTestHandler("T")
	sm.Subscribe(h1)
	waitForHandler(t, sm, "T", h1)

	sm.unsubscribe(h1)
	waitForHandlerAbsent(t, sm, "T")

	require.NotPanics(t, func() { sm.unsubscribe(h1) })
	drainChangeQueue(t, sm)

	_, ok := sm.GetSubscriptionTopicHandler("T")
	require.False(t, ok, "topic T must remain absent after a duplicate unsubscribe")
}

// A stale unsubscribe for a replaced handler must not stop the shared SQS poller, and must not
// affect sibling topics' handlers on the same queue.
func Test_Unsubscribe_StaleHandlerDoesNotStopPollerOrSiblings(t *testing.T) {
	t.Parallel()
	sm, stub := newTestSubscriptionManager(t)

	h1 := newTestHandler("T1")
	sm.Subscribe(h1)
	waitForHandler(t, sm, "T1", h1) // first subscription on this manager starts the poller

	h2 := newTestHandler("T2")
	sm.Subscribe(h2)
	waitForHandler(t, sm, "T2", h2)

	h3 := newTestHandler("T3")
	sm.Subscribe(h3)
	waitForHandler(t, sm, "T3", h3)

	waitForPollerStart(t, stub, 1)
	require.Equal(t, 1, stub.callCount(), "the poller must only be started once for the shared queue")
	pollerCtx := stub.lastCtx()
	require.NotNil(t, pollerCtx)
	require.NoError(t, pollerCtx.Err())

	// T1's handler is replaced, then T1's old handler's delayed unsubscribe arrives late.
	h1b := newTestHandler("T1")
	sm.Subscribe(h1b)
	waitForHandler(t, sm, "T1", h1b)

	sm.unsubscribe(h1)
	drainChangeQueue(t, sm)

	got1, ok := sm.GetSubscriptionTopicHandler("T1")
	require.True(t, ok)
	require.Same(t, h1b, got1, "T1 must still resolve to its newer handler")

	got2, ok := sm.GetSubscriptionTopicHandler("T2")
	require.True(t, ok)
	require.Same(t, h2, got2, "sibling topic T2 must be unaffected")

	got3, ok := sm.GetSubscriptionTopicHandler("T3")
	require.True(t, ok)
	require.Same(t, h3, got3, "sibling topic T3 must be unaffected")

	require.Equal(t, 1, stub.callCount(), "the stale unsubscribe must not restart the poller")
	require.NoError(t, pollerCtx.Err(), "the stale unsubscribe must not stop the poller")
}

// Removing the actual final handler for a queue must still stop SQS consumption.
func Test_Unsubscribe_LastHandlerRemoved_StopsPoller(t *testing.T) {
	t.Parallel()
	sm, stub := newTestSubscriptionManager(t)

	h1 := newTestHandler("T")
	sm.Subscribe(h1)
	waitForHandler(t, sm, "T", h1)
	waitForPollerStart(t, stub, 1)

	pollerCtx := stub.lastCtx()
	require.NotNil(t, pollerCtx)
	require.NoError(t, pollerCtx.Err())

	sm.unsubscribe(h1)
	waitForHandlerAbsent(t, sm, "T")

	require.Eventually(t, func() bool {
		return pollerCtx.Err() != nil
	}, 2*time.Second, time.Millisecond, "poller must stop once the last handler is removed")
}
