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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func newTestHandler(topic string) *SubscriptionTopicHandler {
	return &SubscriptionTopicHandler{
		topic:        topic,
		requestTopic: topic,
		handler:      func(context.Context, *pubsub.NewMessage) error { return nil },
		ctx:          context.Background(),
	}
}

// Once shutdown has started, a Subscribe that only reaches its topicsChangeCh send afterward
// must not block forever trying to deliver an event nobody will read, and the manager-wide wg
// that Close waits on (via Subscribe's wg.Add/wg.Done) must still complete.
//
// Init is deliberately never called here, so no controller goroutine ever exists to receive on
// topicsChangeCh: without the shutdown-aware select, the send below would block forever, making
// this a direct, deterministic reproduction of the hang rather than a race against real timing.
func Test_Subscribe_AfterShutdownStarted_DoesNotBlock(t *testing.T) {
	t.Parallel()
	sm := NewSubscriptionMgmt(logger.NewLogger("test")).(*SubscriptionManager)

	sm.Close()

	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		sm.createSubscribeListener(newTestHandler("T"))
	}()

	waited := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(waited)
	}()

	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("a Subscribe starting after shutdown began blocked the manager's wg forever")
	}
}

// A Subscribe that starts and delivers its event before shutdown begins must be unaffected by
// the shutdown-aware select added to createSubscribeListener.
func Test_Subscribe_BeforeShutdown_StillDeliversEvent(t *testing.T) {
	t.Parallel()
	sm := NewSubscriptionMgmt(logger.NewLogger("test")).(*SubscriptionManager)
	sm.Init(nil, nil, func(ctx context.Context, _ *sqsQueueInfo, _ *sqsQueueInfo) {
		<-ctx.Done()
	})
	t.Cleanup(sm.Close)

	h := newTestHandler("T")
	sm.Subscribe(h)

	require.Eventually(t, func() bool {
		got, ok := sm.GetSubscriptionTopicHandler("T")
		return ok && got == h
	}, 2*time.Second, time.Millisecond, "topic T never resolved to the expected handler")
}
