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

package queues

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/component/daprmq/daprmqtest"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func newTestComponent(t *testing.T, fake *daprmqtest.Fake) pubsub.PubSub {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	ps := NewDaprMQQueues(logger.NewLogger("test"))
	// No consumerID: on a queue, every subscriber competes for the same messages.
	props := map[string]string{"httpEndpoint": srv.URL, "grpcEndpoint": "localhost:1", "pollInterval": "10ms", "receiveMode": "poll"}
	require.NoError(t, ps.Init(t.Context(), pubsub.Metadata{Base: metadata.Base{Properties: props}}))
	t.Cleanup(func() { _ = ps.Close() })
	return ps
}

func TestPublishedMessagesAreConsumedFromTheQueueOfTheTopicsName(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake)

	require.NoError(t, ps.Publish(t.Context(), &pubsub.PublishRequest{
		Topic: "orders", Data: []byte(`{"id":1}`), Metadata: map[string]string{"priority": "0"},
	}))
	fake.Snapshot(func(f *daprmqtest.Fake) {
		assert.Len(t, f.Queues["orders"], 1)
		assert.Equal(t, []int{0}, f.Priorities)
		assert.Empty(t, f.Published)
	})

	received := make(chan string, 1)
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(_ context.Context, msg *pubsub.NewMessage) error {
		received <- string(msg.Data)
		return nil
	}))
	select {
	case data := <-received:
		assert.JSONEq(t, `{"id":1}`, data)
	case <-time.After(5 * time.Second):
		t.Fatal("no message delivered")
	}
	fake.Snapshot(func(f *daprmqtest.Fake) { assert.Empty(t, f.Subscribers, "a queue has no topic subscription") })
}

func TestGetComponentMetadataHasNoConsumerID(t *testing.T) {
	md := (&daprMQQueues{}).GetComponentMetadata()

	assert.Contains(t, md, "httpEndpoint")
	assert.NotContains(t, md, "consumerID")
}
