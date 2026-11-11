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

package topics

import (
	"context"
	"encoding/json"
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

func props(url string) map[string]string {
	return map[string]string{"httpEndpoint": url, "grpcEndpoint": "localhost:1", "consumerID": "app", "pollInterval": "10ms", "receiveMode": "poll"}
}

func newTestComponent(t *testing.T, fake *daprmqtest.Fake) pubsub.PubSub {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	ps := NewDaprMQTopics(logger.NewLogger("test"))
	require.NoError(t, ps.Init(t.Context(), pubsub.Metadata{Base: metadata.Base{Properties: props(srv.URL)}}))
	t.Cleanup(func() { _ = ps.Close() })
	return ps
}

func receiveOne(t *testing.T, ps pubsub.PubSub) {
	t.Helper()
	received := make(chan struct{}, 1)
	require.NoError(t, ps.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: "orders"}, func(context.Context, *pubsub.NewMessage) error {
		received <- struct{}{}
		return nil
	}))
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("no message delivered")
	}
}

func TestPublishGoesToTheTopic(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake)

	require.NoError(t, ps.Publish(t.Context(), &pubsub.PublishRequest{Topic: "orders", Data: []byte(`1`)}))

	fake.Snapshot(func(f *daprmqtest.Fake) {
		assert.Len(t, f.Published["orders"], 1)
		assert.Empty(t, f.Queues)
	})
}

func TestSubscribeRegistersTheConsumerIDAndConsumesItsQueue(t *testing.T) {
	fake := daprmqtest.New()
	ps := newTestComponent(t, fake)
	fake.Enqueue("orders-sub-app", json.RawMessage(`{"data":1}`))

	receiveOne(t, ps)

	fake.Snapshot(func(f *daprmqtest.Fake) { assert.Equal(t, 1, f.Subscribers["orders/app"]) })
}

func TestSubscribeToAnExistingSubscriptionConsumesItsQueue(t *testing.T) {
	fake := daprmqtest.New()
	fake.Subscribers["orders/app"] = 1
	ps := newTestComponent(t, fake)
	fake.Enqueue("orders-sub-app", json.RawMessage(`{"data":1}`))

	receiveOne(t, ps)
}

func TestInitRequiresAConsumerID(t *testing.T) {
	p := props("http://localhost:1")
	delete(p, "consumerID")

	err := NewDaprMQTopics(logger.NewLogger("test")).Init(t.Context(), pubsub.Metadata{Base: metadata.Base{Properties: p}})

	require.Error(t, err)
}

func TestGetComponentMetadataListsTheFields(t *testing.T) {
	md := (&daprMQTopics{}).GetComponentMetadata()

	assert.Contains(t, md, "httpEndpoint")
	assert.Contains(t, md, "lockRenewalInterval")
	assert.True(t, md["consumerID"].Ignored)
}
