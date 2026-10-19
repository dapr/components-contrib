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

package amqp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	backoff "github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

func TestParseSubscriptionOptions(t *testing.T) {
	tests := []struct {
		name string
		md   map[string]string
		want subscriptionOptions
	}{
		{
			name: "no options",
			md:   nil,
			want: subscriptionOptions{},
		},
		{
			name: "runtime keys are ignored",
			md:   map[string]string{"rawPayload": "true"},
			want: subscriptionOptions{},
		},
		{
			name: "all options",
			md: map[string]string{
				"subscriptionName": "billing",
				"durable":          "true",
				"shared":           "true",
				"selector":         "type = 'order'",
			},
			want: subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true, Selector: "type = 'order'"},
		},
		{
			name: "keys are matched without regard to case",
			md:   map[string]string{"SUBSCRIPTIONNAME": "billing", "Durable": "yes"},
			want: subscriptionOptions{SubscriptionName: "billing", Durable: true},
		},
		{
			name: "surrounding spaces are trimmed",
			md:   map[string]string{"subscriptionName": " billing ", "selector": " a = 1 "},
			want: subscriptionOptions{SubscriptionName: "billing", Selector: "a = 1"},
		},
		{
			name: "a falsy flag stays off",
			md:   map[string]string{"subscriptionName": "billing", "durable": "false", "shared": "no"},
			want: subscriptionOptions{SubscriptionName: "billing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSubscriptionOptions(tt.md)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSubscriptionOptionsValidate(t *testing.T) {
	withConsumer := &metadata{ConsumerID: "orders-service"}

	tests := []struct {
		name    string
		opts    subscriptionOptions
		md      *metadata
		topic   string
		wantErr string
	}{
		{
			name:  "no options",
			md:    &metadata{},
			topic: "orders",
		},
		{
			name:  "a selector alone needs no name",
			opts:  subscriptionOptions{Selector: "a = 1"},
			md:    &metadata{},
			topic: "orders",
		},
		{
			name:    "durable needs a name",
			opts:    subscriptionOptions{Durable: true},
			md:      withConsumer,
			topic:   "orders",
			wantErr: "need a subscriptionName",
		},
		{
			name:    "shared needs a name",
			opts:    subscriptionOptions{Shared: true},
			md:      withConsumer,
			topic:   "orders",
			wantErr: "need a subscriptionName",
		},
		{
			name:  "named, durable and shared",
			opts:  subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true},
			md:    withConsumer,
			topic: "orders",
		},
		{
			name:    "the link name separator is reserved",
			opts:    subscriptionOptions{SubscriptionName: "billing|2"},
			md:      withConsumer,
			topic:   "orders",
			wantErr: "must not contain",
		},
		{
			name:    "a queue has no subscription to name",
			opts:    subscriptionOptions{SubscriptionName: "billing"},
			md:      withConsumer,
			topic:   "queue:orders",
			wantErr: "cannot be used with the queue",
		},
		{
			name:    "a topic with the queue prefix is a queue",
			opts:    subscriptionOptions{SubscriptionName: "billing"},
			md:      &metadata{ConsumerID: "orders-service", QueueAddressPrefix: "queue://"},
			topic:   "queue://orders",
			wantErr: "cannot be used with the queue",
		},
		{
			name:    "a named subscription needs a container ID",
			opts:    subscriptionOptions{SubscriptionName: "billing"},
			md:      &metadata{},
			topic:   "orders",
			wantErr: "needs a container ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.validate(tt.md, tt.topic)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestReceiverOptions(t *testing.T) {
	t.Run("no options keep the broker defaults", func(t *testing.T) {
		assert.Nil(t, subscriptionOptions{}.receiverOptions())
	})

	t.Run("a selector alone only sets the filter", func(t *testing.T) {
		opts := subscriptionOptions{Selector: "a = 1"}.receiverOptions()
		require.NotNil(t, opts)
		assert.Len(t, opts.Filters, 1)
		assert.Empty(t, opts.Name)
		assert.Empty(t, opts.SourceCapabilities)
		assert.Equal(t, amqp.DurabilityNone, opts.SourceDurability)
	})

	t.Run("durable and shared set the source, not the target", func(t *testing.T) {
		opts := subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true, Selector: "a = 1"}.receiverOptions()
		require.NotNil(t, opts)
		assert.Equal(t, "billing", opts.Name)
		assert.Equal(t, []string{capabilityTopic, capabilityShared}, opts.SourceCapabilities)
		assert.Equal(t, amqp.DurabilityUnsettledState, opts.SourceDurability)
		assert.Equal(t, amqp.ExpiryPolicyNever, opts.SourceExpiryPolicy)
		assert.Len(t, opts.Filters, 1)

		// The un-prefixed fields describe the target terminus.
		assert.Equal(t, amqp.DurabilityNone, opts.Durability)
		assert.Empty(t, opts.ExpiryPolicy)
		assert.Empty(t, opts.Capabilities)
	})

	t.Run("a named subscription that is not durable is volatile", func(t *testing.T) {
		opts := subscriptionOptions{SubscriptionName: "billing"}.receiverOptions()
		require.NotNil(t, opts)
		assert.Equal(t, []string{capabilityTopic}, opts.SourceCapabilities)
		assert.Equal(t, amqp.DurabilityNone, opts.SourceDurability)
		assert.Empty(t, opts.SourceExpiryPolicy)
		assert.Empty(t, opts.Filters)
	})
}

func TestContainerID(t *testing.T) {
	assert.Equal(t, "configured", (&metadata{ContainerID: "configured", ConsumerID: "app"}).containerID())
	assert.Equal(t, "app", (&metadata{ConsumerID: "app"}).containerID())
	assert.Empty(t, (&metadata{}).containerID())
}

func TestParseMetadataNamedSubscriptionSettings(t *testing.T) {
	props := getFakeProperties()
	props["containerID"] = "orders-service"
	props["durableMessages"] = "true"

	m, err := parseAMQPMetaData(pubsub.Metadata{Base: mdata.Base{Properties: props}}, logger.NewLogger("test"), "", "")
	require.NoError(t, err)
	assert.Equal(t, "orders-service", m.ContainerID)
	assert.Equal(t, "client", m.ConsumerID)
	assert.True(t, m.DurableMessages)
}

func TestClaimSubscriptionName(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)

	require.NoError(t, a.claimSubscriptionName("billing"))
	require.NoError(t, a.claimSubscriptionName("audit"))

	err := a.claimSubscriptionName("billing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already in use")

	a.releaseSubscriptionName("billing")
	require.NoError(t, a.claimSubscriptionName("billing"))
}

func TestNewMessageDurable(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)

	tests := []struct {
		name        string
		durable     bool
		ttl         string
		wantHeader  bool
		wantDurable bool
		wantTTL     time.Duration
	}{
		{name: "no header by default"},
		{name: "durable", durable: true, wantHeader: true, wantDurable: true},
		{name: "ttl only", ttl: "30", wantHeader: true, wantTTL: 30 * time.Second},
		{name: "durable with ttl", durable: true, ttl: "30", wantHeader: true, wantDurable: true, wantTTL: 30 * time.Second},
		{name: "durable with an invalid ttl", durable: true, ttl: "soon", wantHeader: true, wantDurable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pubsub.PublishRequest{Data: []byte("hello"), Metadata: map[string]string{}}
			if tt.ttl != "" {
				req.Metadata["ttlInSeconds"] = tt.ttl
			}

			m := a.newMessage(&metadata{DurableMessages: tt.durable}, req, "orders")
			if !tt.wantHeader {
				assert.Nil(t, m.Header)
				return
			}

			require.NotNil(t, m.Header)
			assert.Equal(t, tt.wantDurable, m.Header.Durable)
			assert.Equal(t, tt.wantTTL, m.Header.TTL)
		})
	}
}

// TestSubscribeRejectsInvalidOptions verifies that invalid subscription options
// fail Subscribe before it touches the broker, and leave no name claimed.
func TestSubscribeRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		md      map[string]string
		wantErr string
	}{
		{
			name:    "durable without a name",
			topic:   "orders",
			md:      map[string]string{"durable": "true"},
			wantErr: "need a subscriptionName",
		},
		{
			name:    "named subscription on a queue",
			topic:   "queue:orders",
			md:      map[string]string{"subscriptionName": "billing"},
			wantErr: "cannot be used with the queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
			a.metadata = &metadata{URL: "amqp://127.0.0.1:1", ConsumerID: "orders-service"}

			err := a.Subscribe(context.Background(), pubsub.SubscribeRequest{Topic: tt.topic, Metadata: tt.md}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Empty(t, a.names)
		})
	}
}

// TestSubscribeReleasesTheNameWhenAttachFails verifies that a named
// subscription which cannot reach the broker gives its name back, so that the
// runtime can subscribe with it again.
func TestSubscribeReleasesTheNameWhenAttachFails(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	a.metadata = &metadata{URL: "amqp://127.0.0.1:1", ConsumerID: "orders-service"}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := a.Subscribe(ctx, pubsub.SubscribeRequest{
		Topic:    "orders",
		Metadata: map[string]string{"subscriptionName": "billing", "durable": "true", "shared": "true"},
	}, nil)
	require.Error(t, err)
	assert.Empty(t, a.names)
}

func TestCheckSelector(t *testing.T) {
	require.NoError(t, checkSelector(subscriptionOptions{}, nil, "orders"), "no selector, nothing to check")
	require.NoError(t, checkSelector(subscriptionOptions{Selector: "a = 1"}, "a = 1", "orders"))

	err := checkSelector(subscriptionOptions{Selector: "a = 1"}, nil, "orders")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without the selector")
}

func TestReattachDelay(t *testing.T) {
	refused := &backoff.ExponentialBackOff{
		InitialInterval:     2 * time.Second,
		Multiplier:          2,
		MaxInterval:         time.Minute,
		RandomizationFactor: 0,
		Clock:               backoff.SystemClock,
	}
	refused.Reset()

	assert.Equal(t, reattachFloor, reattachDelay(false, false, refused), "a link of the shared connection keeps the floor")

	// A named subscription that keeps failing before it delivers backs off.
	assert.Equal(t, 2*time.Second, reattachDelay(true, false, refused))
	assert.Equal(t, 4*time.Second, reattachDelay(true, false, refused))
	assert.Equal(t, 8*time.Second, reattachDelay(true, false, refused))

	// One delivered message resets it.
	assert.Equal(t, reattachFloor, reattachDelay(true, true, refused))
	assert.Equal(t, 2*time.Second, reattachDelay(true, false, refused))

	// A backoff shorter than the floor still waits for the floor.
	assert.Equal(t, reattachFloor, reattachDelay(true, false, backoff.NewConstantBackOff(time.Millisecond)))
}

func TestAttacher(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	md := &metadata{ConsumerID: "orders-service"}

	t.Run("an unnamed subscription claims nothing", func(t *testing.T) {
		attach, release, err := a.attacher(md, "orders", subscriptionOptions{Selector: "a = 1"})
		require.NoError(t, err)
		require.NotNil(t, attach)
		release()
		assert.Empty(t, a.names)
	})

	t.Run("a named subscription claims its name until released", func(t *testing.T) {
		sub := subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true}

		_, release, err := a.attacher(md, "orders", sub)
		require.NoError(t, err)

		_, _, err = a.attacher(md, "orders", sub)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already in use")

		release()
		_, release, err = a.attacher(md, "orders", sub)
		require.NoError(t, err)
		release()
	})
}

// fakeReceiver hands out queued messages, then returns err, or blocks until
// the context ends when err is nil.
type fakeReceiver struct {
	mu       sync.Mutex
	messages []*amqp.Message
	err      error
	accepted []string
	modified []string
}

func (f *fakeReceiver) Receive(ctx context.Context, _ *amqp.ReceiveOptions) (*amqp.Message, error) {
	f.mu.Lock()
	if len(f.messages) > 0 {
		msg := f.messages[0]
		f.messages = f.messages[1:]
		f.mu.Unlock()

		return msg, nil
	}
	err := f.err
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}

	<-ctx.Done()

	return nil, ctx.Err()
}

func (f *fakeReceiver) AcceptMessage(_ context.Context, msg *amqp.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.accepted = append(f.accepted, string(msg.GetData()))

	return nil
}

func (f *fakeReceiver) ModifyMessage(_ context.Context, msg *amqp.Message, opts *amqp.ModifyMessageOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if opts == nil || !opts.DeliveryFailed {
		return errors.New("a failed message must be returned with DeliveryFailed")
	}
	f.modified = append(f.modified, string(msg.GetData()))

	return nil
}

func TestDeliver(t *testing.T) {
	handler := func(_ context.Context, msg *pubsub.NewMessage) error {
		if string(msg.Data) == "bad" {
			return errors.New("handler failed")
		}

		return nil
	}

	t.Run("accepts handled messages and returns failed ones for redelivery", func(t *testing.T) {
		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		receiver := &fakeReceiver{
			messages: []*amqp.Message{amqp.NewMessage([]byte("one")), amqp.NewMessage([]byte("bad")), amqp.NewMessage([]byte("two"))},
			err:      errors.New("link detached"),
		}
		var released atomic.Bool
		link := &subscriptionLink{receiver: receiver, release: func() { released.Store(true) }}

		received := a.deliver(t.Context(), link, handler, "orders", "orders")

		assert.True(t, received)
		assert.True(t, released.Load(), "the link is released when delivery ends")
		assert.Equal(t, []string{"one", "two"}, receiver.accepted)
		assert.Equal(t, []string{"bad"}, receiver.modified)
	})

	t.Run("reports a link that failed before it delivered anything", func(t *testing.T) {
		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		receiver := &fakeReceiver{err: &amqp.Error{Condition: "amqp:illegal-state"}}
		link := &subscriptionLink{receiver: receiver, release: func() {}}

		assert.False(t, a.deliver(t.Context(), link, handler, "orders", "orders"))
	})
}

// TestRunSubscriptionReleasesWhatItClaimed verifies that a subscription that
// ends frees its link and its name, so that the runtime can subscribe with the
// name again, for example after a hot reload.
func TestRunSubscriptionReleasesWhatItClaimed(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	a.backOffConfig = retry.DefaultConfig()

	sub := subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true}
	_, releaseName, err := a.attacher(&metadata{ConsumerID: "orders-service"}, "orders", sub)
	require.NoError(t, err)

	var released atomic.Bool
	link := &subscriptionLink{receiver: &fakeReceiver{}, release: func() { released.Store(true) }, named: true}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.runSubscription(ctx, cancel, link, nil, releaseName, nil, "orders", "orders")
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription did not end")
	}

	assert.True(t, released.Load())
	require.NoError(t, a.claimSubscriptionName("billing"), "the name is free again")
}
