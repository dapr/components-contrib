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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
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
				"subscriptionName":    "billing",
				"durable":             "true",
				"shared":              "true",
				"selector":            "type = 'order'",
				"allowSelectorChange": "true",
			},
			want: subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true, Selector: "type = 'order'", AllowSelectorChange: true},
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

		received, err := a.deliver(t.Context(), link, handler, "orders", "orders")

		assert.True(t, received)
		assert.Equal(t, receiver.err, err)
		assert.True(t, released.Load(), "the link is released when delivery ends")
		assert.Equal(t, []string{"one", "two"}, receiver.accepted)
		assert.Equal(t, []string{"bad"}, receiver.modified)
	})

	t.Run("reports a link that failed before it delivered anything", func(t *testing.T) {
		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		receiver := &fakeReceiver{err: &amqp.Error{Condition: "amqp:illegal-state"}}
		link := &subscriptionLink{receiver: receiver, release: func() {}}

		received, err := a.deliver(t.Context(), link, handler, "orders", "orders")

		assert.False(t, received)
		assert.Equal(t, receiver.err, err)
	})
}

// errSelectorRefused is how ActiveMQ Artemis refuses a durable subscription whose
// selector changed while consumers are attached. It arrives from the first
// Receive, as the remote error of a detached link.
var errSelectorRefused = &amqp.LinkError{RemoteErr: &amqp.Error{
	Condition:   amqp.ErrCondIllegalState,
	Description: "Unable to recreate subscription, consumers already exist",
}}

func TestSelectorChange(t *testing.T) {
	sub := subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true, Selector: "a = 1"}

	t.Run("no error", func(t *testing.T) {
		require.NoError(t, sub.selectorChange(nil, "orders"))
	})

	t.Run("the refusal from the first receive ends the subscription", func(t *testing.T) {
		err := sub.selectorChange(errSelectorRefused, "orders")

		var changed *selectorChangeError
		require.ErrorAs(t, err, &changed)
		require.ErrorIs(t, err, errSelectorRefused)
		assert.Contains(t, err.Error(), `"billing"`)
		assert.Contains(t, err.Error(), `"a = 1"`)
		assert.Contains(t, err.Error(), "allowSelectorChange")
		assert.Contains(t, err.Error(), "consumers already exist")
	})

	t.Run("the refusal from the attach ends the subscription", func(t *testing.T) {
		var changed *selectorChangeError
		require.ErrorAs(t, sub.selectorChange(errSelectorRefused.RemoteErr, "orders"), &changed)
	})

	t.Run("another refusal is re-attached", func(t *testing.T) {
		limit := &amqp.LinkError{RemoteErr: &amqp.Error{
			Condition:   amqp.ErrCondIllegalState,
			Description: "Maximum Consumer Limit Reached on Queue:orders-service.billing",
		}}
		assert.Equal(t, limit, sub.selectorChange(limit, "orders"))
	})

	t.Run("a link that was only detached is re-attached", func(t *testing.T) {
		detached := &amqp.LinkError{}
		assert.Equal(t, detached, sub.selectorChange(detached, "orders"))
	})

	t.Run("allowSelectorChange keeps the re-attach", func(t *testing.T) {
		allowed := sub
		allowed.AllowSelectorChange = true
		assert.Equal(t, errSelectorRefused, allowed.selectorChange(errSelectorRefused, "orders"))
	})
}

// TestSubscribeForeverEndsOnSelectorChange verifies that a durable subscription
// the broker refuses because its selector changed ends, rather than re-attach
// until the broker recreates it, and that allowSelectorChange keeps the
// re-attach.
func TestSubscribeForeverEndsOnSelectorChange(t *testing.T) {
	run := func(t *testing.T, sub subscriptionOptions) (attached bool) {
		t.Helper()

		a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
		a.backOffConfig = retry.Config{Policy: retry.PolicyConstant, Duration: time.Millisecond}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		link := &subscriptionLink{receiver: &fakeReceiver{err: errSelectorRefused}, release: func() {}, named: true, sub: sub}
		attach := func(context.Context) (*subscriptionLink, error) {
			attached = true
			cancel()

			return nil, errors.New("ended by the test")
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			a.subscribeForever(ctx, link, attach, nil, "orders", "orders")
		}()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the subscription did not end")
		}

		return attached
	}

	sub := subscriptionOptions{SubscriptionName: "billing", Durable: true, Shared: true, Selector: "a = 1"}

	t.Run("the subscription ends", func(t *testing.T) {
		assert.False(t, run(t, sub))
	})

	t.Run("allowSelectorChange re-attaches", func(t *testing.T) {
		allowed := sub
		allowed.AllowSelectorChange = true
		assert.True(t, run(t, allowed))
	})
}

// TestReattachGivesUpOnSelectorChange verifies that a refusal the attach
// itself reports is not retried.
func TestReattachGivesUpOnSelectorChange(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	a.backOffConfig = retry.Config{Policy: retry.PolicyConstant, Duration: time.Millisecond}

	calls := 0
	attach := func(context.Context) (*subscriptionLink, error) {
		calls++

		return nil, &selectorChangeError{cause: errSelectorRefused}
	}

	_, err := a.reattach(t.Context(), attach, "orders", a.reconnectBackOff(t.Context()))

	var changed *selectorChangeError
	require.ErrorAs(t, err, &changed)
	assert.Equal(t, 1, calls)
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

// TestRefusedBackOffStartsAtTheConfiguredInterval verifies that the backoff for
// a refused subscription starts from backOffInitialInterval, not from the
// default the retry helper builds the exponential backoff with.
func TestRefusedBackOffStartsAtTheConfiguredInterval(t *testing.T) {
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	a.backOffConfig = retry.Config{
		Policy:          retry.PolicyExponential,
		InitialInterval: 30 * time.Second,
		Multiplier:      2,
		MaxInterval:     5 * time.Minute,
		MaxRetries:      -1,
	}

	b := a.refusedBackOff(t.Context())
	assert.Equal(t, 30*time.Second, b.NextBackOff())
	assert.Equal(t, 60*time.Second, b.NextBackOff())
}

// TestConnOptions verifies that a named subscription connects with the same
// TLS and SASL settings as the shared connection, mutual TLS with anonymous
// SASL included.
func TestConnOptions(t *testing.T) {
	caPEM, certPEM, keyPEM := testTLSMaterial(t)
	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)

	t.Run("mutual TLS with anonymous SASL", func(t *testing.T) {
		md := &metadata{
			URL:       "amqps://broker:5671",
			Anonymous: true,
			tlsCfg:    tlsCfg{CaCert: caPEM, ClientCert: certPEM, ClientKey: keyPEM},
		}

		opts, err := a.connOptions(md, "orders-service")
		require.NoError(t, err)
		assert.Equal(t, "orders-service", opts.ContainerID)
		require.NotNil(t, opts.TLSConfig)
		assert.Len(t, opts.TLSConfig.Certificates, 1)
		assert.NotNil(t, opts.TLSConfig.RootCAs)
		assert.NotNil(t, opts.SASLType)
	})

	t.Run("username and password over plain AMQP", func(t *testing.T) {
		md := &metadata{URL: "amqp://broker:5672", Username: "user", Password: "secret"}

		opts, err := a.connOptions(md, "orders-service")
		require.NoError(t, err)
		assert.Nil(t, opts.TLSConfig)
		assert.NotNil(t, opts.SASLType)
	})

	t.Run("the shared connection lets go-amqp generate the container ID", func(t *testing.T) {
		opts, err := a.connOptions(&metadata{URL: "amqp://broker:5672", Anonymous: true}, "")
		require.NoError(t, err)
		assert.Empty(t, opts.ContainerID)
	})
}

// TestNamedConnectionUsesMutualTLSAndAnonymousSASL dials the connection of a
// named subscription against a local TLS server that requires a client
// certificate and offers the ANONYMOUS and PLAIN SASL mechanisms. The
// connection must present the client certificate and pick ANONYMOUS, as a
// broker that only accepts mutual TLS expects.
func TestNamedConnectionUsesMutualTLSAndAnonymousSASL(t *testing.T) {
	ca, caKey, caPEM := testCA(t)
	certPEM, keyPEM := testLeaf(t, ca, caKey, "test-client", x509.ExtKeyUsageClientAuth)
	serverCertPEM, serverKeyPEM := testLeaf(t, ca, caKey, "127.0.0.1", x509.ExtKeyUsageServerAuth)

	serverCert, err := tls.X509KeyPair([]byte(serverCertPEM), []byte(serverKeyPEM))
	require.NoError(t, err)
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(ca)

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	require.NoError(t, err)
	defer listener.Close()

	type handshake struct {
		clientCN  string
		mechanism string
		err       error
	}
	result := make(chan handshake, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			result <- handshake{err: err}
			return
		}
		defer conn.Close()

		tlsConn := conn.(*tls.Conn)
		if err = tlsConn.Handshake(); err != nil {
			result <- handshake{err: err}
			return
		}

		var got handshake
		got.clientCN = tlsConn.ConnectionState().PeerCertificates[0].Subject.CommonName
		got.mechanism, got.err = serveSASLMechanisms(conn)
		result <- got
	}()

	a := NewAMQPPubsub(logger.NewLogger("test")).(*amqpPubSub)
	md := &metadata{
		URL:       "amqps://" + listener.Addr().String(),
		Anonymous: true,
		tlsCfg:    tlsCfg{CaCert: caPEM, ClientCert: certPEM, ClientKey: keyPEM},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// The fake server stops after the client picks a mechanism, so the dial
	// itself fails. Only what the client sent matters.
	go a.connectAs(ctx, md, "orders-service") //nolint:errcheck

	select {
	case got := <-result:
		require.NoError(t, got.err)
		assert.Equal(t, "test-client", got.clientCN)
		assert.Equal(t, "ANONYMOUS", got.mechanism)
	case <-ctx.Done():
		t.Fatal("the client never completed the TLS handshake and SASL init")
	}
}

// serveSASLMechanisms plays the broker side of an AMQP SASL negotiation up to
// the client's choice: it answers the SASL protocol header, offers ANONYMOUS
// and PLAIN, and returns the mechanism named in the client's sasl-init frame.
func serveSASLMechanisms(conn net.Conn) (string, error) {
	saslHeader := []byte{'A', 'M', 'Q', 'P', 3, 1, 0, 0}

	header := make([]byte, len(saslHeader))
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
	if !bytes.Equal(header, saslHeader) {
		return "", fmt.Errorf("the client sent %v, not the SASL protocol header", header)
	}

	// A sasl-mechanisms frame that offers ANONYMOUS and PLAIN.
	frame := []byte{
		0, 0, 0, 34, 2, 1, 0, 0, // size, data offset, SASL frame type, channel 0
		0x00, 0x53, 0x40, // described type sasl-mechanisms
		0xc0, 21, 1, // list8 of one field
		0xe0, 18, 2, 0xa3, // array8 of two sym8
		9, 'A', 'N', 'O', 'N', 'Y', 'M', 'O', 'U', 'S',
		5, 'P', 'L', 'A', 'I', 'N',
	}

	if _, err := conn.Write(append(saslHeader, frame...)); err != nil {
		return "", err
	}

	// The sasl-init frame names the chosen mechanism as a symbol.
	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBytes); err != nil {
		return "", err
	}
	rest := make([]byte, binary.BigEndian.Uint32(sizeBytes)-4)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return "", err
	}

	for _, mechanism := range []string{"ANONYMOUS", "PLAIN"} {
		if bytes.Contains(rest, []byte(mechanism)) {
			return mechanism, nil
		}
	}

	return "", fmt.Errorf("the sasl-init frame names no known mechanism: %x", rest)
}

// testTLSMaterial returns a CA certificate, and a client certificate and key it
// signed, all PEM encoded.
func testTLSMaterial(t *testing.T) (caPEM, certPEM, keyPEM string) {
	t.Helper()

	ca, caKey, caPEM := testCA(t)
	certPEM, keyPEM = testLeaf(t, ca, caKey, "test-client", x509.ExtKeyUsageClientAuth)

	return caPEM, certPEM, keyPEM
}

// testCA returns a self-signed CA certificate, its key, and the certificate
// PEM encoded.
func testCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return ca, key, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// testLeaf returns a certificate for name, signed by ca, and its key, both PEM
// encoded. An IP address name is also set as an IP SAN.
func testLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, name string, usage x509.ExtKeyUsage) (certPEM, keyPEM string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if ip := net.ParseIP(name); ip != nil {
		template.IPAddresses = []net.IP{ip}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}
