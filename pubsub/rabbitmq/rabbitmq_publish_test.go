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

package rabbitmq

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type publishConfirmationFunc func(context.Context) (bool, error)

func (f publishConfirmationFunc) WaitContext(ctx context.Context) (bool, error) { return f(ctx) }

type publishTestBroker struct {
	*rabbitMQInMemoryBroker
	publish      func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error)
	declare      func(bool) error
	confirm      func() error
	abort        func()
	publishes    atomic.Int32
	declarations atomic.Int32
	aborts       atomic.Int32
	closed       atomic.Bool
}

func (b *publishTestBroker) IsClosed() bool { return b.closed.Load() }

func newPublishTestBroker() *publishTestBroker {
	return &publishTestBroker{rabbitMQInMemoryBroker: newBroker()}
}

func (b *publishTestBroker) PublishWithDeferredConfirmWithContext(ctx context.Context, _, _ string, _, _ bool, msg amqp.Publishing) (rabbitMQConfirmation, error) {
	b.publishes.Add(1)
	if b.publish != nil {
		return b.publish(ctx, msg)
	}
	return nil, nil
}

func (b *publishTestBroker) ExchangeDeclare(_ string, _ string, _, _, _, _ bool, _ amqp.Table) error {
	b.declarations.Add(1)
	if b.declare != nil {
		return b.declare(false)
	}
	return nil
}

func (b *publishTestBroker) ExchangeDeclarePassive(_ string, _ string, _, _, _, _ bool, _ amqp.Table) error {
	b.declarations.Add(1)
	if b.declare != nil {
		return b.declare(true)
	}
	return nil
}

func (b *publishTestBroker) Confirm(bool) error {
	if b.confirm != nil {
		return b.confirm()
	}
	return nil
}

func (b *publishTestBroker) Abort() error {
	b.aborts.Add(1)
	if b.abort != nil {
		b.abort()
	}
	return nil
}

func newPublicPublisher(b *publishTestBroker) *rabbitMQ {
	r := NewRabbitMQ(logger.NewLogger("test")).(*rabbitMQ)
	r.metadata = &rabbitmqMetadata{ExchangeKind: fanoutExchangeKind, ReconnectWait: 0}
	r.connection, r.channel, r.connectionCount = b, b, 1
	r.connectionDial = func(context.Context, string, string, string, time.Duration, *tls.Config, bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
		return b, b, nil
	}
	return r
}

func startPublicPublish(r *rabbitMQ, ctx context.Context, topic string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- r.Publish(ctx, &pubsub.PublishRequest{Topic: topic}) }()
	return done
}

func awaitPublish(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("Publish exceeded the cancellation budget")
		return nil
	}
}

func awaitPublishGate(t *testing.T, gate <-chan struct{}) {
	t.Helper()
	select {
	case <-gate:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish did not reach the synchronized operation")
	}
}

func TestPublishPublicConfirmationOutcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		firstACK bool
		finalACK bool
	}{
		{"ack", true, true}, {"successful retry", false, true}, {"nack exhaustion", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newPublishTestBroker()
			b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
				ack := tc.finalACK
				if b.publishes.Load() == 1 {
					ack = tc.firstACK
				}
				return fakeConfirmation{acked: ack}, nil
			}
			r := newPublicPublisher(b)
			err := r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"})
			if tc.finalACK {
				require.NoError(t, err)
				expected := int32(1)
				if !tc.firstACK {
					expected = 2
				}
				require.Equal(t, expected, b.publishes.Load())
			} else {
				require.EqualError(t, err, "did not receive confirmation of publishing")
				require.Equal(t, int32(publishMaxRetries), b.publishes.Load())
			}
		})
	}
}

func TestPublishPublicCanceledBeforeStart(t *testing.T) {
	t.Parallel()
	b := newPublishTestBroker()
	r := newPublicPublisher(b)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, r.Publish(ctx, &pubsub.PublishRequest{Topic: "topic"}), context.Canceled)
	require.Zero(t, b.publishes.Load())
	require.Zero(t, b.declarations.Load())
	require.Zero(t, b.aborts.Load())
}

func TestPublishPublicCanceledDeclaration(t *testing.T) {
	t.Parallel()
	for _, passive := range []bool{false, true} {
		name := "active"
		if passive {
			name = "passive"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := newPublishTestBroker()
			r := newPublicPublisher(b)
			if passive {
				r.metadata.ExchangeDeclareMode = exchangeDeclareModePassive
			}
			entered, aborted := make(chan struct{}), make(chan struct{})
			var once sync.Once
			b.abort = func() { once.Do(func() { close(aborted) }) }
			b.declare = func(gotPassive bool) error {
				require.Equal(t, passive, gotPassive)
				close(entered)
				<-aborted
				return errors.New("transport interrupted during declaration")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := startPublicPublish(r, ctx, "topic")
			awaitPublishGate(t, entered)
			cancel()
			require.ErrorIs(t, awaitPublish(t, done), context.Canceled)
			require.Equal(t, int32(1), b.declarations.Load())
			require.Zero(t, b.publishes.Load())
		})
	}
}

func TestPublishPublicCanceledConfirmationAndWrite(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"confirmation", "confirmation closed by cancellation", "write", "write returns nil"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			b := newPublishTestBroker()
			r := newPublicPublisher(b)
			entered, aborted := make(chan struct{}), make(chan struct{})
			var once sync.Once
			b.abort = func() { once.Do(func() { close(aborted) }) }
			b.publish = func(ctx context.Context, _ amqp.Publishing) (rabbitMQConfirmation, error) {
				if stage == "write" || stage == "write returns nil" {
					close(entered)
					<-aborted
					if stage == "write returns nil" {
						return nil, nil
					}
					return nil, errors.New("transport interrupted during write")
				}
				return publishConfirmationFunc(func(ctx context.Context) (bool, error) {
					close(entered)
					if stage == "confirmation closed by cancellation" {
						<-aborted
						return false, nil
					}
					<-ctx.Done()
					return false, ctx.Err()
				}), nil
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := startPublicPublish(r, ctx, "topic")
			awaitPublishGate(t, entered)
			cancel()
			require.ErrorIs(t, awaitPublish(t, done), context.Canceled)
			require.Equal(t, int32(1), b.publishes.Load())
			require.Equal(t, int32(1), b.aborts.Load())
		})
	}
}

func TestPublishPublicNACKPreservedOnRetryCancellation(t *testing.T) {
	t.Parallel()
	for _, duringRetry := range []bool{false, true} {
		name := "retry delay"
		if duringRetry {
			name = "later in-flight attempt remains uncertain"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := newPublishTestBroker()
			r := newPublicPublisher(b)
			first, second := make(chan struct{}), make(chan struct{})
			b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
				if b.publishes.Load() == 1 {
					return publishConfirmationFunc(func(context.Context) (bool, error) { close(first); return false, nil }), nil
				}
				return publishConfirmationFunc(func(ctx context.Context) (bool, error) { close(second); <-ctx.Done(); return false, ctx.Err() }), nil
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := startPublicPublish(r, ctx, "topic")
			awaitPublishGate(t, first)
			if duringRetry {
				awaitPublishGate(t, second)
			} else {
				// Acquiring the same lock establishes that the first confirmation returned.
				require.NoError(t, r.channelMutex.Lock(t.Context()))
				r.channelMutex.Unlock()
			}
			cancel()
			require.EqualError(t, awaitPublish(t, done), "did not receive confirmation of publishing")
			expected := int32(1)
			if duringRetry {
				expected = 2
			}
			require.Equal(t, expected, b.publishes.Load())
			// The returned text preserves the first attempt's NACK. When expected is
			// two, the second publication was sent and its effect is still uncertain.
		})
	}
}

func TestPublishPublicAccessRefused(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"active declaration", "passive declaration", "publish", "reconnect"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			denied := &amqp.Error{Code: amqp.AccessRefused, Reason: "ACCESS_REFUSED - write permission denied", Server: true}
			b := newPublishTestBroker()
			r := newPublicPublisher(b)
			var dials atomic.Int32
			switch stage {
			case "active declaration", "passive declaration":
				if stage == "passive declaration" {
					r.metadata.ExchangeDeclareMode = exchangeDeclareModePassive
				}
				b.declare = func(bool) error { return denied }
			case "publish":
				b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) { return nil, denied }
			default:
				b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
					return nil, errors.New(errorChannelConnection)
				}
			}
			r.connectionDial = func(context.Context, string, string, string, time.Duration, *tls.Config, bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
				dials.Add(1)
				return nil, nil, denied
			}
			err := r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"})
			require.ErrorIs(t, err, denied)
			require.Equal(t, denied.Error(), err.Error())
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			expectedDials := int32(0)
			if stage == "reconnect" {
				expectedDials = 1
			}
			require.Equal(t, expectedDials, dials.Load())
			require.LessOrEqual(t, b.publishes.Load(), int32(1))
		})
	}
}

func TestPublishPublicReconnectFailureExhaustion(t *testing.T) {
	t.Parallel()
	b := newPublishTestBroker()
	r := newPublicPublisher(b)
	b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
		return nil, errors.New(errorChannelConnection)
	}
	failure := errors.New("dial: connection refused")
	var calls atomic.Int32
	r.connectionDial = func(context.Context, string, string, string, time.Duration, *tls.Config, bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
		calls.Add(1)
		return nil, nil, failure
	}
	require.ErrorIs(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"}), failure)
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), b.publishes.Load())
}

func TestPublishPublicCanceledReconnect(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"delay", "dial", "confirm setup", "before installing state"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			old, next := newPublishTestBroker(), newPublishTestBroker()
			r := newPublicPublisher(old)
			entered, aborted := make(chan struct{}), make(chan struct{})
			var abortOnce sync.Once
			next.abort = func() { abortOnce.Do(func() { close(aborted) }) }
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var calls atomic.Int32
			old.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
				if stage == "delay" {
					close(entered)
				}
				return nil, errors.New(errorChannelConnection)
			}
			if stage == "delay" {
				r.metadata.ReconnectWait = time.Hour
			}
			if stage == "confirm setup" {
				r.metadata.PublisherConfirm = true
				next.confirm = func() error { close(entered); <-aborted; return amqp.ErrClosed }
			}
			r.connectionDial = func(ctx context.Context, _ string, _ string, _ string, _ time.Duration, _ *tls.Config, _ bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
				calls.Add(1)
				switch stage {
				case "dial":
					close(entered)
					<-ctx.Done()
					return nil, nil, amqp.ErrCredentials
				case "before installing state":
					cancel()
					return next, next, nil
				default:
					return next, next, nil
				}
			}
			done := startPublicPublish(r, ctx, "topic")
			if stage != "before installing state" {
				awaitPublishGate(t, entered)
				cancel()
			}
			require.ErrorIs(t, awaitPublish(t, done), context.Canceled)
			require.Equal(t, int32(1), old.publishes.Load())
			require.Zero(t, next.publishes.Load())
			if stage == "delay" {
				require.Zero(t, calls.Load())
			} else {
				require.Equal(t, int32(1), calls.Load())
				require.Nil(t, r.channel)
				require.Nil(t, r.connection)
				require.Equal(t, 1, r.connectionCount)
			}
		})
	}
}

func TestPublishPublicConcurrentWaitersDoNotAbortHolder(t *testing.T) {
	t.Parallel()
	b := newPublishTestBroker()
	r := newPublicPublisher(b)
	entered, release := make(chan struct{}), make(chan struct{})
	b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
		close(entered)
		<-release
		return fakeConfirmation{acked: true}, nil
	}
	holder := startPublicPublish(r, t.Context(), "holder")
	awaitPublishGate(t, entered)
	const callers = 8
	waiters := make([]<-chan error, callers)
	for i := range waiters {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		waiters[i] = startPublicPublish(r, ctx, "waiter")
	}
	for _, done := range waiters {
		require.ErrorIs(t, awaitPublish(t, done), context.DeadlineExceeded)
	}
	require.Zero(t, b.aborts.Load())
	require.Equal(t, int32(1), b.publishes.Load())
	close(release)
	require.NoError(t, awaitPublish(t, holder))
}

func TestPublishPublicConcurrentErrorIsolation(t *testing.T) {
	t.Parallel()
	b := newPublishTestBroker()
	r := newPublicPublisher(b)
	denied := &amqp.Error{Code: amqp.AccessRefused, Reason: "ACCESS_REFUSED - one caller", Server: true}
	b.publish = func(_ context.Context, msg amqp.Publishing) (rabbitMQConfirmation, error) {
		if string(msg.Body) == "denied" {
			return nil, denied
		}
		return fakeConfirmation{acked: true}, nil
	}
	const callers = 16
	results := make([]chan error, callers)
	for i := range results {
		results[i] = make(chan error, 1)
		go func(i int) {
			data := []byte("ack")
			if i%2 == 0 {
				data = []byte("denied")
			}
			results[i] <- r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic", Data: data})
		}(i)
	}
	for i, done := range results {
		err := awaitPublish(t, done)
		if i%2 == 0 {
			require.ErrorIs(t, err, denied)
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, int32(callers), b.publishes.Load())
	require.Zero(t, b.aborts.Load())
}

func TestPublishPublicStaleGenerationAndFinishedCancellation(t *testing.T) {
	t.Parallel()
	old, next := newPublishTestBroker(), newPublishTestBroker()
	r := newPublicPublisher(old)
	failed := make(chan struct{})
	old.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
		close(failed)
		return nil, errors.New(errorChannelConnection)
	}
	r.metadata.ReconnectWait = 100 * time.Millisecond
	var dials atomic.Int32
	r.connectionDial = func(context.Context, string, string, string, time.Duration, *tls.Config, bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
		dials.Add(1)
		return nil, nil, errors.New("stale reconnect must not dial")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := startPublicPublish(r, ctx, "topic")
	awaitPublishGate(t, failed)
	require.NoError(t, r.channelMutex.Lock(t.Context()))
	r.connection, r.channel, r.connectionCount = next, next, 2
	r.declaredExchanges = make(map[string]bool)
	r.channelMutex.Unlock()
	require.NoError(t, awaitPublish(t, done))
	cancel()
	require.NoError(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "next caller"}))
	require.Zero(t, dials.Load())
	require.Zero(t, old.aborts.Load())
	require.Zero(t, next.aborts.Load())
	require.Equal(t, int32(2), next.publishes.Load())
}

func TestPublishPublicClosedConfirmationRemainsUncertain(t *testing.T) {
	t.Parallel()
	b := newPublishTestBroker()
	r := newPublicPublisher(b)
	r.metadata.ReconnectWait = time.Hour
	completed := make(chan struct{})
	b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
		return publishConfirmationFunc(func(context.Context) (bool, error) {
			b.closed.Store(true)
			close(completed)
			return false, amqp.ErrClosed
		}), nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := startPublicPublish(r, ctx, "topic")
	awaitPublishGate(t, completed)
	require.NoError(t, r.channelMutex.Lock(t.Context()))
	r.channelMutex.Unlock()
	cancel()
	err := awaitPublish(t, done)
	require.ErrorIs(t, err, context.Canceled)
	require.NotContains(t, err.Error(), "did not receive confirmation of publishing")
	require.Equal(t, int32(1), b.publishes.Load())
}

func TestPublishPublicLiveContextFailureExhaustion(t *testing.T) {
	t.Parallel()
	for _, failure := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			t.Parallel()
			b := newPublishTestBroker()
			r := newPublicPublisher(b)
			b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) { return nil, failure }
			require.ErrorIs(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"}), failure)
			require.NoError(t, t.Context().Err())
			require.Equal(t, int32(publishMaxRetries), b.publishes.Load())
		})
	}
}

func TestPublishPublicNACKSurvivesTransportUncertainty(t *testing.T) {
	t.Parallel()
	for _, cancelLast := range []bool{false, true} {
		name := "generic exhaustion"
		if cancelLast {
			name = "pipe error then canceled confirmation"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := newPublishTestBroker()
			r := newPublicPublisher(b)
			entered := make(chan struct{})
			b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
				switch b.publishes.Load() {
				case 1:
					return fakeConfirmation{acked: false}, nil
				case 2:
					return nil, io.ErrClosedPipe
				default:
					if !cancelLast {
						return nil, errors.New("uncertain transport failure")
					}
					return publishConfirmationFunc(func(ctx context.Context) (bool, error) { close(entered); <-ctx.Done(); return false, ctx.Err() }), nil
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := startPublicPublish(r, ctx, "topic")
			if cancelLast {
				awaitPublishGate(t, entered)
				cancel()
			}
			// Preserve the first broker NACK while later sent attempts remain uncertain.
			// That text cannot establish rejection or zero effect for the later attempts.
			if !cancelLast {
				select {
				case err := <-done:
					require.EqualError(t, err, "did not receive confirmation of publishing")
				case <-time.After(5 * time.Second):
					t.Fatal("publish did not exhaust its existing retry budget")
				}
			} else {
				require.EqualError(t, awaitPublish(t, done), "did not receive confirmation of publishing")
			}
			require.Equal(t, int32(publishMaxRetries), b.publishes.Load())
		})
	}
}

type publishObservedContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *publishObservedContext) Err() error {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Err()
}

func TestPublishPublicQueuedBeforeCloseIsTerminal(t *testing.T) {
	t.Parallel()
	b := newPublishTestBroker()
	r := newPublicPublisher(b)
	require.NoError(t, r.channelMutex.Lock(t.Context()))
	ctx := &publishObservedContext{Context: t.Context(), entered: make(chan struct{})}
	done := startPublicPublish(r, ctx, "topic")
	awaitPublishGate(t, ctx.entered)
	r.closed.Store(true)
	require.NoError(t, r.reset())
	r.channelMutex.Unlock()
	err := awaitPublish(t, done)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.EqualError(t, err, "component is closed")
	require.Zero(t, b.publishes.Load())
}

func TestPublishPublicFailureLogging(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		publishError     string
		reconnectFailure bool
	}{
		{name: "retry exhaustion", publishError: "publish failed"},
		{name: "reconnect exhaustion", publishError: errorChannelConnection},
		{name: "reconnect failure exhaustion", publishError: errorChannelConnection, reconnectFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newPublishTestBroker()
			publishErr := errors.New(tc.publishError)
			b.publish = func(context.Context, amqp.Publishing) (rabbitMQConfirmation, error) {
				return nil, publishErr
			}
			r := newPublicPublisher(b)
			var output bytes.Buffer
			r.logger = logger.NewLogger(t.Name())
			r.logger.EnableJSONOutput(true)
			r.logger.SetOutput(&output)
			t.Cleanup(func() { r.logger.SetOutput(io.Discard) })
			dialErr := errors.New("reconnect failed")
			if tc.reconnectFailure {
				r.connectionDial = func(context.Context, string, string, string, time.Duration, *tls.Config, bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
					return nil, nil, dialErr
				}
			}

			expectedErr := publishErr
			if tc.reconnectFailure {
				expectedErr = dialErr
			}
			require.ErrorIs(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"}), expectedErr)

			var attempts, reconnects, exhausted int
			for line := range bytes.SplitSeq(bytes.TrimSpace(output.Bytes()), []byte("\n")) {
				var entry struct {
					Level   string `json:"level"`
					Message string `json:"msg"`
				}
				require.NoError(t, json.Unmarshal(line, &entry))
				switch {
				case strings.HasPrefix(entry.Message, logMessagePrefix+" publishing attempt ("):
					attempts++
					attemptErr := publishErr
					if tc.reconnectFailure && attempts > 1 {
						attemptErr = dialErr
					}
					require.Equal(t, "warning", entry.Level)
					require.Equal(t, fmt.Sprintf("%s publishing attempt (%d/%d) failed: %v", logMessagePrefix, attempts, publishMaxRetries, attemptErr), entry.Message)
				case strings.HasPrefix(entry.Message, logMessagePrefix+" publisher is reconnecting in "):
					reconnects++
					require.Equal(t, "warning", entry.Level)
					require.Equal(t, logMessagePrefix+" publisher is reconnecting in 0s ...", entry.Message)
				case strings.HasPrefix(entry.Message, logMessagePrefix+" publishing failed:"):
					exhausted++
					require.Equal(t, "error", entry.Level)
					require.Equal(t, logMessagePrefix+" publishing failed: "+expectedErr.Error(), entry.Message)
				}
			}
			require.Equal(t, publishMaxRetries, attempts)
			expectedReconnects := 0
			if tc.publishError == errorChannelConnection {
				expectedReconnects = publishMaxRetries - 1
			}
			require.Equal(t, expectedReconnects, reconnects)
			require.Equal(t, 1, exhausted)
		})
	}
}
