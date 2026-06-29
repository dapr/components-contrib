/*
Copyright 2024 The Dapr Authors
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

package kafka

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

// TestPingUninitialised verifies that Ping returns an error when the
// component has not been initialised (k.config == nil).
func TestPingUninitialised(t *testing.T) {
	k := &Kafka{logger: logger.NewLogger("kafka_test")}
	err := k.Ping(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not initialised")
}

// TestPingCancelledContext verifies that Ping respects a cancelled context.
func TestPingCancelledContext(t *testing.T) {
	k := &Kafka{
		logger:  logger.NewLogger("kafka_test"),
		config:  sarama.NewConfig(),
		brokers: []string{"localhost:9092"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := k.Ping(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestPingDeadlineExceededContext verifies that Ping respects a context whose
// deadline is already in the past.
func TestPingDeadlineExceededContext(t *testing.T) {
	k := &Kafka{
		logger:  logger.NewLogger("kafka_test"),
		config:  sarama.NewConfig(),
		brokers: []string{"localhost:9092"},
	}

	// Deadline in the past.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := k.Ping(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestPingUnreachableBroker verifies that Ping fails fast for an unreachable
// broker (using a very short timeout so the test does not hang).
func TestPingUnreachableBroker(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = 200 * time.Millisecond
	cfg.Net.ReadTimeout = 200 * time.Millisecond
	cfg.Net.WriteTimeout = 200 * time.Millisecond
	cfg.Metadata.Retry.Max = 0

	k := &Kafka{
		logger:  logger.NewLogger("kafka_test"),
		config:  cfg,
		brokers: []string{"127.0.0.1:19999"}, // non-routable port
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := k.Ping(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "health check")
}

// TestPingClosedComponent verifies that Ping returns an error when the
// component has been closed.
func TestPingClosedComponent(t *testing.T) {
	k := &Kafka{
		logger:  logger.NewLogger("kafka_test"),
		config:  sarama.NewConfig(),
		brokers: []string{"localhost:9092"},
	}
	k.closed.Store(true)

	err := k.Ping(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed")
}

// TestPingContextCancelledMidFlight verifies that Ping returns promptly when
// the context is cancelled shortly after the call is made against a
// non-routable (blackholed) address. The test must complete well before any
// Net dial timeout would fire — this proves the goroutine/select path works.
func TestPingContextCancelledMidFlight(t *testing.T) {
	// Use a config with long dial timeouts to prove ctx cancellation is what
	// causes the early return, not the Net timeout.
	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = 30 * time.Second
	cfg.Net.ReadTimeout = 30 * time.Second
	cfg.Net.WriteTimeout = 30 * time.Second
	cfg.Metadata.Retry.Max = 0

	// Stand up a local TCP listener that we never Accept() from. The kernel
	// completes the TCP handshake from the listen backlog, so the Sarama client
	// connects and then blocks waiting for a broker response that never arrives.
	// This makes the "in-flight" state deterministic across environments, unlike
	// relying on TEST-NET blackhole behaviour which varies by CI network stack.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	k := &Kafka{
		logger:  logger.NewLogger("kafka_test"),
		config:  cfg,
		brokers: []string{ln.Addr().String()},
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay to simulate the caller giving up.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err = k.Ping(ctx)
	elapsed := time.Since(start)

	require.Error(t, err)
	// The connection is accepted by the kernel but never answered, so context
	// cancellation is what unblocks Ping and err wraps context.Canceled. Some
	// Sarama paths may surface the cancelled read as a broker-connect error
	// instead; both prove Ping returns on ctx rather than the Net timeout.
	if !errors.Is(err, context.Canceled) {
		require.Contains(t, err.Error(), "health check",
			"unexpected error (not ctx.Canceled and not a broker health-check error): %v", err)
	}
	// Must complete well within the 30 s Net.DialTimeout.
	require.Less(t, elapsed, 5*time.Second, "Ping should return shortly after ctx cancel, not wait for the net timeout")
}
