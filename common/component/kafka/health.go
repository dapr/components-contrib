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
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// Ping performs a minimal connectivity check against the configured Kafka
// brokers. It opens a short-lived Sarama client that issues a single,
// lightweight metadata request (Metadata.Full=false so no topic-level scan is
// performed) and then closes it immediately.
//
// The check is fully bounded and cancellable:
//   - Pass a context with a deadline to enforce a wall-clock limit. When a
//     deadline is present, all Net and Metadata timeouts are clamped to the
//     remaining time so the call cannot block beyond the deadline.
//   - Context cancellation is honoured mid-flight: sarama.NewClient is run in
//     a goroutine and the function returns ctx.Err() as soon as ctx is done.
//     Any partially-opened client is closed in the background to avoid leaks.
//
// Ping does NOT modify any internal component state and is safe to call from
// concurrent goroutines after Init() has completed.
func (k *Kafka) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if k.closed.Load() {
		return errors.New("kafka health check: component is closed")
	}

	if k.config == nil {
		return errors.New("kafka health check: component not initialised")
	}

	// Build a minimal config copy that honours the context deadline.
	// Metadata.Full=false avoids a full topic scan which would be expensive and
	// could fail on least-privilege principals that cannot list all topics.
	cfgCopy := *k.config
	cfgCopy.Metadata.Full = false

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errors.New("kafka health check: context deadline already exceeded")
		}
		// Clamp all net timeouts to the remaining deadline so that Sarama's
		// underlying dials respect the caller's overall timeout.
		if cfgCopy.Net.DialTimeout > remaining || cfgCopy.Net.DialTimeout == 0 {
			cfgCopy.Net.DialTimeout = remaining
		}
		if cfgCopy.Net.ReadTimeout > remaining || cfgCopy.Net.ReadTimeout == 0 {
			cfgCopy.Net.ReadTimeout = remaining
		}
		if cfgCopy.Net.WriteTimeout > remaining || cfgCopy.Net.WriteTimeout == 0 {
			cfgCopy.Net.WriteTimeout = remaining
		}
		// Also bound the metadata request itself and disable retries so the
		// total time is governed by the caller's deadline alone.
		cfgCopy.Metadata.Timeout = remaining
		cfgCopy.Metadata.Retry.Max = 0
	}

	type result struct {
		cl  sarama.Client
		err error
	}

	ch := make(chan result, 1)
	go func() {
		// sarama.NewClient with Metadata.Full=false does NOT issue any network
		// request — it only registers the seed broker addresses. We must call
		// RefreshMetadata() explicitly to force an actual TCP connection and
		// retrieve a minimal broker-list response. This is the real connectivity
		// probe; it respects cfgCopy.Metadata.Timeout and .Retry.Max set above.
		cl, err := sarama.NewClient(k.brokers, &cfgCopy)
		if err != nil {
			ch <- result{nil, err}
			return
		}
		if merr := cl.RefreshMetadata(); merr != nil {
			_ = cl.Close()
			ch <- result{nil, merr}
			return
		}
		ch <- result{cl, nil}
	}()

	select {
	case <-ctx.Done():
		// Context cancelled/timed out before the client could connect. Drain the
		// goroutine result when it eventually arrives and close the client to
		// prevent a resource leak.
		go func() {
			r := <-ch
			if r.cl != nil {
				if cerr := r.cl.Close(); cerr != nil {
					k.logger.Debugf("kafka health check: error closing transient client after ctx cancel: %v", cerr)
				}
			}
		}()
		return ctx.Err()

	case r := <-ch:
		if r.err != nil {
			return fmt.Errorf("kafka health check: failed to reach brokers %v: %w", k.brokers, r.err)
		}
		if cerr := r.cl.Close(); cerr != nil {
			k.logger.Debugf("kafka health check: error closing transient client: %v", cerr)
		}
		return nil
	}
}
