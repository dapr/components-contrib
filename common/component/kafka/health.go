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

// Ping checks whether at least one configured Kafka broker is reachable by
// opening a short-lived Sarama client and issuing a metadata request. The
// operation respects the provided context so callers can enforce their own
// timeout (e.g. a liveness-probe deadline).
//
// Ping is additive and does NOT modify any internal state. It is safe to call
// from concurrent goroutines after Init() has completed.
//
// The Net dial/read/write timeouts configured via component metadata
// (dialTimeout, readTimeout, writeTimeout) are inherited by the transient
// client, so Ping() will not block indefinitely on an unreachable cluster.
func (k *Kafka) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if k.config == nil {
		return errors.New("kafka health check: component not initialised")
	}

	// Build a minimal config copy that honours the context deadline.
	cfgCopy := *k.config
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errors.New("kafka health check: context deadline already exceeded")
		}
		// Clamp all net timeouts to the remaining deadline so that Sarama's
		// underlying dials respect the caller's overall timeout.
		if cfgCopy.Net.DialTimeout > remaining {
			cfgCopy.Net.DialTimeout = remaining
		}
		if cfgCopy.Net.ReadTimeout > remaining {
			cfgCopy.Net.ReadTimeout = remaining
		}
		if cfgCopy.Net.WriteTimeout > remaining {
			cfgCopy.Net.WriteTimeout = remaining
		}
	}

	// A transient Sarama client performs broker discovery (metadata) on New.
	// We close it immediately after — this is the connectivity check.
	cl, err := sarama.NewClient(k.brokers, &cfgCopy)
	if err != nil {
		return fmt.Errorf("kafka health check: failed to reach brokers %v: %w", k.brokers, err)
	}
	_ = cl.Close()

	return nil
}
