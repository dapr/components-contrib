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

package indexing

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
)

// Apply validates opts and applies write. With no delay the write is applied
// before Apply returns and the acknowledgement is IndexAckCompleted. With a
// delay the write is queued and applied in the background once delay has
// passed, which makes IndexAckQueued and wait timeouts observable: a wait for
// completion that expires either fails with DEADLINE_EXCEEDED or reports
// IndexAckQueued, and the queued write still lands.
//
// ponytail: queued writes are not tracked, so Close does not wait for them. Track them if the delay is ever used outside development and tests.
func Apply(ctx context.Context, opts search.IndexingOptions, delay time.Duration, write func()) (search.IndexAck, error) {
	if err := CtxErr(ctx); err != nil {
		return search.IndexAckUnspecified, err
	}
	if err := search.ValidateIndexingOptions(ctx, opts, true); err != nil {
		return search.IndexAckUnspecified, err
	}

	if delay <= 0 {
		write()
		return search.IndexAckCompleted, nil
	}

	done := make(chan struct{})
	go func() {
		time.Sleep(delay)
		write()
		close(done)
	}()

	if opts.Mode != search.IndexingModeWaitForCompletion {
		return search.IndexAckQueued, nil
	}

	select {
	case <-done:
		return search.IndexAckCompleted, nil
	case <-ctx.Done():
		return search.IndexAckUnspecified, CtxErr(ctx)
	case <-time.After(opts.WaitTimeout):
		if opts.OnWaitTimeout == search.IndexingWaitTimeoutActionContinueAsync {
			return search.IndexAckQueued, nil
		}
		return search.IndexAckUnspecified, status.Errorf(codes.DeadlineExceeded, "write did not complete within wait_timeout %s", opts.WaitTimeout)
	}
}

// CtxErr returns the context's error as a status error, or nil.
func CtxErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}
