// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package retry

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/kit/retry"
)

func Do(frequency time.Duration, maxRetries uint64, runnable flow.Runnable) flow.Runnable {
	return func(ctx flow.Context) error {
		fn := func() error {
			return runnable(ctx)
		}
		return retry.NotifyRecover(fn,
			backoff.WithMaxRetries(backoff.NewConstantBackOff(frequency), maxRetries),
			func(err error, t time.Duration) {
				ctx.Logf("Failure: %v; Retrying in %s", err, t)
			}, func() {
				ctx.Log("Success!")
			},
		)
	}
}
