/*
Copyright 2022 The Dapr Authors
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
