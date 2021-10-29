// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package simulate

import (
	"sync/atomic"

	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/pkg/errors"
)

func PeriodicError(ctx flow.Context, frequency uint64) func() error {
	counter := uint64(0)
	errorCount := uint64(0)
	return func() error {
		next := atomic.AddUint64(&counter, 1)

		// This behavior is standard to repro a failure of one message in a batch.
		if atomic.LoadUint64(&errorCount) < 2 || next%frequency == 0 {
			// First message errors just to give time for more messages to pile up.
			// Second error is to force an error in a batch.
			ec := atomic.AddUint64(&errorCount, 1)
			ctx.Logf("Simulating error %d", ec)

			return errors.Errorf("simulated error")
		}

		return nil
	}
}
