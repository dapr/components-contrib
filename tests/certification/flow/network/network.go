// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package network

import (
	"errors"
	"net"
	"time"

	"github.com/dapr/components-contrib/tests/certification/flow"
)

func WaitForAddresses(timeout time.Duration, addresses ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		start := time.Now().UTC()

		for _, address := range addresses {
			ctx.Logf("Waiting for address %q", address)
		check:
			for {
				d := timeout - time.Since(start)
				if d <= 0 {
					return errors.New("timeout")
				}

				conn, _ := net.DialTimeout("tcp", address, d)
				if conn != nil {
					conn.Close()

					break check
				}

				time.Sleep(time.Millisecond * 500)
			}
			ctx.Logf("Address %q is ready", address)
		}

		return nil
	}
}
