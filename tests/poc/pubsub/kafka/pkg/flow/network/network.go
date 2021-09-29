package network

import (
	"errors"
	"net"
	"time"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

func WaitForAddresses(timeout time.Duration, addresses ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		start := time.Now().UTC()

		for _, address := range addresses {
		check:
			for {
				d := timeout - time.Since(start)
				if d <= 0 {
					return errors.New("timeout")
				}
				ctx.Logf("Waiting for address %q", address)
				conn, _ := net.DialTimeout("tcp", address, d)
				if conn != nil {
					conn.Close()

					break check
				}

				time.Sleep(time.Millisecond * 500)
			}
		}

		return nil
	}
}
