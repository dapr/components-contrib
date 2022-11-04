/*
Copyright 2021 The Dapr Authors
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

package network

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/tylertreat/comcast/throttler"

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

// InterruptNetwork uses operating system specific functionality to block network traffic on select IPs and ports.
func InterruptNetwork(duration time.Duration, ipv4s []string, ipv6s []string, ports ...string) flow.Runnable {
	/*
		duration:
			- 0: the network will be interrupted indefinitely
			- >0: the network will be interrupted for the specified duration
		ipv4s:
			- []string: the list of IPv4 addresses to which the network interruption will be applied
			- nil: the network interruption will be applied to all IPv4 addresses
		ipv6s:
			- []string: the list of IPv6 addresses to which the network interruption will be applied
			- nil: the network interruption will be applied to all IPv6 addresses
		ports:
			- []string: the list of ports to which the network interruption will be applied
			- nil: the network interruption will be applied to all ports

		Example:
			InterruptNetwork(30 * time.Second, nil, nil, "8080", "9000:9999")
	*/
	return func(ctx flow.Context) error {
		InterruptNetworkWithContext(ctx, duration, ipv4s, ipv6s, ports...)
		return nil
	}
}

// InterruptNetworkWithContext interrupts the network until a timeout or a context is canceled.
func InterruptNetworkWithContext(ctx context.Context, duration time.Duration, ipv4s []string, ipv6s []string, ports ...string) {
	throttler.Run(&throttler.Config{
		Device:           "",
		Stop:             false,
		Latency:          -1,
		TargetBandwidth:  -1,
		DefaultBandwidth: -1,
		PacketLoss:       100,
		TargetIps:        ipv4s,
		TargetIps6:       ipv6s,
		TargetPorts:      ports,
		TargetProtos:     []string{"tcp", "udp"},
		DryRun:           false,
	})

	alreadyCleanedUp := false

	t := time.NewTimer(duration)
	defer func() {
		if !t.Stop() && !alreadyCleanedUp {
			<-t.C
		}
	}()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
	alreadyCleanedUp = true
	throttler.Run(&throttler.Config{
		Device:           "",
		Stop:             true,
		Latency:          -1,
		TargetBandwidth:  -1,
		DefaultBandwidth: -1,
		PacketLoss:       0,
		TargetIps:        nil,
		TargetIps6:       nil,
		TargetPorts:      nil,
		TargetProtos:     nil,
		DryRun:           false,
	})
}
