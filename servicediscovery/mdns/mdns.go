// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"context"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/servicediscovery"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/grandcat/zeroconf"
)

const browseTimeout = time.Second * 1

// NewMDNSResolver creates the instance of mDNS service discovery resolver.
func NewMDNSResolver(logger logger.Logger) servicediscovery.Resolver {
	return &resolver{logger: logger}
}

type resolver struct {
	logger logger.Logger
}

// ResolveID discovers address by app ID.
func (z *resolver) ResolveID(req servicediscovery.ResolveRequest) (string, error) {
	address, err := lookupAddressMDNS(req.ID)
	if err != nil {
		return "", err
	}
	return address, nil
}

// lookupAddressMDNS uses mdns to find the port of a given service entry on a local network
func lookupAddressMDNS(id string) (string, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return "", fmt.Errorf("failed to initialize resolver: %e", err)
	}

	port := -1
	var addr string
	entries := make(chan *zeroconf.ServiceEntry)

	ctx, cancel := context.WithTimeout(context.Background(), browseTimeout)

	go func(results <-chan *zeroconf.ServiceEntry) {
		for entry := range results {
			for _, text := range entry.Text {
				if text == id {
					port = entry.Port
					if len(entry.AddrIPv4) > 0 {
						addr = entry.AddrIPv4[0].String() // entry has IPv4
					} else if len(entry.AddrIPv6) > 0 {
						addr = entry.AddrIPv6[0].String() // entry has IPv6
					} else {
						addr = "localhost" // default
					}

					// cancel timeout because it found the service
					cancel()
					return
				}
			}
		}
	}(entries)

	if err := resolver.Browse(ctx, id, "local.", entries); err != nil {
		return "", fmt.Errorf("failed to browse: %s", err.Error())
	}

	// wait until context is cancelled or timeed out.
	<-ctx.Done()

	if port == -1 || addr == "" {
		return "", fmt.Errorf("couldn't find service: %s", id)
	}

	return fmt.Sprintf("%s:%d", addr, port), nil
}
