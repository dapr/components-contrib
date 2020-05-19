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

func NewMDNSResolver(logger logger.Logger) servicediscovery.Resolver {
	return &resolver{logger: logger}
}

type resolver struct {
	logger logger.Logger
}

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
					return
				}
			}
		}
	}(entries)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	err = resolver.Browse(ctx, id, "local.", entries)
	if err != nil {
		return "", fmt.Errorf("failed to browse: %s", err.Error())
	}

	<-ctx.Done()
	if port == -1 || addr == "" {
		return "", fmt.Errorf("couldn't find service: %s", id)
	}
	return fmt.Sprintf("%s:%d", addr, port), nil
}
