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
	"github.com/dapr/dapr/pkg/discovery"
	"github.com/grandcat/zeroconf"
)

func NewmDNSResolver() *MDNSResolver {
	return &MDNSResolver{}
}

type MDNSResolver struct {
}

func (z *MDNSResolver) ResolveID(req *servicediscovery.ResolveRequest) (string, error) {
	port, err := discovery.LookupPortMDNS(req.Id)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("localhost:%v", port), nil
}

// LookupPortMDNS uses mdns to find the port of a given service entry on a local network
func LookupPortMDNS(id string) (int, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return -1, fmt.Errorf("Failed to initialize resolver: %e", err)
	}

	port := -1
	entries := make(chan *zeroconf.ServiceEntry)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	go func(results <-chan *zeroconf.ServiceEntry) {
		for entry := range results {
			for _, text := range entry.Text {
				if text == id {
					port = entry.Port
					cancel()
					return
				}
			}
		}
	}(entries)

	err = resolver.Browse(ctx, id, "local.", entries)
	if err != nil {
		return -1, fmt.Errorf("failed to browse: %s", err.Error())
	}

	<-ctx.Done()
	if port == -1 {
		return port, fmt.Errorf("couldn't find service: %s", id)
	}
	return port, nil
}
