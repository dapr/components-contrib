// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dapr/components-contrib/servicediscovery"
	"github.com/grandcat/zeroconf"
)

var once sync.Once

func NewMDNSResolver() *Resolver {
	return &Resolver{}
}

type Resolver struct {
	Resolver *zeroconf.Resolver
}

func (z *Resolver) ResolveID(req *servicediscovery.ResolveRequest) (string, error) {
	port, err := z.LookupPortMDNS(req.ID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("localhost:%v", port), nil
}

// LookupPortMDNS uses mdns to find the port of a given service entry on a local network
func (z *Resolver) LookupPortMDNS(id string) (int, error) {
	var err error
	var t *zeroconf.Resolver
	once.Do(func() {
		t, err = zeroconf.NewResolver(nil)
		z.Resolver = t
	})

	if err != nil {
		return -1, fmt.Errorf("failed to initialize resolver: %e", err)
	}

	port := -1
	entries := make(chan *zeroconf.ServiceEntry)

	go func(results <-chan *zeroconf.ServiceEntry) {
		for entry := range results {
			for _, text := range entry.Text {
				if text == id {
					port = entry.Port
					return
				}
			}
		}
	}(entries)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	err = z.Resolver.Browse(ctx, id, "local.", entries)
	if err != nil {
		return -1, fmt.Errorf("failed to browse: %s", err.Error())
	}

	<-ctx.Done()
	if port == -1 {
		return port, fmt.Errorf("couldn't find service: %s", id)
	}
	return port, nil
}
