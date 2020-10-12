// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/grandcat/zeroconf"
)

const browseTimeout = time.Second * 1

// NewResolver creates the instance of mDNS name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{logger: logger}
}

type resolver struct {
	logger logger.Logger
}

// Init registers service for mDNS.
func (m *resolver) Init(metadata nameresolution.Metadata) error {
	var id string
	var hostAddress string
	var ok bool

	props := metadata.Properties

	if id, ok = props[nameresolution.MDNSInstanceName]; !ok {
		return errors.New("name is missing")
	}
	if hostAddress, ok = props[nameresolution.MDNSInstanceAddress]; !ok {
		return errors.New("address is missing")
	}

	p, ok := props[nameresolution.MDNSInstancePort]
	if !ok {
		return errors.New("port is missing")
	}

	port, err := strconv.ParseInt(p, 10, 32)
	if err != nil {
		return errors.New("port is invalid")
	}

	err = m.registerMDNS(id, []string{hostAddress}, int(port))
	if err == nil {
		m.logger.Infof("local service entry announced: %s -> %s:%d", id, hostAddress, port)
	}

	return err
}

func (m *resolver) registerMDNS(id string, ips []string, port int) error {
	started := make(chan bool, 1)
	var err error

	go func() {
		var server *zeroconf.Server

		host, _ := os.Hostname()
		info := []string{id}

		if len(ips) > 0 {
			server, err = zeroconf.RegisterProxy(host, id, "local.", port, host, ips, info, nil)
		} else {
			server, err = zeroconf.Register(host, id, "local.", port, info, nil)
		}

		if err != nil {
			started <- false
			m.logger.Errorf("error from zeroconf register: %s", err)

			return
		}
		started <- true

		// Wait until it gets SIGTERM event.
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig

		server.Shutdown()
	}()

	<-started

	return err
}

// ResolveID resolves name to address via mDNS.
func (m *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
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
				if text == req.ID {
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

	if err = resolver.Browse(ctx, req.ID, "local.", entries); err != nil {
		// cancel context
		cancel()

		return "", fmt.Errorf("failed to browse: %s", err.Error())
	}

	// wait until context is cancelled or timeed out.
	<-ctx.Done()

	if port == -1 || addr == "" {
		return "", fmt.Errorf("couldn't find service: %s", req.ID)
	}

	return fmt.Sprintf("%s:%d", addr, port), nil
}
