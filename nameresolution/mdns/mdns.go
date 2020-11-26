// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/grandcat/zeroconf"
)

const browseTimeout = time.Second * 1
const browseRefreshTimeout = time.Second * 2
const browseRefreshInterval = time.Second * 30
const addressTTL = time.Second * 60

type address struct {
	ip        string
	expiresAt time.Time
}

type addressList struct {
	addresses []*address
	counter   uint32
	mu        sync.Mutex
}

// expire removes any expired addresses from the address list.
func (a *addressList) expire() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// remove expired addresses
	i := 0
	for _, addr := range a.addresses {
		if time.Now().Before(addr.expiresAt) {
			a.addresses[i] = addr
			i++
		}
	}
	for j := i; j < len(a.addresses); j++ {
		// clear truncated pointers
		a.addresses[j] = nil
	}
	// resize slice
	a.addresses = a.addresses[:i]
}

// add adds or updates an address to the address list.
func (a *addressList) add(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, addr := range a.addresses {
		if addr.ip == ip {
			// ip exists, renew expiration.
			addr.expiresAt = time.Now().Add(addressTTL)
			return
		}
	}
	// ip is new.
	a.addresses = append(a.addresses, &address{
		ip:        ip,
		expiresAt: time.Now().Add(addressTTL),
	})
}

// next gets the next address from the list.
func (a *addressList) next() *string {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.addresses) == 0 {
		return nil
	}

	index := a.counter % uint32(len(a.addresses))
	addr := a.addresses[index]
	if a.counter == math.MaxUint32-1 {
		a.counter = 0
	}
	a.counter++
	return &addr.ip
}

// NewResolver creates the instance of mDNS name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	r := &resolver{
		ipv4Addresses: make(map[string]*addressList),
		ipv6Addresses: make(map[string]*addressList),
		logger:        logger,
	}

	// periodically refresh all app addresses in the background.
	go func() {
		for {
			r.browseAll()

			time.Sleep(browseRefreshInterval)
		}
	}()

	return r
}

type resolver struct {
	ipv4Mu        sync.RWMutex
	ipv4Addresses map[string]*addressList
	ipv6Mu        sync.RWMutex
	ipv6Addresses map[string]*addressList
	logger        logger.Logger
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
	// Attempt to get next ipv4 address for app id first.
	m.ipv4Mu.RLock()
	if ipv4AddrList, ok := m.ipv4Addresses[req.ID]; ok {
		ipv4Addr := ipv4AddrList.next()
		if ipv4Addr != nil {
			return *ipv4Addr, nil
		}
	}
	m.ipv4Mu.RUnlock()

	// Attempt to get next ipv6 address for app id.
	m.ipv6Mu.RLock()
	if ipv6AddrList, ok := m.ipv6Addresses[req.ID]; ok {
		ipv6Addr := ipv6AddrList.next()
		if ipv6Addr != nil {
			return *ipv6Addr, nil
		}
	}
	m.ipv6Mu.RUnlock()

	// No cached addresses, browse the network for the app id.
	return m.browseFirstOnly(req.ID)
}

func (m *resolver) browseFirstOnly(appID string) (string, error) {
	var addr string

	ctx, cancel := context.WithTimeout(context.Background(), browseTimeout)
	defer cancel()

	// onFirst will be executed for each address received
	// for the provided app id. Cancelling the context will
	// stop any further browsing so this will only actually
	// be invoked on the first address.
	onFirst := func(ip string) {
		addr = ip
		cancel()
	}

	// onErr will be called only when we receive a browsing
	// error. We cancel the context to stop browsing.
	onErr := func(_ error) {
		cancel()
	}

	err := m.browse(ctx, appID, onFirst, onErr)
	if err != nil {
		return "", err
	}

	// wait until context is cancelled or timed out.
	<-ctx.Done()

	if addr == "" {
		return "", fmt.Errorf("couldn't find service: %s", appID)
	}

	return addr, nil
}

func (m *resolver) browseAll() error {
	// Get a list of all known app ids.
	appIDKeys := make(map[string]struct{})
	for appID, addr := range m.ipv4Addresses {
		addr.expire() // Remove expired ipv4 addresses.

		appIDKeys[appID] = struct{}{}
	}
	for appID, addr := range m.ipv6Addresses {
		addr.expire() // Remove expired ipv6 addresses.

		appIDKeys[appID] = struct{}{}
	}
	appIDs := make([]string, 0, len(appIDKeys))
	for appID := range appIDKeys {
		appIDs = append(appIDs, appID)
	}

	browseCtx, cancel := context.WithTimeout(context.Background(), browseRefreshTimeout)
	defer cancel()

	// onErr will be called only when we receive a browsing
	// error. We cancel the context to stop browsing.
	onErr := func(_ error) {
		cancel()
	}

	// Refresh each known app id.
	for _, appID := range appIDs {
		err := m.browse(browseCtx, appID, nil, onErr)
		if err != nil {
			m.logger.Warn("Error refreshing app id %s, error: %+v", appID, err)
		}
	}

	// wait until browsing context is cancelled (i.e. on error) or timed out.
	<-browseCtx.Done()

	return nil
}

func (m *resolver) browse(ctx context.Context, appID string, onEach func(ip string), onErr func(error)) error {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return fmt.Errorf("failed to initialize resolver: %e", err)
	}
	entries := make(chan *zeroconf.ServiceEntry)

	// Browse the network and find the addresses for this app id.
	go func(results <-chan *zeroconf.ServiceEntry) {
		select {
		case <-ctx.Done():
			return
		case entry := <-results:
			for _, text := range entry.Text {
				if text == appID {
					// On app id match.
					var addr string
					port := entry.Port

					if len(entry.AddrIPv4) > 0 {
						// Update the cache with a IPv4 address.
						addr = fmt.Sprintf("%s:%d", entry.AddrIPv4[0].String(), port)

						m.ipv4Mu.Lock()
						if _, ok := m.ipv4Addresses[appID]; !ok {
							m.ipv4Addresses[appID] = &addressList{}
						}
						m.ipv4Addresses[appID].add(addr)
						m.ipv4Mu.Unlock()
					} else if len(entry.AddrIPv6) > 0 {
						// Update the cache with a IPv6 address.
						addr = fmt.Sprintf("%s:%d", entry.AddrIPv6[0].String(), port)

						m.ipv6Mu.Lock()
						if _, ok := m.ipv6Addresses[appID]; !ok {
							m.ipv6Addresses[appID] = &addressList{}
						}
						m.ipv6Addresses[appID].add(addr)
						m.ipv6Mu.Unlock()
					} else {
						// Default: add a localhost IPv4 entry.
						addr = fmt.Sprintf("localhost:%d", port)

						m.ipv4Mu.Lock()
						m.ipv4Addresses[appID].add(addr)
						m.ipv4Mu.Unlock()
					}

					// On each address, invoke a custom callback.
					if onEach != nil {
						onEach(addr)
					}
				}
			}
		}
	}(entries)

	if err = resolver.Browse(ctx, appID, "local.", entries); err != nil {
		// On error, invoke a custom callback.
		if onErr != nil {
			onErr(err)
		}

		return fmt.Errorf("failed to browse: %s", err.Error())
	}

	return nil
}
