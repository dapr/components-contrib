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

const (
	browseTimeout         = time.Second * 1
	browseRefreshTimeout  = time.Second * 10
	browseRefreshInterval = time.Second * 30
	addressTTL            = time.Second * 60
)

type address struct {
	ip        string
	expiresAt time.Time
}

type addressList struct {
	addresses []*address
	counter   uint32
	mu        sync.RWMutex
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
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.addresses) == 0 {
		return nil
	}

	if a.counter == math.MaxUint32 {
		a.counter = 0
	}
	index := a.counter % uint32(len(a.addresses))
	addr := a.addresses[index]
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

		// Register as a unique instance
		pid := syscall.Getpid()
		instance := fmt.Sprintf("%s-%d", host, pid)

		if len(ips) > 0 {
			server, err = zeroconf.RegisterProxy(instance, id, "local.", port, host, ips, info, nil)
		} else {
			server, err = zeroconf.Register(instance, id, "local.", port, info, nil)
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
	ipv4AddrList, ipv4Exists := m.ipv4Addresses[req.ID]
	m.ipv4Mu.RUnlock()
	if ipv4Exists {
		ipv4Addr := ipv4AddrList.next()
		if ipv4Addr != nil {
			m.logger.Debugf("found mdns ipv4 address in cache: %s", *ipv4Addr)
			return *ipv4Addr, nil
		}
	}

	// Attempt to get next ipv6 address for app id.
	m.ipv6Mu.RLock()
	ipv6AddrList, ipv6Exists := m.ipv6Addresses[req.ID]
	m.ipv6Mu.RUnlock()
	if ipv6Exists {
		ipv6Addr := ipv6AddrList.next()
		if ipv6Addr != nil {
			m.logger.Debugf("found mdns ipv6 address in cache: %s", *ipv6Addr)
			return *ipv6Addr, nil
		}
	}

	// No cached addresses, browse the network for the app id.
	m.logger.Debugf("no mdns address found in cache, browsing for app %s", req.ID)
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

	onErr := func(_ error) {
		cancel() // We cancel the context to stop browsing.
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
	m.logger.Debugf("refreshing mdns cache")

	// Check if we have any ipv4 or ipv6 addresses
	// in the address cache that need refreshing.
	m.ipv4Mu.RLock()
	numIPv4Addr := len(m.ipv4Addresses)
	m.ipv4Mu.RUnlock()

	m.ipv6Mu.RLock()
	numIPv6Addr := len(m.ipv4Addresses)
	m.ipv6Mu.RUnlock()

	numApps := numIPv4Addr + numIPv6Addr
	if numApps == 0 {
		m.logger.Debugf("no mdns app's to refresh")
		return nil
	}

	// Build a set of all known app's currently in
	// the address cache. Ensuring any expired addresses
	// are evicted. Then browse the network for each
	// app and update the address cache.
	appIDKeys := make(map[string]struct{})

	m.ipv4Mu.RLock()
	for appID, addr := range m.ipv4Addresses {
		old := len(addr.addresses)
		addr.expire()
		m.logger.Debugf("%d ipv4 addresses expired from the mdns cache", old-len(addr.addresses))

		appIDKeys[appID] = struct{}{}
	}
	m.ipv4Mu.RUnlock()

	m.ipv6Mu.RLock()
	for appID, addr := range m.ipv6Addresses {
		old := len(addr.addresses)
		addr.expire()
		m.logger.Debugf("%d ipv6 addresses expired from the mdns cache", old-len(addr.addresses))

		appIDKeys[appID] = struct{}{}
	}
	m.ipv6Mu.RUnlock()

	appIDs := make([]string, 0, len(appIDKeys))
	for appID := range appIDKeys {
		appIDs = append(appIDs, appID)
	}

	browseCtx, cancel := context.WithTimeout(context.Background(), browseRefreshTimeout)
	defer cancel()

	onErr := func(_ error) {
		cancel() // We cancel the context to stop browsing.
	}

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
		for entry := range results {
			m.logger.Debugf("mdns response for app %s received", appID)
			for _, text := range entry.Text {
				if text != appID {
					m.logger.Debugf("mdns response doesn't match app id %s", appID)
					continue
				}
				hasIPv4Address := len(entry.AddrIPv4) > 0
				hasIPv6Address := len(entry.AddrIPv6) > 0

				if !hasIPv4Address && !hasIPv6Address {
					m.logger.Debugf("mdns response doesn't contain any addresses for app id %s", appID)
				}

				var addr string
				port := entry.Port

				// Service entry contains addresses for this
				// app id. We currently only support the first
				// address in entry's AddrIPv4 and AddrIPv6
				// arrays. These addresses will be added to
				// the internal address cache for this appid.
				// NOTE: We use write locks here to read and
				// modify the address cache. We could use more
				// granular read and write locks but this would
				// increasse the code complexity.
				if hasIPv4Address {
					addr = fmt.Sprintf("%s:%d", entry.AddrIPv4[0].String(), port)
					m.logger.Debugf("mdns response for app %s has ipv4 address %s.", appID, addr)

					m.ipv4Mu.Lock()
					if _, ok := m.ipv4Addresses[appID]; !ok {
						m.ipv4Addresses[appID] = &addressList{}
					}
					m.ipv4Addresses[appID].add(addr)
					m.ipv4Mu.Unlock()
				}
				if hasIPv6Address {
					addr = fmt.Sprintf("%s:%d", entry.AddrIPv6[0].String(), port)
					m.logger.Debugf("mdns response for app %s has ipv6 address %s.", appID, addr)

					m.ipv6Mu.Lock()
					if _, ok := m.ipv6Addresses[appID]; !ok {
						m.ipv6Addresses[appID] = &addressList{}
					}
					m.ipv6Addresses[appID].add(addr)
					m.ipv6Mu.Unlock()
				}

				// On each address, invoke a custom callback.
				if onEach != nil {
					onEach(addr)
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
