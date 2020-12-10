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
	// firstOnlyTimeout is the timeout used when
	// browsing for the first response to a single app id.
	firstOnlyTimeout = time.Second * 1
	// refreshTimeout is the timeout used when
	// browsing for any responses to a single app id.
	refreshTimeout = time.Second * 2
	// refreshInterval is the duration between
	// background address refreshes.
	refreshInterval = time.Second * 30
	// addressTTL is the duration an address has before
	// becoming stale and being evicted.
	addressTTL = time.Second * 45
)

// address is used to store an ip address along with
// an expiry time at which point the address is considered
// too stale to trust.
type address struct {
	ip        string
	expiresAt time.Time
}

// addressList represents a set of addresses along with
// data used to control and access said addresses.
type addressList struct {
	addresses []*address
	counter   uint32
	mu        sync.RWMutex
}

// expire removes any addresses with an expiry time earlier
// than the current time.
func (a *addressList) expire() {
	a.mu.Lock()
	defer a.mu.Unlock()

	i := 0
	for _, addr := range a.addresses {
		if time.Now().Before(addr.expiresAt) {
			a.addresses[i] = addr
			i++
		}
	}
	for j := i; j < len(a.addresses); j++ {
		a.addresses[j] = nil // clear truncated pointers
	}
	a.addresses = a.addresses[:i] // resize slice
}

// add adds a new address to the address list with a
// maximum expiry time. For existing addresses, the
// expiry time is updated to the maximum.
// TODO: Consider enforcing a maximum address list size.
func (a *addressList) add(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, addr := range a.addresses {
		if addr.ip == ip {
			addr.expiresAt = time.Now().Add(addressTTL)

			return
		}
	}
	a.addresses = append(a.addresses, &address{
		ip:        ip,
		expiresAt: time.Now().Add(addressTTL),
	})
}

// next gets the next address from the list given
// the current round robin implementation.
// There are no guarentees of selection guarentees
// beyond best effort linear iteration.
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
		appAddressesIPv4: make(map[string]*addressList),
		appAddressesIPv6: make(map[string]*addressList),
		refreshChan:      make(chan string),
		logger:           logger,
	}

	go func() {
		for {
			select {
			case appID := <-r.refreshChan: // refresh app addresses on demand.
				if err := r.refreshApp(context.Background(), appID); err != nil {
					r.logger.Warnf(err.Error())
				}
			case <-time.After(refreshInterval): // refresh all app addresses periodically.
				if err := r.refreshAllApps(context.Background()); err != nil {
					r.logger.Warnf(err.Error())
				}
			}
		}
	}()

	return r
}

type resolver struct {
	ipv4Mu           sync.RWMutex
	appAddressesIPv4 map[string]*addressList
	ipv6Mu           sync.RWMutex
	appAddressesIPv6 map[string]*addressList
	refreshChan      chan string
	logger           logger.Logger
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
	// check for cached IPv4 addresses for this app id first.
	m.ipv4Mu.RLock()
	addrListIPv4, ipv4Exists := m.appAddressesIPv4[req.ID]
	m.ipv4Mu.RUnlock()
	if ipv4Exists {
		addrIPv4 := addrListIPv4.next()
		if addrIPv4 != nil {
			m.logger.Debugf("found mDNS IPv4 address in cache: %s", *addrIPv4)

			return *addrIPv4, nil
		}
	}

	// check for cached IPv6 addresses for this app id second.
	m.ipv6Mu.RLock()
	addrListIPv6, ipv6Exists := m.appAddressesIPv6[req.ID]
	m.ipv6Mu.RUnlock()
	if ipv6Exists {
		addrIPv6 := addrListIPv6.next()
		if addrIPv6 != nil {
			m.logger.Debugf("found mDNS IPv6 address in cache: %s", *addrIPv6)

			return *addrIPv6, nil
		}
	}

	// cache miss, fallback to browsing the network for addresses.
	m.logger.Debugf("no mDNS address found in cache, browsing network for app id %s", req.ID)

	// get the first address we receive...
	addr, err := m.browseFirstOnly(context.Background(), req.ID)
	if err == nil {
		// ...and trigger a background refresh for any additional addresses.
		m.refreshChan <- req.ID
	}
	return addr, err
}

// browseFirstOnly will perform a mDNS network browse for an address
// matching the provided app id. It will return the first address it
// receives and stop browsing for any more.
func (m *resolver) browseFirstOnly(ctx context.Context, appID string) (string, error) {
	var addr string

	ctx, cancel := context.WithTimeout(ctx, firstOnlyTimeout)
	defer cancel()

	// onFirst will be invoked on the first address received.
	// Due to the asynchronous nature of cancel() there
	// is no guarentee that this will ONLY be invoked on the
	// first address. Ensure that multiple invokations of this
	// function are safe.
	onFirst := func(ip string) {
		addr = ip
		cancel() // cancel to stop browsing.
	}

	m.logger.Debugf("Browsing for first mDNS address for app id %s", appID)

	err := m.browse(ctx, appID, onFirst)
	if err != nil {
		return "", err
	}

	// wait for the context to be canceled or time out.
	<-ctx.Done()

	switch ctx.Err() {
	case context.Canceled:
		// expect this when we've found an address and canceled the browse.
		m.logger.Debugf("Browsing for first mDNS address for app id %s canceled.", appID)
	case context.DeadlineExceeded:
		// expect this when we've be unable to find the first address before the timeout.
		m.logger.Debugf("Browsing for first mDNS address for app id %s timed out.", appID)
	}

	if addr == "" {
		return "", fmt.Errorf("couldn't find service: %s", appID)
	}

	return addr, nil
}

// refreshApp will perform a mDNS network browse for a provided
// app id. This function is blocking.
func (m *resolver) refreshApp(ctx context.Context, appID string) error {
	m.logger.Debugf("Refreshing mDNS addresses for app id %s.", appID)

	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	if err := m.browse(ctx, appID, nil); err != nil {
		return err
	}

	// wait for the context to be canceled or time out.
	<-ctx.Done()

	switch ctx.Err() {
	case context.Canceled:
		// this is not expected, investigate why context was canceled.
		m.logger.Warnf("Refreshing mDNS addresses for app id %s canceled.", appID)
	case context.DeadlineExceeded:
		// expect this when our browse has timedout.
		m.logger.Debugf("Refreshing mDNS addresses for app id %s timed out.", appID)
	}

	return nil
}

// refreshAllApps will perform a mDNS network browse for each address
// currently in the cache. This function is blocking.
func (m *resolver) refreshAllApps(ctx context.Context) error {
	m.logger.Debugf("Refreshing all mDNS addresses.")

	// check if we have any IPv4 or IPv6 addresses
	// in the address cache that need refreshing.
	m.ipv4Mu.RLock()
	numAppIPv4Addr := len(m.appAddressesIPv4)
	m.ipv4Mu.RUnlock()

	m.ipv6Mu.RLock()
	numAppIPv6Addr := len(m.appAddressesIPv4)
	m.ipv6Mu.RUnlock()

	numApps := numAppIPv4Addr + numAppIPv6Addr
	if numApps == 0 {
		m.logger.Debugf("no mDNS apps to refresh.")

		return nil
	}

	var wg sync.WaitGroup

	// expired addresses will be evicted by getAppIDs()
	for _, appID := range m.getAppIDs() {
		_appID := appID
		go func() {
			wg.Add(1)
			defer wg.Done()

			m.refreshApp(ctx, _appID)
		}()
	}

	// wait for all the app refreshes to complete.
	wg.Wait()

	return nil
}

// browse will perform a non-blocking mdns network browse for the provided app id.
func (m *resolver) browse(ctx context.Context, appID string, onEach func(ip string)) error {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return fmt.Errorf("failed to initialize resolver: %e", err)
	}
	entries := make(chan *zeroconf.ServiceEntry)

	// handle each service entry returned from the mDNS browse.
	go func(results <-chan *zeroconf.ServiceEntry) {
		for {
			select {
			case <-ctx.Done():
				switch ctx.Err() {
				case context.Canceled:
					m.logger.Debugf("mDNS browse for app id %s canceled.", appID)
				case context.DeadlineExceeded:
					m.logger.Debugf("mDNS browse for app id %s timed out.", appID)
				}

				return
			case entry := <-results:
				if entry == nil {
					break
				}

				for _, text := range entry.Text {
					if text != appID {
						m.logger.Debugf("mDNS response doesn't match app id %s, skipping.", appID)

						break
					}

					m.logger.Debugf("mDNS response for app id %s received.", appID)

					hasIPv4Address := len(entry.AddrIPv4) > 0
					hasIPv6Address := len(entry.AddrIPv6) > 0

					if !hasIPv4Address && !hasIPv6Address {
						m.logger.Debugf("mDNS response for app id %s doesn't contain any IPv4 or IPv6 addresses, skipping.", appID)

						break
					}

					var addr string
					port := entry.Port

					// TODO: We currently only use the first IPv4 and IPv6 address.
					// We should understand the cases in which additional addresses
					// are returned and whether we need to support them.
					if hasIPv4Address {
						addr = fmt.Sprintf("%s:%d", entry.AddrIPv4[0].String(), port)
						m.addAppAddressIPv4(appID, addr)
					}
					if hasIPv6Address {
						addr = fmt.Sprintf("%s:%d", entry.AddrIPv6[0].String(), port)
						m.addAppAddressIPv6(appID, addr)
					}

					if onEach != nil {
						onEach(addr) // invoke callback.
					}
				}
			}
		}
	}(entries)

	if err = resolver.Browse(ctx, appID, "local.", entries); err != nil {
		return fmt.Errorf("failed to browse: %s", err.Error())
	}

	return nil
}

// addAppAddressIPv4 adds an IPv4 address to the
// cache for the provided app id.
func (m *resolver) addAppAddressIPv4(appID string, addr string) {
	m.ipv4Mu.Lock()
	defer m.ipv4Mu.Unlock()

	m.logger.Debugf("Adding IPv4 address %s for app id %s cache entry.", addr, appID)
	if _, ok := m.appAddressesIPv4[appID]; !ok {
		m.appAddressesIPv4[appID] = &addressList{}
	}
	m.appAddressesIPv4[appID].add(addr)
}

// addAppIPv4Address adds an IPv6 address to the
// cache for the provided app id.
func (m *resolver) addAppAddressIPv6(appID string, addr string) {
	m.ipv6Mu.Lock()
	defer m.ipv6Mu.Unlock()

	m.logger.Debugf("Adding IPv6 address %s for app id %s cache entry.", addr, appID)
	if _, ok := m.appAddressesIPv6[appID]; !ok {
		m.appAddressesIPv6[appID] = &addressList{}
	}
	m.appAddressesIPv6[appID].add(addr)
}

// getAppIDsIPv4 returns a list of the current IPv4 app IDs.
// This method uses expire on read to evict expired addreses.
func (m *resolver) getAppIDsIPv4() []string {
	m.ipv4Mu.RLock()
	m.ipv4Mu.RUnlock()

	var appIDs []string
	for appID, addr := range m.appAddressesIPv4 {
		old := len(addr.addresses)
		addr.expire()
		m.logger.Debugf("%d IPv4 address(es) expired from the mDNS cache", old-len(addr.addresses))
		appIDs = append(appIDs, appID)
	}

	return appIDs
}

// getAppIDsIPv6 returns a list of the known IPv6 app IDs.
// This method uses expire on read to evict expired addreses.
func (m *resolver) getAppIDsIPv6() []string {
	m.ipv6Mu.RLock()
	defer m.ipv6Mu.RUnlock()

	var appIDs []string
	for appID, addr := range m.appAddressesIPv6 {
		old := len(addr.addresses)
		addr.expire()
		m.logger.Debugf("%d IPv6 address(es) expired from the mDNS cache", old-len(addr.addresses))
		appIDs = append(appIDs, appID)
	}

	return appIDs
}

// getAppIDs returns a list of app ids currently in
// the cache, ensuring expired addresses are evicted.
func (m *resolver) getAppIDs() []string {
	return union(m.getAppIDsIPv4(), m.getAppIDsIPv6())
}

// union merges the elements from two lists into a set.
func union(first []string, second []string) []string {
	keys := make(map[string]struct{})
	for _, id := range first {
		keys[id] = struct{}{}
	}
	for _, id := range second {
		keys[id] = struct{}{}
	}
	result := make([]string, 0, len(keys))
	for id := range keys {
		result = append(result, id)
	}

	return result
}
