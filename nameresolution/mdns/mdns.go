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

package mdns

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

const (
	// firstOnlyTimeout is the timeout used when
	// browsing for the first response to a single app id.
	firstOnlyTimeout = time.Second * 1
	// refreshTimeout is the timeout used when
	// browsing for any responses to a single app id.
	refreshTimeout = time.Second * 3
	// refreshInterval is the duration between
	// background address refreshes.
	refreshInterval = time.Second * 30
	// addressTTL is the duration an address has before
	// becoming stale and being evicted.
	addressTTL = time.Second * 60
	// max integer value supported on this architecture.
	maxInt = int(^uint(0) >> 1)
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
	addresses []address
	counter   int
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
	a.addresses = a.addresses[:i]
}

// add adds a new address to the address list with a
// maximum expiry time. For existing addresses, the
// expiry time is updated to the maximum.
// TODO: Consider enforcing a maximum address list size.
func (a *addressList) add(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i := range a.addresses {
		if a.addresses[i].ip == ip {
			a.addresses[i].expiresAt = time.Now().Add(addressTTL)

			return
		}
	}
	a.addresses = append(a.addresses, address{
		ip:        ip,
		expiresAt: time.Now().Add(addressTTL),
	})
}

// next gets the next address from the list given
// the current round robin implementation.
// There are no guarantees on the selection
// beyond best effort linear iteration.
func (a *addressList) next() *string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.addresses) == 0 {
		return nil
	}

	if a.counter == maxInt {
		a.counter = 0
	}
	index := a.counter % len(a.addresses)
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

	// refresh app addresses on demand.
	go func() {
		for appID := range r.refreshChan {
			if err := r.refreshApp(context.Background(), appID); err != nil {
				r.logger.Warnf(err.Error())
			}
		}
	}()

	// refresh all app addresses periodically.
	go func() {
		for {
			time.Sleep(refreshInterval)

			if err := r.refreshAllApps(context.Background()); err != nil {
				r.logger.Warnf(err.Error())
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
	var appID string
	var hostAddress string
	var ok bool
	var instanceID string

	props := metadata.Properties

	if appID, ok = props[nameresolution.MDNSInstanceName]; !ok {
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

	if instanceID, ok = props[nameresolution.MDNSInstanceID]; !ok {
		instanceID = ""
	}

	err = m.registerMDNS(instanceID, appID, []string{hostAddress}, int(port))
	if err == nil {
		m.logger.Infof("local service entry announced: %s -> %s:%d", appID, hostAddress, port)
	}

	return err
}

func (m *resolver) registerMDNS(instanceID string, appID string, ips []string, port int) error {
	started := make(chan bool, 1)
	var err error

	go func() {
		var server *zeroconf.Server

		host, _ := os.Hostname()
		info := []string{appID}

		// default instance id is unique to the process.
		if instanceID == "" {
			instanceID = fmt.Sprintf("%s-%d", host, syscall.Getpid())
		}

		if len(ips) > 0 {
			server, err = zeroconf.RegisterProxy(instanceID, appID, "local.", port, host, ips, info, nil)
		} else {
			server, err = zeroconf.Register(instanceID, appID, "local.", port, info, nil)
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
	if addr := m.nextIPv4Address(req.ID); addr != nil {
		return *addr, nil
	}

	// check for cached IPv6 addresses for this app id second.
	if addr := m.nextIPv6Address(req.ID); addr != nil {
		return *addr, nil
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
	// is no guarantee that this will ONLY be invoked on the
	// first address. Ensure that multiple invocations of this
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

	if errors.Is(ctx.Err(), context.Canceled) {
		// expect this when we've found an address and canceled the browse.
		m.logger.Debugf("Browsing for first mDNS address for app id %s canceled.", appID)
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// expect this when we've been unable to find the first address before the timeout.
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
	if appID == "" {
		return nil
	}

	m.logger.Debugf("Refreshing mDNS addresses for app id %s.", appID)

	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	if err := m.browse(ctx, appID, nil); err != nil {
		return err
	}

	// wait for the context to be canceled or time out.
	<-ctx.Done()

	if errors.Is(ctx.Err(), context.Canceled) {
		// this is not expected, investigate why context was canceled.
		m.logger.Warnf("Refreshing mDNS addresses for app id %s canceled.", appID)
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// expect this when our browse has timedout.
		m.logger.Debugf("Refreshing mDNS addresses for app id %s timed out.", appID)
	}

	return nil
}

// refreshAllApps will perform a mDNS network browse for each address
// currently in the cache. This function is blocking.
func (m *resolver) refreshAllApps(ctx context.Context) error {
	m.logger.Debug("Refreshing all mDNS addresses.")

	// check if we have any IPv4 or IPv6 addresses
	// in the address cache that need refreshing.
	m.ipv4Mu.RLock()
	numAppIPv4Addr := len(m.appAddressesIPv4)
	m.ipv4Mu.RUnlock()

	m.ipv6Mu.RLock()
	numAppIPv6Addr := len(m.appAddressesIPv6)
	m.ipv6Mu.RUnlock()

	numApps := numAppIPv4Addr + numAppIPv6Addr
	if numApps == 0 {
		m.logger.Debug("no mDNS apps to refresh.")

		return nil
	}

	var wg sync.WaitGroup

	// expired addresses will be evicted by getAppIDs()
	for _, appID := range m.getAppIDs() {
		wg.Add(1)

		go func(a string) {
			defer wg.Done()

			m.refreshApp(ctx, a)
		}(appID)
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
				if errors.Is(ctx.Err(), context.Canceled) {
					m.logger.Debugf("mDNS browse for app id %s canceled.", appID)
				} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
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
		var addrList addressList
		m.appAddressesIPv4[appID] = &addrList
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
		var addrList addressList
		m.appAddressesIPv6[appID] = &addrList
	}
	m.appAddressesIPv6[appID].add(addr)
}

// getAppIDsIPv4 returns a list of the current IPv4 app IDs.
// This method uses expire on read to evict expired addreses.
func (m *resolver) getAppIDsIPv4() []string {
	m.ipv4Mu.RLock()
	defer m.ipv4Mu.RUnlock()

	appIDs := make([]string, 0, len(m.appAddressesIPv4))
	for appID, addr := range m.appAddressesIPv4 {
		old := len(addr.addresses)
		addr.expire()
		m.logger.Debugf("%d IPv4 address(es) expired for app id %s.", old-len(addr.addresses), appID)
		appIDs = append(appIDs, appID)
	}

	return appIDs
}

// getAppIDsIPv6 returns a list of the known IPv6 app IDs.
// This method uses expire on read to evict expired addreses.
func (m *resolver) getAppIDsIPv6() []string {
	m.ipv6Mu.RLock()
	defer m.ipv6Mu.RUnlock()

	appIDs := make([]string, 0, len(m.appAddressesIPv6))
	for appID, addr := range m.appAddressesIPv6 {
		old := len(addr.addresses)
		addr.expire()
		m.logger.Debugf("%d IPv6 address(es) expired for app id %s.", old-len(addr.addresses), appID)
		appIDs = append(appIDs, appID)
	}

	return appIDs
}

// getAppIDs returns a list of app ids currently in
// the cache, ensuring expired addresses are evicted.
func (m *resolver) getAppIDs() []string {
	return union(m.getAppIDsIPv4(), m.getAppIDsIPv6())
}

// nextIPv4Address returns the next IPv4 address for
// the provided app id from the cache.
func (m *resolver) nextIPv4Address(appID string) *string {
	m.ipv4Mu.RLock()
	defer m.ipv4Mu.RUnlock()
	addrList, exists := m.appAddressesIPv4[appID]
	if exists {
		addr := addrList.next()
		if addr != nil {
			m.logger.Debugf("found mDNS IPv4 address in cache: %s", *addr)

			return addr
		}
	}

	return nil
}

// nextIPv6Address returns the next IPv6 address for
// the provided app id from the cache.
func (m *resolver) nextIPv6Address(appID string) *string {
	m.ipv6Mu.RLock()
	defer m.ipv6Mu.RUnlock()
	addrList, exists := m.appAddressesIPv6[appID]
	if exists {
		addr := addrList.next()
		if addr != nil {
			m.logger.Debugf("found mDNS IPv6 address in cache: %s", *addr)

			return addr
		}
	}

	return nil
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
