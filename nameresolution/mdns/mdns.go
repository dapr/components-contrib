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
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

const (
	// browseOneTimeout is the timeout used when
	// browsing for the first response to a single app id.
	browseOneTimeout = time.Second * 1
	// subscriberTimeout is the timeout used when
	// subscribing to the first browser returning a response.
	subscriberTimeout = time.Second * 2
	// subscriberCleanupWait is the time to wait before
	// performing a clean up of a subscriber pool. This
	// MUST be greater than subscriberTimeout.
	subscriberCleanupWait = time.Millisecond * 2500
	// refreshTimeout is the timeout used when
	// browsing for any responses to a single app id.
	refreshTimeout = time.Second * 3
	// refreshInterval is the duration between
	// background address refreshes.
	refreshInterval = time.Second * 30
	// addressTTL is the duration an address has before
	// becoming stale and being evicted.
	addressTTL = time.Second * 60
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
	counter   atomic.Uint32
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

	//nolint:gosec
	l := uint32(len(a.addresses))
	if l == 0 {
		return nil
	}

	if a.counter.Load() == math.MaxUint32 {
		// This will only reset unless another goroutine has done that already
		a.counter.CompareAndSwap(math.MaxUint32, 0)
	}
	counter := a.counter.Add(1) - 1
	addr := a.addresses[counter%l]

	return &addr.ip
}

// SubscriberPool is used to manage
// a pool of subscribers for a given app id.
// 'Once' belongs to the first subscriber as
// it is their responsibility to fetch the
// address associated with the app id and
// publish it to the other subscribers.
// WARN: pools are not thread safe and intended
// to be accessed only when using subMu lock.
type SubscriberPool struct {
	Once        *sync.Once
	Subscribers []Subscriber
}

func NewSubscriberPool(w Subscriber) *SubscriberPool {
	return &SubscriberPool{
		Once:        new(sync.Once),
		Subscribers: []Subscriber{w},
	}
}

func (p *SubscriberPool) Add(sub Subscriber) {
	p.Subscribers = append(p.Subscribers, sub)
}

type Subscriber struct {
	AddrChan chan string
	ErrChan  chan error
}

func (s *Subscriber) Close() {
	close(s.AddrChan)
	close(s.ErrChan)
}

func NewSubscriber() Subscriber {
	return Subscriber{
		AddrChan: make(chan string, 1),
		ErrChan:  make(chan error, 1),
	}
}

// NewResolver creates the instance of mDNS name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	runCtx, runCancel := context.WithCancel(context.Background())

	r := &Resolver{
		subs:             make(map[string]*SubscriberPool),
		appAddressesIPv4: make(map[string]*addressList),
		appAddressesIPv6: make(map[string]*addressList),
		// an app id for every app that is resolved can be pushed
		// onto this channel. We don't want to block the sender as
		// they are resolving the app id as part of the service invocation.
		// Instead we use a buffered channel to balance the load whilst
		// the background refreshes are being performed. Once this buffer
		// becomes full, the sender will block and service invocation
		// will be delayed. We don't expect a single app to resolve
		// too many other app IDs so we set this to a sensible value
		// to avoid over allocating the buffer.
		refreshChan: make(chan string, 36),
		// registrations channel to signal the resolver to
		// stop serving queries for registered app ids.
		registrations: make(map[string]chan struct{}),
		// shutdown refreshers
		runCtx:    runCtx,
		runCancel: runCancel,
		logger:    logger,
	}

	return r
}

type Resolver struct {
	// subscribers are used when multiple callers
	// request the same app ID before it is cached.
	// Only 1 will fetch the address, the rest will
	// subscribe for the address or an error.
	subs  map[string]*SubscriberPool
	subMu sync.RWMutex
	// IPv4 cache is used to store IPv4 addresses.
	ipv4Mu           sync.RWMutex
	appAddressesIPv4 map[string]*addressList
	// IPv6 cache is used to store IPv6 addresses.
	ipv6Mu           sync.RWMutex
	appAddressesIPv6 map[string]*addressList
	// refreshChan is used to trigger background refreshes
	// of app IDs in case there are more than 1 server
	// hosting that app id.
	refreshChan chan string
	// registrations are the app ids that have been
	// registered with this resolver. A single resolver
	// may serve multiple app ids - although this is
	// expected to be 1 when initialized by the dapr runtime.
	registrationMu sync.RWMutex
	registrations  map[string]chan struct{}
	// shutdown refreshes.
	runCtx         context.Context
	runCancel      context.CancelFunc
	serversRunning sync.WaitGroup
	refreshRunning atomic.Bool
	logger         logger.Logger
}

func (m *Resolver) startRefreshers() {
	if !m.refreshRunning.CompareAndSwap(false, true) {
		// Refreshers are already running
		return
	}

	// refresh app addresses periodically and on demand
	go func() {
		defer m.refreshRunning.Store(false)

		t := time.NewTicker(refreshInterval)
		defer t.Stop()

		for {
			select {
			// Refresh on demand
			case appID := <-m.refreshChan:
				go func() {
					if err := m.refreshApp(m.runCtx, appID); err != nil {
						m.logger.Warnf(err.Error())
					}
				}()
			// Refresh periodically
			case <-t.C:
				go func() {
					if err := m.refreshAllApps(m.runCtx); err != nil {
						m.logger.Warnf(err.Error())
					}
				}()
			// Stop on context canceled
			case <-m.runCtx.Done():
				m.logger.Debug("stopping cache refreshes")
				return
			}
		}
	}()
}

// Init registers service for mDNS.
func (m *Resolver) Init(ctx context.Context, metadata nameresolution.Metadata) error {
	if metadata.Instance.AppID == "" {
		return errors.New("name is missing")
	}
	if metadata.Instance.Address == "" {
		return errors.New("address is missing")
	}
	if metadata.Instance.DaprInternalPort <= 0 {
		return errors.New("port is missing or invalid")
	}

	err := m.registerMDNS("", metadata.Instance.AppID, []string{metadata.Instance.Address}, metadata.Instance.DaprInternalPort)
	if err != nil {
		return err
	}

	m.logger.Infof("local service entry announced: %s -> %s:%d", metadata.Instance.AppID, metadata.Instance.Address, metadata.Instance.DaprInternalPort)

	go m.startRefreshers()

	return nil
}

func (m *Resolver) getZeroconfResolver() (resolver *zeroconf.Resolver, err error) {
	// Try with IPv4 + IPv6 first, then IPv4-only, then IPv6-only
	opts := []zeroconf.ClientOption{
		zeroconf.SelectIPTraffic(zeroconf.IPv4AndIPv6),
		zeroconf.SelectIPTraffic(zeroconf.IPv4),
		zeroconf.SelectIPTraffic(zeroconf.IPv6),
	}
	for i := range opts {
		resolver, err = zeroconf.NewResolver(opts[i])
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, errors.New("failed to initialize resolver after attempting IPv4+IPv6, IPv4-only, and IPv6-only")
	}
	return resolver, nil
}

// Close is not formally part of the name resolution interface as proposed
// in https://github.com/dapr/components-contrib/issues/1472 but this is
// used in the tests to clean up the mDNS registration.
func (m *Resolver) Close() error {
	// stop all app ids currently being served from this resolver.
	m.registrationMu.Lock()
	defer m.registrationMu.Unlock()
	for _, doneChan := range m.registrations {
		close(doneChan)
	}
	// clear the registrations map.
	m.registrations = make(map[string]chan struct{})

	// stop all background operations
	if m.runCancel != nil {
		m.runCancel()
	}

	// Wait for all running servers
	m.serversRunning.Wait()

	return nil
}

func (m *Resolver) registerMDNS(instanceID string, appID string, ips []string, port int) error {
	started := make(chan error)

	// Register the app id with the resolver.
	done := make(chan struct{}, 1)
	key := appID + ":" + strconv.Itoa(port) // WARN: we do not support unique ips.

	// NOTE: The registrations map is used to track all registered
	// app ids. The Dapr runtime currently only registers 1 app ID
	// per instance so this is only really used in the tests.
	m.registrationMu.Lock()
	_, exists := m.registrations[key]
	if exists {
		m.registrationMu.Unlock()
		return fmt.Errorf("app id %s already registered for port %d", appID, port)
	}
	m.registrations[key] = done
	m.registrationMu.Unlock()

	m.serversRunning.Add(1)
	go func() {
		var (
			server *zeroconf.Server
			err    error
		)
		defer m.serversRunning.Done()

		host, _ := os.Hostname()
		info := []string{appID}

		// default instance id is unique to the process.
		if instanceID == "" {
			instanceID = host + ":" + strconv.Itoa(syscall.Getpid())
		}

		if len(ips) > 0 {
			server, err = zeroconf.RegisterProxy(instanceID, appID, "local.", port, host, ips, info, nil)
		} else {
			server, err = zeroconf.Register(instanceID, appID, "local.", port, info, nil)
		}

		if err != nil {
			m.logger.Errorf("error from zeroconf register: %s", err)
			started <- err
			close(started)
			return
		}
		close(started)

		// wait until either context is canceled (when the component is shut down) or done is received.
		select {
		case <-m.runCtx.Done():
			m.logger.Debugf("context canceled, shutting down…")
		case <-done:
			m.logger.Debugf("received done signal, shutting down…")
		}

		m.logger.Info("stopping mDNS server for app id: ", appID)
		server.Shutdown()
	}()

	// Could return an error
	return <-started
}

// ResolveID resolves name to address via mDNS.
func (m *Resolver) ResolveID(parentCtx context.Context, req nameresolution.ResolveRequest) (string, error) {
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

	// create a new sub which will wait for an address or error.
	sub := NewSubscriber()

	// add the sub to the pool of subs for this app id.
	m.subMu.Lock()
	appIDSubs, exists := m.subs[req.ID]
	if !exists {
		// WARN: must set appIDSubs variable for use below.
		appIDSubs = NewSubscriberPool(sub)
		m.subs[req.ID] = appIDSubs
	} else {
		appIDSubs.Add(sub)
	}
	m.subMu.Unlock()

	// only one subscriber per pool will perform the first browse for the
	// requested app id. The rest will subscribe for an address or error.
	var once *sync.Once
	var published chan struct{}
	ctx, cancel := context.WithTimeout(parentCtx, browseOneTimeout)
	defer cancel()
	appIDSubs.Once.Do(func() {
		published = make(chan struct{})
		m.browseOne(ctx, req.ID, published)

		// once will only be set for the first browser.
		once = new(sync.Once)
	})

	// if subscribed to the app id but the first browser has already
	// read the subscribers to send the publish event then we may
	// not receive the address or error. The first browser will always
	// update the cache before reading the subscribers so we can
	// recheck the cache here to make sure we get the address or error
	// regardless. This block should not be executed by the first
	// browser as they must wait on the published channel and perform
	// the cleanup before returning.
	if once == nil {
		if addr := m.nextIPv4Address(req.ID); addr != nil {
			return *addr, nil
		}
		if addr := m.nextIPv6Address(req.ID); addr != nil {
			return *addr, nil
		}
	}

	select {
	case addr := <-sub.AddrChan:
		// only 1 subscriber should have set the once var so
		// this block should only get invoked once too.
		if once != nil {
			once.Do(func() {
				// trigger the background refresh for additional addresses.
				// WARN: this can block if refreshChan is full.
				m.refreshChan <- req.ID

				// block on the published channel as this signals that we have
				// published the address to all other subscribers before we return.
				<-published

				// AddrChan is a buffered channel and we cannot guarantee that
				// all subscribers will read the value even though we have published.
				// Therefore it is not safe to remove the subscribers until after
				// any subscribers would have timed out so we run a delayed background
				// cleanup.
				go func() {
					time.Sleep(subscriberCleanupWait)
					m.subMu.Lock()
					delete(m.subs, req.ID)
					m.subMu.Unlock()
				}()
			})
		}
		return addr, nil
	case err := <-sub.ErrChan:
		if once != nil {
			once.Do(func() {
				// block on the published channel as this signals that we have
				// published the error to all other subscribers before we return.
				<-published

				// ErrChan is a buffered channel and we cannot guarantee that
				// all subscribers will read the value even though we have published.
				// Therefore it is not safe to remove the subscribers until after
				// any subscribers would have timed out so we run a delayed background
				// cleanup.
				go func() {
					time.Sleep(subscriberCleanupWait)
					m.subMu.Lock()
					delete(m.subs, req.ID)
					m.subMu.Unlock()
				}()
			})
		}
		return "", err
	case <-time.After(subscriberTimeout):
		// If no address or error has been received
		// within the timeout, we will check the cache again and
		// if no address is present we will return an error.
		if addr := m.nextIPv4Address(req.ID); addr != nil {
			return *addr, nil
		}
		if addr := m.nextIPv6Address(req.ID); addr != nil {
			return *addr, nil
		}
		return "", fmt.Errorf("timeout waiting for address for app id %s", req.ID)
	}
}

// browseOne will perform a mDNS network browse for an address
// matching the provided app id. It will return the first address it
// receives and stop browsing for any more.
// This must be called in a sync.Once block to avoid concurrency issues.
func (m *Resolver) browseOne(ctx context.Context, appID string, published chan struct{}) {
	go func() {
		var addr string

		browseCtx, cancel := context.WithCancel(ctx)
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

		err := m.browse(browseCtx, appID, onFirst)
		if err != nil {
			m.pubErrToSubs(appID, err)

			published <- struct{}{} // signal that all subscribers have been notified.
			return
		}

		// wait for the context to be canceled or time out.
		<-browseCtx.Done()

		if errors.Is(browseCtx.Err(), context.Canceled) {
			// expect this when we've found an address and canceled the browse.
			m.logger.Debugf("Browsing for first mDNS address for app id %s canceled.", appID)
		} else if errors.Is(browseCtx.Err(), context.DeadlineExceeded) {
			// expect this when we've been unable to find the first address before the timeout.
			m.logger.Debugf("Browsing for first mDNS address for app id %s timed out.", appID)
		}

		// if onFirst has been invoked then we should have an address.
		if addr == "" {
			m.pubErrToSubs(appID, fmt.Errorf("couldn't find service: %s", appID))

			published <- struct{}{} // signal that all subscribers have been notified.
			return
		}

		m.pubAddrToSubs(appID, addr)

		published <- struct{}{} // signal that all subscribers have been notified.
	}()
}

func (m *Resolver) pubErrToSubs(reqID string, err error) {
	m.subMu.RLock()
	defer m.subMu.RUnlock()
	pool, ok := m.subs[reqID]
	if !ok {
		// we would always expect atleast 1 subscriber for this reqID.
		m.logger.Warnf("no subscribers found for app id %s", reqID)
		return
	}
	for _, subscriber := range pool.Subscribers {
		// ErrChan is a buffered channel so this is non blocking unless full.
		subscriber.ErrChan <- err
		subscriber.Close()
	}
}

func (m *Resolver) pubAddrToSubs(reqID string, addr string) {
	m.subMu.RLock()
	defer m.subMu.RUnlock()
	pool, ok := m.subs[reqID]
	if !ok {
		// we would always expect atleast 1 subscriber for this reqID.
		m.logger.Warnf("no subscribers found for app id %s", reqID)
		return
	}
	for _, subscriber := range pool.Subscribers {
		// AddrChan is a buffered channel so this is non blocking unless full.
		subscriber.AddrChan <- addr
		subscriber.Close()
	}
}

// refreshApp will perform a mDNS network browse for a provided
// app id. This function is blocking.
func (m *Resolver) refreshApp(refreshCtx context.Context, appID string) error {
	if appID == "" {
		return nil
	}

	m.logger.Debugf("Refreshing mDNS addresses for app id %s.", appID)

	refreshCtx, cancel := context.WithTimeout(refreshCtx, refreshTimeout)
	defer cancel()

	if err := m.browse(refreshCtx, appID, nil); err != nil {
		return err
	}

	// wait for the context to be canceled or time out.
	<-refreshCtx.Done()

	if errors.Is(refreshCtx.Err(), context.Canceled) {
		// this is not expected, investigate why context was canceled.
		m.logger.Warnf("Refreshing mDNS addresses for app id %s canceled.", appID)
	} else if errors.Is(refreshCtx.Err(), context.DeadlineExceeded) {
		// expect this when our browse has timedout.
		m.logger.Debugf("Refreshing mDNS addresses for app id %s timed out.", appID)
	}

	return nil
}

// refreshAllApps will perform a mDNS network browse for each address
// currently in the cache. This function is blocking.
func (m *Resolver) refreshAllApps(ctx context.Context) error {
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

			err := m.refreshApp(ctx, a)
			if err != nil {
				m.logger.Warnf("error refreshing mDNS addresses for app id %s: %v", a, err)
			}
		}(appID)
	}

	// wait for all the app refreshes to complete.
	wg.Wait()

	return nil
}

// browse will perform a non-blocking mdns network browse for the provided app id.
func (m *Resolver) browse(ctx context.Context, appID string, onEach func(ip string)) error {
	entries := make(chan *zeroconf.ServiceEntry)

	handleEntry := func(entry *zeroconf.ServiceEntry) {
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

			// TODO: we currently only use the first IPv4 and IPv6 address.
			// We should understand the cases in which additional addresses
			// are returned and whether we need to support them.
			if hasIPv4Address {
				addr = entry.AddrIPv4[0].String() + ":" + strconv.Itoa(port)
				m.addAppAddressIPv4(appID, addr)
			}
			if hasIPv6Address {
				addr = entry.AddrIPv6[0].String() + ":" + strconv.Itoa(port)
				m.addAppAddressIPv6(appID, addr)
			}

			if onEach != nil {
				onEach(addr) // invoke callback.
			}
		}
	}

	// handle each service entry returned from the mDNS browse.
	go func(results <-chan *zeroconf.ServiceEntry) {
		for {
			select {
			case entry := <-results:
				if entry == nil {
					break
				}
				handleEntry(entry)
			case <-ctx.Done():
				// drain the results before exiting.
				for len(results) > 0 {
					handleEntry(<-results)
				}

				if errors.Is(ctx.Err(), context.Canceled) {
					m.logger.Debugf("mDNS browse for app id %s canceled.", appID)
				} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					m.logger.Debugf("mDNS browse for app id %s timed out.", appID)
				}

				return // stop listening for results.
			}
		}
	}(entries)

	resolver, err := m.getZeroconfResolver()
	if err != nil {
		return err
	}
	return resolver.Browse(ctx, appID, "local.", entries)
}

// addAppAddressIPv4 adds an IPv4 address to the
// cache for the provided app id.
func (m *Resolver) addAppAddressIPv4(appID string, addr string) {
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
func (m *Resolver) addAppAddressIPv6(appID string, addr string) {
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
func (m *Resolver) getAppIDsIPv4() []string {
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
func (m *Resolver) getAppIDsIPv6() []string {
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
func (m *Resolver) getAppIDs() []string {
	return union(m.getAppIDsIPv4(), m.getAppIDsIPv6())
}

// nextIPv4Address returns the next IPv4 address for
// the provided app id from the cache.
func (m *Resolver) nextIPv4Address(appID string) *string {
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
func (m *Resolver) nextIPv6Address(appID string) *string {
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
	keys := make(map[string]struct{}, len(first)+len(second))
	for _, id := range first {
		keys[id] = struct{}{}
	}
	for _, id := range second {
		keys[id] = struct{}{}
	}
	result := make([]string, len(keys))
	i := 0
	for id := range keys {
		result[i] = id
		i++
	}

	return result
}
