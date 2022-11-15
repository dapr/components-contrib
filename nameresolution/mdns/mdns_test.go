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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

const (
	localhost      = "127.0.0.1"
	numConcurrency = 100
)

func TestInitMetadata(t *testing.T) {
	tests := []struct {
		missingProp string
		props       map[string]string
	}{
		{
			"name",
			map[string]string{
				nr.MDNSInstanceAddress: localhost,
				nr.MDNSInstancePort:    "30003",
			},
		},
		{
			"address",
			map[string]string{
				nr.MDNSInstanceName: "testAppID",
				nr.MDNSInstancePort: "30003",
			},
		},
		{
			"port",
			map[string]string{
				nr.MDNSInstanceName:    "testAppID",
				nr.MDNSInstanceAddress: localhost,
			},
		},
		{
			"port",
			map[string]string{
				nr.MDNSInstanceName:    "testAppID",
				nr.MDNSInstanceAddress: localhost,
				nr.MDNSInstancePort:    "abcd",
			},
		},
	}

	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()

	for _, tt := range tests {
		t.Run(tt.missingProp+" is missing", func(t *testing.T) {
			// act
			err := resolver.Init(nr.Metadata{Base: metadata.Base{Properties: tt.props}})

			// assert
			assert.Error(t, err)
		})
	}
}

func TestInitRegister(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()
	md := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: localhost,
		nr.MDNSInstancePort:    "1234",
	}}}

	// act
	err := resolver.Init(md)
	require.NoError(t, err)
}

func TestInitRegisterDuplicate(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()
	md := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: localhost,
		nr.MDNSInstancePort:    "1234",
	}}}
	md2 := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: localhost,
		nr.MDNSInstancePort:    "1234",
	}}}

	// act
	err := resolver.Init(md)
	require.NoError(t, err)
	err = resolver.Init(md2)
	expectedError := "app id testAppID already registered for port 1234"
	require.EqualErrorf(t, err, expectedError, "Error should be: %v, got %v", expectedError, err)
}

func TestResolver(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()
	md := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: localhost,
		nr.MDNSInstancePort:    "1234",
	}}}

	// act
	err := resolver.Init(md)
	require.NoError(t, err)

	request := nr.ResolveRequest{ID: "testAppID"}
	pt, err := resolver.ResolveID(request)

	// assert
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:1234", localhost), pt)
}

func TestResolverClose(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	md := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: localhost,
		nr.MDNSInstancePort:    "1234",
	}}}

	// act
	err := resolver.Init(md)
	require.NoError(t, err)

	request := nr.ResolveRequest{ID: "testAppID"}
	pt, err := resolver.ResolveID(request)

	// assert
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:1234", localhost), pt)

	// act again
	err = resolver.Close()

	// assert
	require.NoError(t, err)
}

func TestResolverMultipleInstances(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()

	// register instance A
	instanceAID := "A"
	instanceAName := "testAppID"
	instanceAAddress := localhost
	instanceAPort := "1234"
	instanceAPQDN := fmt.Sprintf("%s:%s", instanceAAddress, instanceAPort)

	instanceA := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    instanceAName,
		nr.MDNSInstanceAddress: instanceAAddress,
		nr.MDNSInstancePort:    instanceAPort,
		nr.MDNSInstanceID:      instanceAID,
	}}}
	err1 := resolver.Init(instanceA)
	require.NoError(t, err1)

	// register instance B
	instanceBID := "B"
	instanceBName := "testAppID"
	instanceBAddress := localhost
	instanceBPort := "5678"
	instanceBPQDN := fmt.Sprintf("%s:%s", instanceBAddress, instanceBPort)

	instanceB := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    instanceBName,
		nr.MDNSInstanceAddress: instanceBAddress,
		nr.MDNSInstancePort:    instanceBPort,
		nr.MDNSInstanceID:      instanceBID,
	}}}
	err2 := resolver.Init(instanceB)
	require.NoError(t, err2)

	// act...
	request := nr.ResolveRequest{ID: "testAppID"}

	// first resolution will return the first responder's address and trigger a cache refresh.
	addr1, err := resolver.ResolveID(request)
	require.NoError(t, err)
	require.Contains(t, []string{instanceAPQDN, instanceBPQDN}, addr1)

	// we want the resolution to be served from the cache so that it can
	// be load balanced rather than received by a subscription. Therefore,
	// we must sleep here long enough to allow the first browser to populate
	// the cache.
	time.Sleep(1 * time.Second)

	// assert that when we resolve the test app id n times, we see only
	// instance A and instance B and we see them each atleast m times.
	instanceACount := atomic.Uint32{}
	instanceBCount := atomic.Uint32{}
	for i := 0; i < 100; i++ {
		addr, err := resolver.ResolveID(request)
		require.NoError(t, err)
		require.Contains(t, []string{instanceAPQDN, instanceBPQDN}, addr)
		if addr == instanceAPQDN {
			instanceACount.Add(1)
		} else if addr == instanceBPQDN {
			instanceBCount.Add(1)
		}
	}
	// 45 allows some variation in distribution.
	require.Greater(t, instanceACount.Load(), uint32(45))
	require.Greater(t, instanceBCount.Load(), uint32(45))
}

func TestResolverNotFound(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()

	// act
	request := nr.ResolveRequest{ID: "testAppIDNotFound"}
	pt, err := resolver.ResolveID(request)

	// assert
	expectedError := "couldn't find service: testAppIDNotFound"
	require.EqualErrorf(t, err, expectedError, "Error should be: %v, got %v", expectedError, err)
	assert.Equal(t, "", pt)
}

// TestResolverConcurrency is used to run concurrent tests in
// series as they rely on a shared mDNS server on the host
// machine.
func TestResolverConcurrency(t *testing.T) {
	tt := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "ResolverConcurrencyNotFound",
			test: ResolverConcurrencyNotFound,
		},
		{
			name: "ResolverConcurrencyFound",
			test: ResolverConcurrencyFound,
		},
		{
			name: "ResolverConcurrencySubscriberClear",
			test: ResolverConcurrencySubsriberClear,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, tc.test)
	}
}

// WARN: This is deliberately not a test function.
// This test case must be run in serial and is executed
// by the TestResolverConcurrency test function.
func ResolverConcurrencySubsriberClear(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()
	md := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: localhost,
		nr.MDNSInstancePort:    "1234",
	}}}

	// act
	err := resolver.Init(md)
	require.NoError(t, err)

	request := nr.ResolveRequest{ID: "testAppID"}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			pt, err := resolver.ResolveID(request)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%s:1234", localhost), pt)
		}()
	}

	wg.Wait()

	// Wait long enough for the background clear to occur.
	time.Sleep(3 * time.Second)
	require.Equal(t, 0, len(resolver.subs))
}

// WARN: This is deliberately not a test function.
// This test case must be run in serial and is executed
// by the TestResolverConcurrency test function.
func ResolverConcurrencyFound(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()

	// register instance A
	appAID := "A"
	appAName := "testAppA"
	appAAddress := localhost
	appAPort := "1234"
	appAPQDN := fmt.Sprintf("%s:%s", appAAddress, appAPort)

	appA := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    appAName,
		nr.MDNSInstanceAddress: appAAddress,
		nr.MDNSInstancePort:    appAPort,
		nr.MDNSInstanceID:      appAID,
	}}}
	err1 := resolver.Init(appA)
	require.NoError(t, err1)

	// register instance B
	appBID := "B"
	appBName := "testAppB"
	appBAddress := localhost
	appBPort := "5678"
	appBBPQDN := fmt.Sprintf("%s:%s", appBAddress, appBPort)

	appB := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    appBName,
		nr.MDNSInstanceAddress: appBAddress,
		nr.MDNSInstancePort:    appBPort,
		nr.MDNSInstanceID:      appBID,
	}}}
	err2 := resolver.Init(appB)
	require.NoError(t, err2)

	// register instance C
	appCID := "C"
	appCName := "testAppC"
	appCAddress := localhost
	appCPort := "3456"
	appCBPQDN := fmt.Sprintf("%s:%s", appCAddress, appCPort)

	appC := nr.Metadata{Base: metadata.Base{Properties: map[string]string{
		nr.MDNSInstanceName:    appCName,
		nr.MDNSInstanceAddress: appCAddress,
		nr.MDNSInstancePort:    appCPort,
		nr.MDNSInstanceID:      appCID,
	}}}
	err3 := resolver.Init(appC)
	require.NoError(t, err3)

	// act...
	wg := sync.WaitGroup{}
	for i := 0; i < numConcurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var appID string
			r := i % 3
			if r == 0 {
				appID = "testAppA"
			} else if r == 1 {
				appID = "testAppB"
			} else {
				appID = "testAppC"
			}
			request := nr.ResolveRequest{ID: appID}

			start := time.Now()
			pt, err := resolver.ResolveID(request)
			elapsed := time.Since(start)
			// assert
			require.NoError(t, err)
			if r == 0 {
				assert.Equal(t, appAPQDN, pt)
			} else if r == 1 {
				assert.Equal(t, appBBPQDN, pt)
			} else if r == 2 {
				assert.Equal(t, appCBPQDN, pt)
			}

			// It should take a maximum of 3 seconds to resolve an address.
			assert.Less(t, elapsed, 3*time.Second)
		}(i)
	}

	wg.Wait()
}

// WARN: This is deliberately not a test function.
// This test case must be run in serial and is executed
// by the TestResolverConcurrency test function.
func ResolverConcurrencyNotFound(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test")).(*Resolver)
	defer resolver.Close()

	// act...
	wg := sync.WaitGroup{}
	for i := 0; i < numConcurrency; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			var appID string
			r := idx % 3
			if r == 0 {
				appID = "testAppA"
			} else if r == 1 {
				appID = "testAppB"
			} else {
				appID = "testAppC"
			}
			request := nr.ResolveRequest{ID: appID}

			// act
			start := time.Now()
			pt, err := resolver.ResolveID(request)
			elapsed := time.Since(start)

			// assert
			expectedError := "couldn't find service: " + appID
			require.EqualErrorf(t, err, expectedError, "Error should be: %v, got %v", expectedError, err)
			assert.Equal(t, "", pt)
			assert.Less(t, elapsed, 2*time.Second) // browse timeout is 1 second, so we expect an error shortly after.
		}()
	}

	wg.Wait()
}

func TestAddressListExpire(t *testing.T) {
	// arrange
	base := time.Now()
	expired := base.Add(-60 * time.Second)
	notExpired := base.Add(60 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				ip:        "expired0",
				expiresAt: expired,
			},
			{
				ip:        "expired1",
				expiresAt: expired,
			},
			{
				ip:        "notExpired0",
				expiresAt: notExpired,
			},
		},
	}

	// act
	addressList.expire()

	// assert
	require.Len(t, addressList.addresses, 1)
}

func TestAddressListAddNewAddress(t *testing.T) {
	// arrange
	expiry := time.Now().Add(60 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				ip:        "addr0",
				expiresAt: expiry,
			},
			{
				ip:        "addr1",
				expiresAt: expiry,
			},
		},
	}

	// act
	addressList.add("addr2")

	// assert
	require.Len(t, addressList.addresses, 3)
	require.Equal(t, "addr2", addressList.addresses[2].ip)
}

func TestAddressListAddExisitingAddress(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				ip:        "addr0",
				expiresAt: expiry,
			},
			{
				ip:        "addr1",
				expiresAt: expiry,
			},
		},
	}

	// act
	addressList.add("addr1")
	deltaSec := int(addressList.addresses[1].expiresAt.Sub(expiry).Seconds())

	// assert
	require.Len(t, addressList.addresses, 2)
	require.Greater(t, deltaSec, 0) // Ensures expiry has been extended for existing address.
}

func TestAddressListNext(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				ip:        "addr0",
				expiresAt: expiry,
			},
			{
				ip:        "addr1",
				expiresAt: expiry,
			},
			{
				ip:        "addr2",
				expiresAt: expiry,
			},
			{
				ip:        "addr3",
				expiresAt: expiry,
			},
			{
				ip:        "addr4",
				expiresAt: expiry,
			},
			{
				ip:        "addr5",
				expiresAt: expiry,
			},
		},
	}

	// act & assert
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
	require.Equal(t, "addr4", *addressList.next())
	require.Equal(t, "addr5", *addressList.next())
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
}

func TestAddressListNextMaxCounter(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				ip:        "addr0",
				expiresAt: expiry,
			},
			{
				ip:        "addr1",
				expiresAt: expiry,
			},
			{
				ip:        "addr2",
				expiresAt: expiry,
			},
			{
				ip:        "addr3",
				expiresAt: expiry,
			},
			{
				ip:        "addr4",
				expiresAt: expiry,
			},
			{
				ip:        "addr5",
				expiresAt: expiry,
			},
		},
	}

	// act & assert
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
	addressList.counter.Store(math.MaxUint32)
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
}

func TestAddressListNextNoAddress(t *testing.T) {
	// arrange
	addressList := &addressList{
		addresses: []address{},
	}

	// act & assert
	require.Nil(t, addressList.next())
}

func TestAddressListNextWithAdd(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				ip:        "addr0",
				expiresAt: expiry,
			},
			{
				ip:        "addr1",
				expiresAt: expiry,
			},
			{
				ip:        "addr2",
				expiresAt: expiry,
			},
			{
				ip:        "addr3",
				expiresAt: expiry,
			},
			{
				ip:        "addr4",
				expiresAt: expiry,
			},
			{
				ip:        "addr5",
				expiresAt: expiry,
			},
		},
	}

	// act & assert
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
	addressList.add("addr6")
	require.Equal(t, "addr4", *addressList.next())
	require.Equal(t, "addr5", *addressList.next())
	require.Equal(t, "addr6", *addressList.next())
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
}

func TestAddressListNextWithExpiration(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	expired := time.Now().Add(-60 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				ip:        "addr0",
				expiresAt: expired,
			},
			{
				ip:        "addr1",
				expiresAt: expiry,
			},
			{
				ip:        "addr2",
				expiresAt: expired,
			},
			{
				ip:        "addr3",
				expiresAt: expiry,
			},
			{
				ip:        "addr4",
				expiresAt: expired,
			},
			{
				ip:        "addr5",
				expiresAt: expiry,
			},
		},
	}

	// act & assert
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
	addressList.expire()
	require.Equal(t, "addr3", *addressList.next())
	require.Equal(t, "addr5", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
}

func TestUnion(t *testing.T) {
	// arrange
	testCases := []struct {
		first    []string
		second   []string
		expected []string
	}{
		{
			first:    []string{"a", "b", "c"},
			second:   []string{"a", "c", "d", "e"},
			expected: []string{"a", "b", "c", "d", "e"},
		},
		{
			first:    []string{"a", "b", "c"},
			second:   []string{"d", "e", "f", "g"},
			expected: []string{"a", "b", "c", "d", "e", "f", "g"},
		},
		{
			first:    []string{"a", "b"},
			second:   []string{"a", "b"},
			expected: []string{"a", "b"},
		},
		{
			first:    []string{"a", "b"},
			second:   []string{"b", "a"},
			expected: []string{"a", "b"},
		},
	}
	for _, tt := range testCases {
		// act
		union := union(tt.first, tt.second)

		// assert
		var matches int
		for _, i1 := range tt.expected {
			for _, i2 := range union {
				if i1 == i2 {
					matches++
				}
			}
		}
		require.Equal(t, len(tt.expected), matches)
	}
}
