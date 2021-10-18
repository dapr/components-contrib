// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	tests := []struct {
		missingProp string
		props       map[string]string
	}{
		{
			"name",
			map[string]string{
				nr.MDNSInstanceAddress: "127.0.0.1",
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
				nr.MDNSInstanceAddress: "127.0.0.1",
			},
		},
		{
			"port",
			map[string]string{
				nr.MDNSInstanceName:    "testAppID",
				nr.MDNSInstanceAddress: "127.0.0.1",
				nr.MDNSInstancePort:    "abcd",
			},
		},
	}

	// arrange
	resolver := NewResolver(logger.NewLogger("test"))

	for _, tt := range tests {
		t.Run(tt.missingProp+" is missing", func(t *testing.T) {
			// act
			err := resolver.Init(nr.Metadata{Properties: tt.props})

			// assert
			require.Error(t, err)
		})
	}
}

func TestResolver(t *testing.T) {
	// arrange
	resolver := NewResolver(logger.NewLogger("test"))
	md := nr.Metadata{Properties: map[string]string{
		nr.MDNSInstanceName:    "testAppID",
		nr.MDNSInstanceAddress: "127.0.0.1",
		nr.MDNSInstancePort:    "1234",
	}}
	expectedAddress := "127.0.0.1:1234"

	// act
	err := resolver.Init(md)
	require.NoError(t, err)

	request := nr.ResolveRequest{ID: "testAppID"}
	pt, err := resolver.ResolveID(request)

	// assert
	require.NoError(t, err)
	require.Equal(t, expectedAddress, pt)
}

func TestResolverMultipleInstances(t *testing.T) {
	// arrange...
	resolver := NewResolver(logger.NewLogger("test"))

	// register instance A
	require.NoError(t, registerAndInitResolver(
		resolver,
		"A",
		"testAppID",
		"127.0.0.1",
		"1234"))
	expectedInstanceAAddress := "127.0.0.1:1234"

	// register instance B
	require.NoError(t, registerAndInitResolver(
		resolver,
		"B",
		"testAppID",
		"127.0.0.1",
		"5678"))
	expectedInstanceBAddress := "127.0.0.1:5678"

	// act...
	request := nr.ResolveRequest{ID: "testAppID"}

	// first resolution will return the first responder's address and trigger a cache refresh.
	addr1, err := resolver.ResolveID(request)
	require.NoError(t, err)
	require.Contains(t, []string{expectedInstanceAAddress, expectedInstanceBAddress}, addr1)

	// delay long enough for the background address cache to populate.
	time.Sleep(1 * time.Second)

	// assert that when we resolve the test app id n times, we see only
	// instance A and instance B and we see them each atleast m times.
	instanceACount := 0
	instanceBCount := 0
	for i := 0; i < 100; i++ {
		addr, err := resolver.ResolveID(request)
		require.NoError(t, err)
		require.Contains(t, []string{expectedInstanceAAddress, expectedInstanceBAddress}, addr)
		if addr == expectedInstanceAAddress {
			instanceACount++
		} else if addr == expectedInstanceBAddress {
			instanceBCount++
		}
	}
	// 45 allows some variation in distribution.
	require.Greater(t, instanceACount, 45)
	require.Greater(t, instanceBCount, 45)
}

// registerAndInitResolver uses an existing resolver to register and a new mDNS instance.
func registerAndInitResolver(resolver nr.Resolver, id, name, address, port string) error {
	instance := nr.Metadata{Properties: map[string]string{
		nr.MDNSInstanceName:    name,
		nr.MDNSInstanceAddress: address,
		nr.MDNSInstancePort:    port,
		nr.MDNSInstanceID:      id,
	}}
	return resolver.Init(instance)
}

func TestAddressListExpire(t *testing.T) {
	// arrange
	base := time.Now()
	expired := base.Add(-60 * time.Second)
	notExpired := base.Add(60 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				value:     "expired0",
				expiresAt: expired,
			},
			{
				value:     "expired1",
				expiresAt: expired,
			},
			{
				value:     "notExpired0",
				expiresAt: notExpired,
			},
		},
	}

	// act
	require.False(t, addressList.expire())

	// assert
	require.Len(t, addressList.addresses, 1)
}

func TestAddressListExpireEmpty(t *testing.T) {
	// arrange
	base := time.Now()
	expired := base.Add(-60 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				value:     "expired0",
				expiresAt: expired,
			},
			{
				value:     "expired1",
				expiresAt: expired,
			},
		},
	}

	// act
	require.True(t, addressList.expire())

	// assert
	require.Len(t, addressList.addresses, 0)
}

func TestAddressListAddNewAddress(t *testing.T) {
	// arrange
	expiry := time.Now().Add(60 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				value:     "addr0",
				expiresAt: expiry,
			},
			{
				value:     "addr1",
				expiresAt: expiry,
			},
		},
	}

	// act
	addressList.add("addr2", addressTTL)

	// assert
	require.Len(t, addressList.addresses, 3)
	require.Equal(t, "addr2", addressList.addresses[2].value)
}

func TestAddressListAddExisitingAddress(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	addressList := &addressList{
		addresses: []address{
			{
				value:     "addr0",
				expiresAt: expiry,
			},
			{
				value:     "addr1",
				expiresAt: expiry,
			},
		},
	}

	// act
	addressList.add("addr1", addressTTL)
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
				value:     "addr0",
				expiresAt: expiry,
			},
			{
				value:     "addr1",
				expiresAt: expiry,
			},
			{
				value:     "addr2",
				expiresAt: expiry,
			},
			{
				value:     "addr3",
				expiresAt: expiry,
			},
			{
				value:     "addr4",
				expiresAt: expiry,
			},
			{
				value:     "addr5",
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
				value:     "addr0",
				expiresAt: expiry,
			},
			{
				value:     "addr1",
				expiresAt: expiry,
			},
			{
				value:     "addr2",
				expiresAt: expiry,
			},
			{
				value:     "addr3",
				expiresAt: expiry,
			},
			{
				value:     "addr4",
				expiresAt: expiry,
			},
			{
				value:     "addr5",
				expiresAt: expiry,
			},
		},
	}

	// act & assert
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
	addressList.counter = maxInt
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
				value:     "addr0",
				expiresAt: expiry,
			},
			{
				value:     "addr1",
				expiresAt: expiry,
			},
			{
				value:     "addr2",
				expiresAt: expiry,
			},
			{
				value:     "addr3",
				expiresAt: expiry,
			},
			{
				value:     "addr4",
				expiresAt: expiry,
			},
			{
				value:     "addr5",
				expiresAt: expiry,
			},
		},
	}

	// act & assert
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
	addressList.add("addr6", addressTTL)
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
				value:     "addr0",
				expiresAt: expired,
			},
			{
				value:     "addr1",
				expiresAt: expiry,
			},
			{
				value:     "addr2",
				expiresAt: expired,
			},
			{
				value:     "addr3",
				expiresAt: expiry,
			},
			{
				value:     "addr4",
				expiresAt: expired,
			},
			{
				value:     "addr5",
				expiresAt: expiry,
			},
		},
	}

	// act & assert
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
	require.Equal(t, "addr3", *addressList.next())
	require.False(t, addressList.expire())
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

func TestAddAddressIncrementsCount(t *testing.T) {
	// arrange
	logger := logger.NewLogger("test")
	var addresses sync.Map
	var addressesCount atomic.Uint32
	ttl := time.Second * 10
	appAID := "testAppIDA"
	appBID := "testAppIDB"

	expectedAddressesCount := 2

	// act
	addAddress(logger, ipFamilyIPv4, &addresses, &addressesCount, appAID, "127.0.0.1:1234", ttl)
	addAddress(logger, ipFamilyIPv4, &addresses, &addressesCount, appAID, "127.0.0.1:5678", ttl)
	addAddress(logger, ipFamilyIPv4, &addresses, &addressesCount, appBID, "127.0.0.1:8080", ttl)

	// assert
	appAAddressesInterface, loaded := addresses.Load(appAID)
	require.True(t, loaded)
	appAAddresses := appAAddressesInterface.(*addressList)
	require.Equal(t, 2, len(appAAddresses.addresses))

	appBAddressesInterface, loaded := addresses.Load(appBID)
	require.True(t, loaded)
	appBAddresses := appBAddressesInterface.(*addressList)
	require.Equal(t, 1, len(appBAddresses.addresses))

	require.Equal(t, expectedAddressesCount, int(addressesCount.Load()))
}

func TestExpireAddressesDecrementsCount(t *testing.T) {
	// arrange
	logger := logger.NewLogger("test")
	var addresses sync.Map
	var addressesCount atomic.Uint32

	expired := time.Now().Add(-60 * time.Second)
	notExpired := time.Now().Add(60 * time.Second)
	appAAddresses := &addressList{
		addresses: []address{
			{
				value:     "addr0",
				expiresAt: expired,
			},
		},
	}
	appBAddresses := &addressList{
		addresses: []address{
			{
				value:     "addr0",
				expiresAt: notExpired,
			},
		},
	}

	addresses.Store("appA", appAAddresses)
	addresses.Store("appB", appBAddresses)
	addressesCount.Store(2)
	require.Equal(t, 2, int(addressesCount.Load()))
	_, loaded := addresses.Load("appA")
	require.True(t, loaded)
	_, loaded = addresses.Load("appB")
	require.True(t, loaded)

	// act
	expireAddresses(logger, ipFamilyIPv4, &addresses, &addressesCount)

	// assert
	require.Equal(t, 1, int(addressesCount.Load()))
	_, loaded = addresses.Load("appA")
	require.False(t, loaded)
	_, loaded = addresses.Load("appB")
	require.True(t, loaded)
}

func TestNextAddressReturnsNilForUnknownAppID(t *testing.T) {
	// arrange
	logger := logger.NewLogger("test")
	var addresses sync.Map

	// act and assert
	require.Nil(t, nextAddress(logger, ipFamilyIPv4, &addresses, "AppA"))
}

func TestNextAddressReturnsAddressListForKnownAppID(t *testing.T) {
	// arrange
	logger := logger.NewLogger("test")
	var addresses sync.Map
	var addressesCount atomic.Uint32

	notExpired := time.Now().Add(60 * time.Second)
	appAAddresses := &addressList{
		addresses: []address{
			{
				value:     "addr0",
				expiresAt: notExpired,
			},
		},
	}
	appBAddresses := &addressList{
		addresses: []address{
			{
				value:     "addr0",
				expiresAt: notExpired,
			},
		},
	}
	addresses.Store("appA", appAAddresses)
	addresses.Store("appB", appBAddresses)
	addressesCount.Store(2)

	require.Equal(t, "addr0", *nextAddress(logger, ipFamilyIPv4, &addresses, "appB"))
}
