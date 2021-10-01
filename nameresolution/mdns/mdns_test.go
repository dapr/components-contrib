// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			assert.Error(t, err)
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

	// act
	err := resolver.Init(md)
	require.NoError(t, err)

	request := nr.ResolveRequest{ID: "testAppID"}
	pt, err := resolver.ResolveID(request)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:1234", pt)
}

func TestResolverMultipleInstances(t *testing.T) {
	// arrange...
	resolver := NewResolver(logger.NewLogger("test"))

	// register instance A
	instanceAID := "A"
	instanceAName := "testAppID"
	instanceAAddress := "127.0.0.1"
	instanceAPort := "1234"
	instanceAPQDN := fmt.Sprintf("%s:%s", instanceAAddress, instanceAPort)

	instanceA := nr.Metadata{Properties: map[string]string{
		nr.MDNSInstanceName:    instanceAName,
		nr.MDNSInstanceAddress: instanceAAddress,
		nr.MDNSInstancePort:    instanceAPort,
		nr.MDNSInstanceID:      instanceAID,
	}}
	err1 := resolver.Init(instanceA)
	require.NoError(t, err1)

	// register instance B
	instanceBID := "B"
	instanceBName := "testAppID"
	instanceBAddress := "127.0.0.1"
	instanceBPort := "5678"
	instanceBPQDN := fmt.Sprintf("%s:%s", instanceBAddress, instanceBPort)

	instanceB := nr.Metadata{Properties: map[string]string{
		nr.MDNSInstanceName:    instanceBName,
		nr.MDNSInstanceAddress: instanceBAddress,
		nr.MDNSInstancePort:    instanceBPort,
		nr.MDNSInstanceID:      instanceBID,
	}}
	err2 := resolver.Init(instanceB)
	require.NoError(t, err2)

	// act...
	request := nr.ResolveRequest{ID: "testAppID"}

	// first resolution will return the first responder's address and trigger a cache refresh.
	addr1, err := resolver.ResolveID(request)
	require.NoError(t, err)
	require.Contains(t, []string{instanceAPQDN, instanceBPQDN}, addr1)

	// delay long enough for the background address cache to populate.
	time.Sleep(1 * time.Second)

	// assert that when we resolve the test app id n times, we see only
	// instance A and instance B and we see them each atleast m times.
	instanceACount := 0
	instanceBCount := 0
	for i := 0; i < 100; i++ {
		addr, err := resolver.ResolveID(request)
		require.NoError(t, err)
		require.Contains(t, []string{instanceAPQDN, instanceBPQDN}, addr)
		if addr == instanceAPQDN {
			instanceACount++
		} else if addr == instanceBPQDN {
			instanceBCount++
		}
	}
	// 45 allows some variation in distribution.
	require.Greater(t, instanceACount, 45)
	require.Greater(t, instanceBCount, 45)
}

func TestAddressListExpire(t *testing.T) {
	// arrange
	base := time.Now()
	expired := base.Add(-60 * time.Second)
	notExpired := base.Add(60 * time.Second)
	addressList := &addressList{
		addresses: []*address{
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
		addresses: []*address{
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
		addresses: []*address{
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
	require.Greater(t, deltaSec, 0)
}

func TestAddressListNext(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	addressList := &addressList{
		addresses: []*address{
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
		addresses: []*address{
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
	addressList.counter = math.MaxUint32
	require.Equal(t, "addr0", *addressList.next())
	require.Equal(t, "addr1", *addressList.next())
	require.Equal(t, "addr2", *addressList.next())
}

func TestAddressListNextNoAddress(t *testing.T) {
	// arrange
	addressList := &addressList{
		addresses: []*address{},
	}

	// act & assert
	require.Nil(t, addressList.next())
}

func TestAddressListNextWithAdd(t *testing.T) {
	// arrange
	expiry := time.Now().Add(10 * time.Second)
	addressList := &addressList{
		addresses: []*address{
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
		addresses: []*address{
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
