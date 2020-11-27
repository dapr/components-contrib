// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"math"
	"testing"
	"time"

	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	request := nr.ResolveRequest{ID: "testAppID", Port: 1234}
	pt, err := resolver.ResolveID(request)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:1234", pt)
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

func TestAddressListNextReturnsNil(t *testing.T) {
	// arrange
	addressList := &addressList{
		addresses: []*address{},
	}

	// act & assert
	require.Nil(t, addressList.next())
}
