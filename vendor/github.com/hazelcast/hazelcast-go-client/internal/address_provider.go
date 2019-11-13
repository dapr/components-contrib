// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
)

type AddressProvider interface {
	LoadAddresses() []core.Address
}

type defaultAddressProvider struct {
	networkConfig *config.NetworkConfig
}

func newDefaultAddressProvider(networkConfig *config.NetworkConfig) *defaultAddressProvider {
	return &defaultAddressProvider{
		networkConfig: networkConfig,
	}
}

func (dap *defaultAddressProvider) LoadAddresses() []core.Address {
	addresses := dap.networkConfig.Addresses()
	possibleAddrs := createAddressesFromString(addresses)
	addrs := make([]core.Address, len(possibleAddrs))
	for i := 0; i < len(possibleAddrs); i++ {
		addrs[i] = core.Address(&possibleAddrs[i])
	}
	return addrs
}
