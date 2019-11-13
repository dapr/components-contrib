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
	"sync"
	"sync/atomic"

	"fmt"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

type proxyManager struct {
	ReferenceID int64
	client      *HazelcastClient
	mu          sync.RWMutex // guards proxies
	proxies     map[string]core.DistributedObject
}

func newProxyManager(client *HazelcastClient) *proxyManager {
	return &proxyManager{
		ReferenceID: 0,
		client:      client,
		proxies:     make(map[string]core.DistributedObject),
	}
}

func (pm *proxyManager) nextReferenceID() int64 {
	return atomic.AddInt64(&pm.ReferenceID, 1)
}

func (pm *proxyManager) getOrCreateProxy(serviceName string, name string) (core.DistributedObject, error) {
	var ns = serviceName + name
	pm.mu.RLock()
	if _, ok := pm.proxies[ns]; ok {
		defer pm.mu.RUnlock()
		return pm.proxies[ns], nil
	}
	pm.mu.RUnlock()
	proxy, err := pm.createProxy(serviceName, name)
	if err != nil {
		return nil, err
	}
	pm.mu.Lock()
	pm.proxies[ns] = proxy
	pm.mu.Unlock()
	return proxy, nil
}

func (pm *proxyManager) createProxy(serviceName string, name string) (core.DistributedObject, error) {
	message := proto.ClientCreateProxyEncodeRequest(name, serviceName, pm.findNextProxyAddress().(*proto.Address))
	_, err := pm.client.InvocationService.invokeOnRandomTarget(message).Result()
	if err != nil {
		return nil, err
	}
	return pm.getProxyByNameSpace(serviceName, name)
}

func (pm *proxyManager) destroyProxy(serviceName string, name string) (bool, error) {
	var ns = serviceName + name
	pm.mu.RLock()
	if _, ok := pm.proxies[ns]; ok {
		pm.mu.RUnlock()
		pm.mu.Lock()
		delete(pm.proxies, ns)
		pm.mu.Unlock()
		message := proto.ClientDestroyProxyEncodeRequest(name, serviceName)
		_, err := pm.client.InvocationService.invokeOnRandomTarget(message).Result()
		if err != nil {
			return false, err
		}
		return true, nil
	}
	pm.mu.RUnlock()
	return false, nil
}

func (pm *proxyManager) findNextProxyAddress() core.Address {
	member := pm.client.LoadBalancer.Next()
	if member != nil {
		return member.Address()
	}
	return nil
}

func (pm *proxyManager) getProxyByNameSpace(serviceName string, name string) (core.DistributedObject, error) {
	if bufutil.ServiceNameMap == serviceName {
		return newMapProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameList == serviceName {
		return newListProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameSet == serviceName {
		return newSetProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameTopic == serviceName {
		return newTopicProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameMultiMap == serviceName {
		return newMultiMapProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameReplicatedMap == serviceName {
		return newReplicatedMapProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameQueue == serviceName {
		return newQueueProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameRingbufferService == serviceName {
		return newRingbufferProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNamePNCounter == serviceName {
		return newPNCounterProxy(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameIDGenerator == serviceName {
		return newFlakeIDGenerator(pm.client, serviceName, name), nil
	} else if bufutil.ServiceNameReliableTopic == serviceName {
		return newReliableTopicProxy(pm.client, serviceName, name)
	}
	return nil, core.NewHazelcastClientServiceNotFoundError(fmt.Sprintf("no factory registered for service: %s",
		serviceName), nil)
}
