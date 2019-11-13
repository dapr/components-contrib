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

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/util/iputil"
)

type lifecycleService struct {
	client    *HazelcastClient
	isLive    atomic.Value
	listeners atomic.Value
	mu        sync.Mutex
	logger    logger.Logger
}

func newLifecycleService(client *HazelcastClient) *lifecycleService {
	newLifecycle := &lifecycleService{client: client}
	newLifecycle.logger = client.logger
	newLifecycle.isLive.Store(true)
	newLifecycle.listeners.Store(make(map[string]interface{})) //Initialize
	for _, listener := range client.Config.LifecycleListeners() {
		if _, ok := listener.(core.LifecycleListener); ok {
			newLifecycle.AddLifecycleListener(listener)
		}
	}
	newLifecycle.fireLifecycleEvent(core.LifecycleStateStarting)
	return newLifecycle
}

func (ls *lifecycleService) AddLifecycleListener(listener interface{}) string {
	registrationID, _ := iputil.NewUUID()
	ls.mu.Lock()
	defer ls.mu.Unlock()
	listeners := ls.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)+1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	copyListeners[registrationID] = listener
	ls.listeners.Store(copyListeners)
	return registrationID
}

func (ls *lifecycleService) RemoveLifecycleListener(registrationID string) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	listeners := ls.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)-1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	_, found := copyListeners[registrationID]
	if found {
		delete(copyListeners, registrationID)
	}
	ls.listeners.Store(copyListeners)
	return found
}

func (ls *lifecycleService) fireLifecycleEvent(newState string) {
	if newState == core.LifecycleStateShuttingDown {
		ls.isLive.Store(false)
	}
	listeners := ls.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(core.LifecycleListener); ok {
			listener.(core.LifecycleListener).LifecycleStateChanged(newState)
		}
	}
	ls.logger.Info("HazelcastClient", ClientVersion, "is", newState)
}

func (ls *lifecycleService) IsRunning() bool {
	return ls.isLive.Load().(bool)
}
