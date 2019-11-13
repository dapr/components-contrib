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
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/util/murmur"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const partitionUpdateInterval = 10 * time.Second

type partitionService struct {
	client  *HazelcastClient
	mp      atomic.Value
	cancel  chan struct{}
	refresh chan struct{}
	period  time.Duration
	logger  logger.Logger
}

func newPartitionService(client *HazelcastClient) *partitionService {
	service := &partitionService{client: client, cancel: make(chan struct{}), refresh: make(chan struct{}, 1)}
	service.mp.Store(make(map[int32]*proto.Address))
	service.logger = client.logger
	service.period = partitionUpdateInterval
	return service
}

func (ps *partitionService) start() {
	ps.doRefresh()
	go func() {
		ticker := time.NewTicker(ps.period)
		for {
			select {
			case <-ticker.C:
				ps.doRefresh()
			case <-ps.refresh:
				ps.doRefresh()
			case <-ps.cancel:
				ticker.Stop()
				return
			}
		}
	}()

}

func (ps *partitionService) getPartitionCount() int32 {
	ps.waitForPartitionsFetchedOnce()
	partitions := ps.mp.Load().(map[int32]*proto.Address)
	return int32(len(partitions))
}

func (ps *partitionService) partitionOwner(partitionID int32) (*proto.Address, bool) {
	ps.waitForPartitionsFetchedOnce()
	partitions := ps.mp.Load().(map[int32]*proto.Address)
	address, ok := partitions[partitionID]
	return address, ok
}

func (ps *partitionService) GetPartitionID(keyData serialization.Data) int32 {
	count := ps.getPartitionCount()
	if count <= 0 {
		return 0
	}
	return murmur.HashToIndex(keyData.GetPartitionHash(), count)
}

func (ps *partitionService) GetPartitionIDWithKey(key interface{}) (int32, error) {
	data, err := ps.client.SerializationService.ToData(key)
	if err != nil {
		return 0, err
	}
	return ps.GetPartitionID(data), nil
}

func (ps *partitionService) doRefresh() {
	connection := ps.client.ConnectionManager.getOwnerConnection()
	if connection == nil {
		ps.logger.Trace("Error while fetching cluster partition table!")
		return
	}
	request := proto.ClientGetPartitionsEncodeRequest()
	result, err := ps.client.InvocationService.invokeOnConnection(request, connection).Result()
	if err != nil {
		if ps.client.lifecycleService.isLive.Load() == true {
			ps.logger.Trace("Error while fetching cluster partition table! ", err)
		}
		return
	}
	ps.processPartitionResponse(result)
}

func (ps *partitionService) processPartitionResponse(result *proto.ClientMessage) {
	partitions /*partitionStateVersion*/, _ := proto.ClientGetPartitionsDecodeResponse(result)()
	newPartitions := make(map[int32]*proto.Address, len(partitions))
	for _, partitionList := range partitions {
		addr := partitionList.Key().(*proto.Address)
		for _, partition := range partitionList.Value().([]int32) {
			newPartitions[partition] = addr
		}
	}
	ps.mp.Store(newPartitions)
}

func (ps *partitionService) shutdown() {
	close(ps.cancel)
}

func (ps *partitionService) waitForPartitionsFetchedOnce() {
	for len(ps.mp.Load().(map[int32]*proto.Address)) == 0 && ps.client.ConnectionManager.IsAlive() {
		ps.doRefresh()
	}
}
