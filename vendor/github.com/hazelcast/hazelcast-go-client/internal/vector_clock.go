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

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type vectorClock struct {
	mutex             sync.RWMutex //guards replicaTimestamps
	replicaTimestamps map[string]int64
}

func newVectorClock() *vectorClock {
	return &vectorClock{replicaTimestamps: make(map[string]int64)}
}

func (v *vectorClock) EntrySet() (entrySet []*proto.Pair) {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	entrySet = make([]*proto.Pair, len(v.replicaTimestamps))
	i := 0
	for key, value := range v.replicaTimestamps {
		entrySet[i] = proto.NewPair(key, value)
		i++
	}
	return entrySet
}

func (v *vectorClock) IsAfter(other *vectorClock) (isAfter bool) {
	anyTimestampGreater := false
	for replicaID, otherReplicaTimestamp := range other.replicaTimestamps {
		localReplicaTimestamp, initialized := v.TimestampForReplica(replicaID)
		if !initialized || localReplicaTimestamp < otherReplicaTimestamp {
			return false
		} else if localReplicaTimestamp > otherReplicaTimestamp {
			anyTimestampGreater = true
		}
	}
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	return anyTimestampGreater || (len(v.replicaTimestamps) > len(other.replicaTimestamps))
}

func (v *vectorClock) SetReplicaTimestamp(replicaID string, timestamp int64) {
	v.mutex.Lock()
	v.replicaTimestamps[replicaID] = timestamp
	v.mutex.Unlock()
}

func (v *vectorClock) TimestampForReplica(replicaID string) (int64, bool) {
	v.mutex.RLock()
	val, ok := v.replicaTimestamps[replicaID]
	v.mutex.RUnlock()
	return val, ok
}
