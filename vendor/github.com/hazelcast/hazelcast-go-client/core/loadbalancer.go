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

package core

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// RandomLoadBalancer is a loadbalancer that selects members randomly from the given cluster service.
type RandomLoadBalancer struct {
	clusterService Cluster
}

func (b *RandomLoadBalancer) Init(cluster Cluster) {
	b.clusterService = cluster
}

func (b *RandomLoadBalancer) Next() Member {
	membersList := b.clusterService.GetMembers()
	size := len(membersList)
	if size > 0 {
		randomIndex := rand.Intn(size)
		return membersList[randomIndex]
	}
	return nil
}

// NewRandomLoadBalancer creates and returns a RandomLoadBalancer.
func NewRandomLoadBalancer() *RandomLoadBalancer {
	rand.Seed(time.Now().Unix())
	return &RandomLoadBalancer{}
}

// RoundRobinLoadBalancer is a loadbalancer where members are used with round robin logic.
type RoundRobinLoadBalancer struct {
	clusterService Cluster
	index          int64
}

func (rrl *RoundRobinLoadBalancer) Init(cluster Cluster) {
	rrl.clusterService = cluster
}

func (rrl *RoundRobinLoadBalancer) Next() Member {
	members := rrl.clusterService.GetMembers()
	size := int64(len(members))
	if size > 0 {
		index := getAndIncrement(&rrl.index) % size
		return members[index]
	}
	return nil
}

// NewRoundRobinLoadBalancer creates and returns a RoundLobinLoadBalancer.
func NewRoundRobinLoadBalancer() *RoundRobinLoadBalancer {
	return &RoundRobinLoadBalancer{}
}

func getAndIncrement(val *int64) int64 {
	newVal := atomic.AddInt64(val, 1)
	return newVal - 1
}
