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

// DistributedObject is the base interface for all distributed objects.
type DistributedObject interface {
	// Destroy destroys this object cluster-wide.
	// Destroy clears and releases all resources for this object.
	Destroy() (bool, error)

	// Name returns the unique name for this DistributedObject.
	Name() string

	// PartitionKey returns the key of partition this DistributedObject is assigned to. The returned value only has meaning
	// for a non partitioned data structure like an IAtomicLong. For a partitioned data structure like an Map
	// the returned value will not be nil, but otherwise undefined.
	PartitionKey() string

	// ServiceName returns the service name for this object.
	ServiceName() string
}
