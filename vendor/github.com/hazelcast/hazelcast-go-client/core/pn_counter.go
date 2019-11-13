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

// PNCounter is PN (Positive-Negative) CRDT counter.
//
// The counter supports adding and subtracting values as well as
// retrieving the current counter value.
// The counter guarantees that whenever two nodes have received the
// same set of updates, possibly in a different order, their state is
// identical, and any conflicting updates are merged automatically.
// If no new updates are made to the shared state, all nodes that can
// communicate will eventually have the same data.
//
// The invocation is remote. This may lead to indeterminate state,
// the update may be applied but the response has not been received.
// In this case, the caller will be notified with a HazelcastTargetDisconnectedError.
//
// The read and write methods provide monotonic read and RYW (read-your-write)
// guarantees. These guarantees are session guarantees which means that if
// no replica with the previously observed state is reachable, the session
// guarantees are lost and the method invocation will return a
// HazelcastConsistencyLostError. This does not mean
// that an update is lost. All of the updates are part of some replica and
// will be eventually reflected in the state of all other replicas. This
// error just means that you cannot observe your own writes because
// all replicas that contain your updates are currently unreachable.
// After you have received a HazelcastConsistencyLostError, you can either
// wait for a sufficiently up-to-date replica to become reachable in which
// case the session can be continued or you can reset the session by calling
// the Reset() method. If you have called the Reset() method,
// a new session is started with the next invocation to a CRDT replica.
//
// NOTE:
// The CRDT state is kept entirely on non-lite (data) members. If there
// aren't any and the methods here are invoked, they will
// fail with a HazelcastNoDataMemberInClusterError.
//
// PNCounter requires Hazelcast 3.10.
type PNCounter interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// Get returns the current value of the counter.
	// It returns HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	Get() (currentValue int64, err error)

	// GetAndAdd adds the given value to the current value.
	// It returns the previous value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	GetAndAdd(delta int64) (previousValue int64, err error)

	// AddAndGet adds the given value to the current value.
	// It returns the updated value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	AddAndGet(delta int64) (updatedValue int64, err error)

	// GetAndSubtract subtracts the given value to the current value.
	// It returns the previous value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	GetAndSubtract(delta int64) (previousValue int64, err error)

	// SubtractAndGet subtracts the given value to the current value.
	// It returns the updated value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members.
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	SubtractAndGet(delta int64) (updatedValue int64, err error)

	// DecrementAndGet decrements by one the current value.
	// It returns the updated value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	DecrementAndGet() (updatedValue int64, err error)

	// IncrementAndGet increments by one the current value.
	// It returns the updated value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	IncrementAndGet() (updatedValue int64, err error)

	// GetAndDecrement decrements by one the current value.
	// It returns the previous value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	GetAndDecrement() (previousValue int64, err error)

	// GetAndIncrement increments by one the current value.
	// It returns the previous value,
	// HazelcastNoDataMemberInClusterError if the cluster does not contain any data members,
	// HazelcastUnsupportedOperationError if the cluster version is less  than 3.10,
	// HazelcastConsistencyLostError if the session guarantees have been lost.
	GetAndIncrement() (previousValue int64, err error)

	// Reset resets the observed state by this PN counter. This method may be used
	// after a method invocation has returned a HazelcastConsistencyLostError
	// to reset the proxy and to be able to start a new session.
	Reset()
}
