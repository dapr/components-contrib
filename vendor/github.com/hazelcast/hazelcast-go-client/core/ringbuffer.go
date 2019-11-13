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

// Ringbuffer is a data-structure where the content is stored in a ring like structure. A ringbuffer has a capacity so it
// won't grow beyond that capacity and endanger the stability of the system. If that capacity is exceeded, than the oldest
// item in the ringbuffer is overwritten.
//
// The ringbuffer has 2 always incrementing sequences:
//  * tailSequence: This is the side where the youngest item is found.
// 					So the tail is the side of the ringbuffer where items are added to.
//  * headSequence: This is the side where the oldest items are found.
// 					So the head is the side where items gets discarded.
//
// The items in the ringbuffer can be found by a sequence that is in between (inclusive) the head and tail sequence.
// If data is read from a ringbuffer with a sequence that is smaller than the headSequence, it means that the data
// is not available anymore.
type Ringbuffer interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// Capacity returns capacity of this ringbuffer.
	Capacity() (capacity int64, err error)

	// Size returns number of items in the ringbuffer.
	// If no ttl is set, the size will always be equal to capacity after the head completed the first loop around the ring.
	// This is because no items are getting retired.
	Size() (size int64, err error)

	// TailSequence returns the sequence of the tail. The tail is the side of the ringbuffer where the items are added to.
	// The initial value of the tail is -1 if the ringbuffer is not backed by a store, otherwise tail sequence will be set to
	// the sequence of the previously last stored item.
	TailSequence() (tailSequence int64, err error)

	// HeadSequence returns the head sequence. The head is the side of the ringbuffer where the oldest items are found.
	// If the ringbuffer is empty, the head will be one more than the tail.
	// The initial value of the head is 0.
	HeadSequence() (headSequence int64, err error)

	// RemainingCapacity returns the remaining capacity of the ringbuffer.
	// The returned value could be stale as soon as it is returned.
	// If ttl is not set, the remaining capacity will always be the capacity.
	RemainingCapacity() (remainingCapacity int64, err error)

	// Add adds an item to the tail of this ringbuffer. Overflow policy determines what will happen,
	// if there is no space left in this ringbuffer. If OverflowPolicyOverwrite was passed,
	// the new item will overwrite the oldest one regardless of the configured time-to-live.
	//
	// In the case when OverflowPolicyFail was specified, the add operation will keep failing until an oldest item in this
	// ringbuffer will reach its time-to-live.
	//
	// Add returns the sequence number of the added item. You can read the added item using this number.
	Add(item interface{}, overflowPolicy OverflowPolicy) (sequence int64, err error)

	// AddAll adds all items in the specified slice to the tail of this buffer. The behavior of this method is essentially
	// the same as the one of the add method.
	//
	// The method does not guarantee that the inserted items will have contiguous sequence numbers.
	AddAll(items []interface{}, overflowPolicy OverflowPolicy) (lastSequence int64, err error)

	// ReadOne reads a single item from this ringbuffer.
	// If the sequence is equal to the current tail sequence plus one,
	// this call will not return a response until an item is added.
	// If it is more than that, an error will be returned.
	//
	// Unlike queue's take, this method does not remove an item from the ringbuffer. This means that the same item
	// can be read by multiple processes.
	ReadOne(sequence int64) (item interface{}, err error)

	// ReadMany reads a batch of items from this ringbuffer.
	// If the number of available items starting at sequence is smaller than maxCount,
	// then this method will not wait for more items to arrive.
	// Instead, available items will be returned.
	//
	// If there are less items available than minCount, then this call will not return a response until
	// a necessary number of items becomes available.
	ReadMany(startSequence int64, minCount int32, maxCount int32, filter interface{}) (readResultSet ReadResultSet, err error)
}

// OverflowPolicy is a policy with which one can control the behavior of what should be done
// when an item is about to be added to the ringbuffer,
// but there is 0 remaining capacity.
//
// Overflowing happens when a time-to-live is set and the oldest item in the ringbuffer (the head) is not old enough to expire.
type OverflowPolicy interface {
	Policy() policy
}

const (
	// OverflowPolicyOverwrite is the policy where the oldest item is overwritten even if it is not old enough to retire.
	// Using this policy you are sacrificing the time-to-live in favor of being able to write.
	//
	// Example: if there is a time-to-live of 30 seconds, the buffer is full and the oldest item in the ring has been placed a
	// second ago, then there are 29 seconds remaining for that item. Using this policy you are going to overwrite no matter
	// what.
	OverflowPolicyOverwrite policy = 0

	// OverflowPolicyFail is the policy where the call will fail immediately and the oldest item will not be overwritten
	// before it is old enough to retire. So this policy sacrifices the ability to write in favor of time-to-live.
	//
	// The advantage of fail is that the caller can decide what to do since it doesn't trap the thread due to backoff.
	//
	// Example: if there is a time-to-live of 30 seconds, the buffer is full and the oldest item in the ring has been placed a
	// second ago, then there are 29 seconds remaining for that item. Using this policy you are not going to overwrite that
	// item for the next 29 seconds.
	OverflowPolicyFail policy = 1
)

// policy type is not used by user.
type policy int32

// Policy returns the policy type.
func (p policy) Policy() policy {
	return p
}

// ReadResultSet is the result of a Ringbuffer ReadMany operation.
type ReadResultSet interface {
	// ReadCount returns the number of items that have been read before filtering.
	// If no filter is set, then the readCount will be the same as size. But if a filter is applied, it could be that items
	// are read, but are filtered out. So if you are trying to make another read based on the ReadResultSet then you should
	// increment the sequence by readCount and not by size. Otherwise you will be re-reading the same filtered messages.
	ReadCount() (readCount int32)

	// Get returns the item at the given index.
	Get(index int32) (item interface{}, err error)

	// Sequence returns the sequence number for the item at the given index.
	// A return value of -1 means that the information is not available.
	Sequence(index int32) (sequence int64, err error)

	// Size returns the result set size.
	Size() (size int32)
}
