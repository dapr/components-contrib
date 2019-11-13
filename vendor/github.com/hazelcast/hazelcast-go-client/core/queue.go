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

import "time"

// Queue is a concurrent, blocking, distributed, observable queue. Queue is not a partitioned data-structure.
// All of the Queue content is stored in a single machine (and in the backup).
// Queue will not scale by adding more members in the cluster.
type Queue interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// AddAll adds all the items. If items slice changes during this operation, behavior is unspecified.
	// AddAll returns true if the queue has changed, false otherwise.
	AddAll(items []interface{}) (changed bool, err error)

	// AddItemListener adds an item listener for this queue.
	// The listener will be invoked for any add/remove item event.
	// To receive an event, listener should implement a corresponding interface for that event
	// such as ItemAddedListener, ItemRemovedListener.
	// AddItemListener returns the registrationID of the listener.
	AddItemListener(listener interface{}, includeValue bool) (registrationID string, err error)

	// Clear removes all of the items in this queue.
	Clear() (err error)

	// Contains returns true if the given item is in this queue.
	Contains(item interface{}) (found bool, err error)

	// ContainsAll returns true if this queue contains all the given items, false otherwise.
	ContainsAll(items []interface{}) (foundAll bool, err error)

	// DrainTo removes all items in this queue and add them to the end of given slice.
	// DrainTo returns the number of moved items.
	DrainTo(slice *[]interface{}) (movedAmount int32, err error)

	// DrainToWithMaxSize removes all items in this queue and add them to the end of given slice.
	// DrainToWithMaxSize returns the number of moved items.
	DrainToWithMaxSize(slice *[]interface{}, maxElements int32) (movedAmount int32, err error)

	// IsEmpty returns true if this queue is empty, false otherwise.
	IsEmpty() (empty bool, err error)

	// Offer inserts the given item to the end of the queue if there is room.
	// If queue is full, offer operation fails.
	// Offer returns true if the item is added, false otherwise.
	Offer(item interface{}) (added bool, err error)

	// OfferWithTimeout inserts the given item to the end of the queue if there is room, otherwise
	// it waits for timeout with timeoutUnit before returning false.
	// OfferWithTimeout returns true if the item is added, false otherwise.
	OfferWithTimeout(item interface{}, timeout time.Duration) (added bool, err error)

	// Peek retrieves, but does not remove the head of this queue.
	// Peek returns the head of this queue, nil if the queue is empty.
	Peek() (item interface{}, err error)

	// Poll retrieves and removes the head of this queue.
	// Poll returns the head of this queue, nil if the queue is empty.
	Poll() (item interface{}, err error)

	// PollWithTimeout retrieves and removes the head of this queue,
	// if the queue is empty it waits timeout with timeoutUnit before returning nil.
	// PollWithTimeout returns the head of this queue, nil if the queue is empty after timeoutInMilliSeconds milliseconds.
	PollWithTimeout(timeout time.Duration) (item interface{}, err error)

	// Put inserts the item at the end of this queue.
	// Put blocks until there is available room in the queue, if necessary.
	Put(item interface{}) (err error)

	// RemainingCapacity returns the number of additional items this queue can contain.
	RemainingCapacity() (remainingCapacity int32, err error)

	// Remove removes an instance of given item from this queue.
	// Remove returns true if the item is removed, false otherwise.
	Remove(item interface{}) (removed bool, err error)

	// RemoveAll removes all items from this queue.
	// RemoveAll returns true if the queue has changed, false otherwise.
	RemoveAll(items []interface{}) (changed bool, err error)

	// RemoveItemListener removes the item listener with the given registrationID from this queue.
	// RemoveItemListener returns true if the listener is removed, false otherwise.
	RemoveItemListener(registrationID string) (removed bool, err error)

	// RetainAll removes all the items from this queue except the 'items'
	// RetainAll returns true if this queue has changed, false otherwise.
	RetainAll(items []interface{}) (changed bool, err error)

	// Size returns the number of elements in this queue.
	Size() (size int32, err error)

	// Take retrieves and removes the head of this queue. It waits to return until
	// an element becomes available if necessary.
	Take() (item interface{}, err error)

	// ToSlice returns all the items in this queue in proper sequence.
	ToSlice() (items []interface{}, err error)
}
