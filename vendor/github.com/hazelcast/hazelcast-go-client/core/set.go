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

// Set is the concurrent, distributed implementation of collection that contains no duplicate elements.
// As implied by its name, this interface models the mathematical 'set' abstraction.
type Set interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// Add adds the specified item to this set if not already present.
	// Add returns true if the item was added, false otherwise.
	Add(item interface{}) (added bool, err error)

	// AddAll adds the items to this set if not already present.
	// AddAll returns true if this set has changed, false otherwise.
	AddAll(items []interface{}) (changed bool, err error)

	// AddItemListener adds an item listener for this set.
	// To receive an event, listener should implement a corresponding interface for that event
	// such as ItemAddedListener, ItemRemovedListener.
	// AddItemListener returns the registrationID of the listener.
	AddItemListener(listener interface{}, includeValue bool) (registrationID string, err error)

	// Clear removes all of the elements from this set.
	Clear() (err error)

	// Contains returns true if the specified item is found in the set.
	Contains(item interface{}) (found bool, err error)

	// ContainsAll returns true if all of the specified items are found in the set.
	ContainsAll(items []interface{}) (foundAll bool, err error)

	// IsEmpty returns true if the set does not have any items, false otherwise.
	IsEmpty() (empty bool, err error)

	// Remove removes the given item from this set.
	// Remove returns true if the given item is removed, false otherwise.
	Remove(item interface{}) (removed bool, err error)

	// RemoveAll removes all the items from this set.
	// RemoveAll returns true if the set has changed, false otherwise.
	RemoveAll(items []interface{}) (changed bool, err error)

	// RetainAll removes all the items from the set except the ones given in the slide.
	// RetainAll returns true if the given list has changed, false otherwise.
	RetainAll(items []interface{}) (changed bool, err error)

	// Size returns the size of this set.
	Size() (size int32, err error)

	// RemoveItemListener removes the listener with the given registrationID.
	// RemoveItemListener returns true if the listener is removed, false otherwise.
	RemoveItemListener(registrationID string) (removed bool, err error)

	// ToSlice returns all the items in this set in proper sequence.
	ToSlice() (items []interface{}, err error)
}
