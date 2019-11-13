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

// List is a concurrent, distributed, ordered collection. The user of this
// interface has precise control over where in the list each element is
// inserted.  The user can access elements by their integer index (position in the list),
// and search for elements in the list.
//
// The Hazelcast List is not a partitioned data-structure. So all the content of the List is stored in a single
// machine (and in the backup). So the List will not scale by adding more members in the cluster.
type List interface {

	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// Add appends the specified element to the end of this list.
	// Add returns true if the list has changed as a result of this operation, false otherwise.
	Add(element interface{}) (changed bool, err error)

	// AddAt inserts the specified element at the specified index.
	// AddAt shifts the subsequent elements to the right.
	AddAt(index int32, element interface{}) (err error)

	// AddAll appends all elements in the specified slice to the end of this list.
	// AddAll returns true if the list has changed as a result of this operation, false otherwise.
	AddAll(elements []interface{}) (changed bool, err error)

	// AddAllAt inserts all elements in the specified slice at specified index, keeping the order of the slice.
	// AddAllAt shifts the subsequent elements to the right.
	// AddAllAt returns true if the list has changed as a result of this operation, false otherwise.
	AddAllAt(index int32, elements []interface{}) (changed bool, err error)

	// AddItemListener adds an item listener for this list.
	// Listener will be invoked whenever an item is added to or removed from this list.
	// To receive an event, listener should implement a corresponding interface for that event
	// such as ItemAddedListener, ItemRemovedListener.
	// AddItemListener returns registrationID of the listener.
	AddItemListener(listener interface{}, includeValue bool) (registrationID string, err error)

	// Clear clears this list.
	Clear() (err error)

	// Contains checks if the list contains the given element.
	// Contains returns true if the list contains the element, false otherwise.
	Contains(element interface{}) (found bool, err error)

	// ContainsAll checks if the list contains all of the given elements.
	// ContainsAll returns true if the list contains all of the elements, otherwise false.
	ContainsAll(elements []interface{}) (foundAll bool, err error)

	// Get retrieves the element at given index.
	Get(index int32) (element interface{}, err error)

	// IndexOf returns the position of first occurrence of the given element in this list.
	IndexOf(element interface{}) (index int32, err error)

	// IsEmpty return true if the list is empty, false otherwise.
	IsEmpty() (empty bool, err error)

	// LastIndexOf returns the position of the last occurrence of the given element in this list.
	LastIndexOf(element interface{}) (index int32, err error)

	// Remove removes the given element from this list.
	// Remove returns true if the list has changed as a result of this operation, false otherwise.
	Remove(element interface{}) (changed bool, err error)

	// RemoveAt removes the element at the given index.
	// RemoveAt returns the removed element.
	RemoveAt(index int32) (previousElement interface{}, err error)

	// RemoveAll removes the given elements from the list.
	// RemoveAll returns true if the list has changed as a result of this operation, false otherwise.
	RemoveAll(elements []interface{}) (changed bool, err error)

	// RemoveItemListener removes the item listener with the given registrationID.
	// RemoveItemListener returns true if the listener is removed, false otherwise.
	RemoveItemListener(registrationID string) (removed bool, err error)

	// RetainAll removes all elements from this list except the ones contained in the given slice.
	// RetainAll returns true if the list has changed as a result of this operation, false otherwise.
	RetainAll(elements []interface{}) (changed bool, err error)

	// Set replaces the element at the specified index in this list with the specified element.
	// Set returns the previousElement from the list.
	Set(index int32, element interface{}) (previousElement interface{}, err error)

	// Size returns the number of elements in this list.
	Size() (size int32, err error)

	// SubList returns a view of this list that contains elements between index numbers
	// from start (inclusive) to end (exclusive).
	SubList(start int32, end int32) (elements []interface{}, err error)

	// ToSlice returns a slice that contains all elements of this list in proper sequence.
	ToSlice() (elements []interface{}, err error)
}
