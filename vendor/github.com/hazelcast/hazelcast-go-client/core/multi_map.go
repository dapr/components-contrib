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

// MultiMap is a specialized map whose keys can be associated with multiple values.
type MultiMap interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// Put stores a key-value pair in the multi-map.
	// It returns true if size of the multi-map is increased, false if the multi-map
	// already contains the key-value pair.
	Put(key interface{}, value interface{}) (increased bool, err error)

	// Get returns a slice of values associated with the specified key.
	// The slice is NOT backed by the map, so changes to the map
	// are NOT reflected in the collection, and vice-versa.
	Get(key interface{}) (values []interface{}, err error)

	// Remove removes an association of the specified value with the specified key. Calling this method does not affect
	// other values associated with the same key.
	// It returns true if the value was detached from the specified key, false if it was not.
	Remove(key interface{}, value interface{}) (removed bool, err error)

	// Delete deletes all the entries with the given key.
	Delete(key interface{}) (err error)

	// RemoveAll detaches all values from the specified key.
	// It returns a slice of old values that were associated with this key prior to this method call.
	// The slice is NOT backed by the map, so changes to the map
	// are NOT reflected in the collection, and vice-versa.
	RemoveAll(key interface{}) (oldValues []interface{}, err error)

	// ContainsKey checks if this multi-map contains a specified key.
	// It returns true if this map contains the specified key, false otherwise.
	ContainsKey(key interface{}) (found bool, err error)

	// ContainsValue returns true if the specified value is associated with at least one key in this multi-map,
	// false otherwise.
	ContainsValue(value interface{}) (found bool, err error)

	// ContainsEntry returns true if this multi-map has an association between
	// the specified key and the specified value, false otherwise.
	ContainsEntry(key interface{}, value interface{}) (found bool, err error)

	// Clear removes all entries from this multi-map.
	Clear() (err error)

	// Size returns the total number of values in this multi-map.
	Size() (size int32, err error)

	// ValueCount returns the number of values associated with the specified key.
	ValueCount(key interface{}) (valueCount int32, err error)

	// Values returns a flat slice of all values stored in this multi-map.
	// The slice is NOT backed by the map, so changes to the map
	// are NOT reflected in the collection, and vice-versa.
	Values() (values []interface{}, err error)

	// KeySet returns a slice of all keys in this multi-map.
	// The slice is NOT backed by the map, so changes to the map
	// are NOT reflected in the collection, and vice-versa.
	KeySet() (keySet []interface{}, err error)

	// EntrySet returns all entries in this multi-map. If a certain key has multiple values associated with it,
	// then one pair will be returned for each value.
	// The slice is NOT backed by the map, so changes to the map
	// are NOT reflected in the collection, and vice-versa.
	EntrySet() (resultPairs []Pair, err error)

	// AddEntryListener adds an entry listener to this multi-map.
	// To receive an event, listener should implement a corresponding interface for that event.
	// Supported listeners for MultiMap:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * MapClearedListener
	// It returns registration ID for this entry listener.
	AddEntryListener(listener interface{}, includeValue bool) (registrationID string, err error)

	// AddEntryListenerToKey adds an entry listener to this multi-map.
	// Supported listeners for MultiMap:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * MapClearedListener
	// This entry listener will only be notified of updates related to this key.
	// It returns registration ID for this entry listener.
	AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID string, err error)

	// RemoveEntryListener removes the entry listener by the registration ID.
	RemoveEntryListener(registrationID string) (removed bool, err error)

	// Lock acquires a lock for the specified key.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until the lock has been acquired.
	//
	// The scope of the lock is for this multi-map only.
	// The acquired lock is only for the key in this multi-map.
	//
	// Locks are re-entrant, so if the key is locked N times, then
	// it should be unlocked N times before another thread can acquire it.
	Lock(key interface{}) (err error)

	// LockWithLeaseTime acquires the lock for the specified key for the specified lease time.
	// After the lease time, the lock will be released.
	//
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until the lock has been acquired.
	//
	// Scope of the lock is for this multi-map only.
	// The acquired lock is only for the key in this multi-map.
	//
	// Locks are re-entrant, so if the key is locked N times, then
	// it should be unlocked N times before another thread can acquire it.
	LockWithLeaseTime(key interface{}, lease time.Duration) (err error)

	// IsLocked returns true if this key is locked, false otherwise.
	IsLocked(key interface{}) (locked bool, err error)

	// TryLock tries to acquire the lock for the specified key.
	// If the lock is not available, then the current thread
	// does not wait and the method returns false immediately.
	TryLock(key interface{}) (locked bool, err error)

	// TryLockWithTimeout tries to acquire the lock for the specified key.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until one of two things happens:
	// the lock is acquired by the current thread, or
	// the specified waiting time elapses.
	TryLockWithTimeout(key interface{}, timeout time.Duration) (locked bool, err error)

	// Tries to acquire the lock for the specified key for the specified lease time.
	// After lease time, the lock will be released.
	//
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until one of two things happens:
	// the lock is acquired by the current thread, or
	// the specified waiting time elapses.
	TryLockWithTimeoutAndLease(key interface{}, timeout time.Duration, lease time.Duration) (locked bool, err error)

	// Unlock unlocks the specified key.
	// It never blocks and returns immediately.
	Unlock(key interface{}) (err error)

	// ForceUnlock forcefully unlocks the specified key, disregarding the acquisition count.
	// This in contrast to the regular unlock, which has to be called the same amount of times as
	// the lock was acquired.
	ForceUnlock(key interface{}) (err error)
}
