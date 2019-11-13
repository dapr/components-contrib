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
	"time"
)

// Map is concurrent, distributed, observable and queryable.
// This map is sync (blocking). Blocking calls return the value of the call and block
// the execution until the return value is calculated.
// It does not allow nil to be used as a key or value.
type Map interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// Put returns the value with the specified key in this map.
	// If the map previously contained a mapping for
	// the key, the old value is replaced by the specified value.
	// Put returns a clone of the previous value, not the original (identically equal) value previously put
	// into the map.
	// Put returns nil if there was no mapping for the key.
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)

	// Get returns the value for the specified key, or nil if this map does not contain this key.
	// Warning:
	// Get returns a clone of original value, modifying the returned value does not change the actual value in
	// the map. One should put modified value back to make changes visible to all nodes.
	//	value,_ = map.Get(key)
	//	value.updateSomeProperty()
	//	map.Put(key,value)
	Get(key interface{}) (value interface{}, err error)

	// Remove removes the mapping for a key from this map if it is present. The map will not contain a mapping for the
	// specified key once the call returns.
	// If you don't need the previously mapped value for the removed key, prefer to use
	// delete and avoid the cost of serialization and network transfer.
	// It returns a clone of the previous value, not the original (identically equal) value
	// previously put into the map.
	// Remove returns the previous value associated with key, or nil if there was no mapping for the key.
	Remove(key interface{}) (value interface{}, err error)

	// RemoveIfSame removes the entry for a key only if it is currently mapped to a given value.
	// RemoveIfSame returns true if the value was removed.
	// This is equivalent to:
	//	if ok,_ := mp.ContainsKey(key) ; ok {
	//		if mp.Get(key) == value {
	//			mp.Remove(key)
	//			return true
	//		}
	//	}
	//	return false
	// except that the action is performed atomically.
	RemoveIfSame(key interface{}, value interface{}) (ok bool, err error)

	// RemoveAll removes all entries which match with the supplied predicate.
	// If this map has index, matching entries will be found via index search,
	// otherwise they will be found by full-scan.
	// Note that calling this method also removes all entries from caller's Near Cache.
	RemoveAll(predicate interface{}) (err error)

	// Size returns the number of entries in this map.
	Size() (size int32, err error)

	// Aggregate applies the aggregation logic on all map entries and returns the result.
	// Fast-Aggregations are the successor of the Map-Reduce Aggregators.
	// They are equivalent to the Map-Reduce Aggregators in most of the use-cases, but instead of running on the Map-Reduce
	// engine they run on the Query infrastructure. Their performance is tens to hundreds times better due to the fact
	// that they run in parallel for each partition and are highly optimized for speed and low memory consumption.
	// The given aggregator must be serializable via hazelcast serialization and have a counterpart on server side.
	// Aggregate returns the result of the given aggregator.
	Aggregate(aggregator interface{}) (result interface{}, err error)

	// AggregateWithPredicate applies the aggregation logic on map entries filtered with the Predicated and returns the result
	//
	// Fast-Aggregations are the successor of the Map-Reduce Aggregators.
	// They are equivalent to the Map-Reduce Aggregators in most of the use-cases, but instead of running on the Map-Reduce
	// engine they run on the Query infrastructure. Their performance is tens to hundreds times better due to the fact
	// that they run in parallel for each partition and are highly optimized for speed and low memory consumption.
	// The given aggregator must be serializable via hazelcast serialization and have a counterpart on server side.
	// The given predicate must be serializable via hazelcast serialization and have a counterpart on server side.
	// AggregateWithPredicate returns the result of the given aggregator.
	AggregateWithPredicate(aggregator interface{}, predicate interface{}) (result interface{}, err error)

	// ContainsKey determines whether this map contains an entry with the key.
	// ContainsKey returns true if this map contains an entry for the specified key.
	ContainsKey(key interface{}) (found bool, err error)

	// ContainsValue determines whether this map contains one or more keys for the specified value.
	// ContainsValue  returns true if this map contains an entry for the specified value.
	ContainsValue(value interface{}) (found bool, err error)

	// Clear clears the map and deletes the items from the backing map store.
	Clear() (err error)

	// Delete removes the mapping for a key from this map if it is present (optional operation).
	// Unlike Remove(), this operation does not return the removed value, which avoids the serialization cost of
	// the returned value. If the removed value will not be used, a delete operation is preferred over a remove
	// operation for better performance.
	// The map will not contain a mapping for the specified key once the call returns.
	// Warning:
	// This method breaks the contract of EntryListener. When an entry is removed by Delete(), it fires an EntryEvent
	// with a nil oldValue. Also, a listener with predicate will have nil values, so only the keys can be queried
	// via predicate.
	Delete(key interface{}) (err error)

	// IsEmpty returns true if this map contains no key-value mappings.
	IsEmpty() (empty bool, err error)

	// AddIndex Adds an index to this map for the specified entries so
	// that queries can run faster.
	//
	// Let's say your map values are Employee struct which implements identifiedDataSerializable.
	//	type Employee struct {
	//		age int32
	//		name string
	//		active bool
	//		// other fields
	//	}
	//	// methods
	// If you are querying your values mostly based on age and active then
	// you should consider indexing these fields.
	//	mp, _ := client.GetMap("employee")
	//	mp.AddIndex("age", true);        // ordered, since we have ranged queries for this field
	//	mp.AddIndex("active", false);    // not ordered, because boolean field cannot have range
	//
	// You should make sure to add the indexes before adding
	// entries to this map.
	//
	// Indexing is executed in parallel on each partition by operation threads on server side. The Map
	// is not blocked during this operation.
	//
	// The time taken in is proportional to the size of the Map and the number of members.
	//
	// Until the index finishes being created, any searches for the attribute will use a full Map scan,
	// thus avoiding using a partially built index and returning incorrect results.
	AddIndex(attribute string, ordered bool) (err error)

	// Evict evicts the specified key from this map.
	// Evict returns true if the key is evicted, false otherwise.
	Evict(key interface{}) (evicted bool, err error)

	// EvictAll evicts all keys from this map except the locked ones.
	// The EvictAll event is fired for any registered listeners for MapEvicted.
	EvictAll() (err error)

	// Flush flushes all the local dirty entries.
	// Please note that this method has effect only if write-behind
	// persistence mode is configured. If the persistence mode is
	// write-through calling this method has no practical effect, but an
	// operation is executed on all partitions wasting resources.
	Flush() (err error)

	// ForceUnlock releases the lock for the specified key regardless of the lock owner.
	// It always successfully unlocks the key, never blocks, and returns immediately.
	ForceUnlock(key interface{}) (err error)

	// Lock acquires the lock for the specified key infinitely.
	// If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
	// dormant until the lock has been acquired.
	// You get a lock whether the value is present in the map or not. Other threads (possibly on other systems) would
	// block on their invoke of Lock() until the non-existent key is unlocked. If the lock holder introduces the key to
	// the map, the Put() operation is not blocked. If a thread not holding a lock on the non-existent key tries to
	// introduce the key while a lock exists on the non-existent key, the Put() operation blocks until it is unlocked.
	// Scope of the lock is this map only. Acquired lock is only for the key in this map.
	// Locks are re-entrant; so, if the key is locked N times, it should be unlocked N times before another thread can
	// acquire it.
	Lock(key interface{}) (err error)

	// LockWithLeaseTime acquires the lock for the specified key for the specified lease time.
	// After lease time, the lock will be released.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until the lock has been acquired.
	// Scope of the lock is this map only.
	// Acquired lock is only for the key in this map.
	// Locks are re-entrant, so if the key is locked N times then
	// it should be unlocked N times before another thread can acquire it.
	LockWithLeaseTime(key interface{}, lease time.Duration) (err error)

	// Unlock releases the lock for the specified key. It never blocks and returns immediately.
	// If the current thread is the holder of this lock,
	// then the hold count is decremented. If the hold count is zero, then the lock is released.
	Unlock(key interface{}) (err error)

	// IsLocked checks the lock for the specified key.
	// If the lock is acquired, it returns true. Otherwise, it returns false.
	IsLocked(key interface{}) (locked bool, err error)

	// Replace replaces the entry for a key only if it is currently mapped to some value.
	// This is equivalent to:
	//	if ok,_ := mp.ContainsKey(key) ; ok {
	//		return mp.Put(key,value)
	//	}else{
	//		return nil
	//  }
	// except that the action is performed atomically.
	// Warning:
	// Replace returns a clone of the previous value, not the original (identically equal) value previously put
	// into the map.
	Replace(key interface{}, value interface{}) (oldValue interface{}, err error)

	// ReplaceIfSame replaces the entry for a key only if it is currently mapped to a given value.
	// This is equivalent to:
	//	if ok,_ := mp.ContainsKey(key) ; ok {
	//		if mp.Get(key) == oldValue {
	//			mp.Put(key,value)
	//			return true
	//		}
	//	}
	//	return false
	// except that the action is performed atomically.
	//
	// This method may return false even if the operation succeeds.
	// Background: If the partition owner for given key goes down after successful value replace,
	// but before the executing node retrieved the invocation result response, then the operation is retried.
	// The invocation retry fails because the value is already updated and the result of such replace call
	// returns false. Hazelcast doesn't guarantee exactly once invocation.
	// ReplaceIfSame returns true if the value was replaced.
	ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (replaced bool, err error)

	// Set puts an entry into this map without returning the old value
	// (which is more efficient than Put()).
	// Set breaks the contract of EntryListener.
	// When an entry is updated by Set(), it fires an EntryEvent with a nil oldValue.
	// If you have previously set a TTL for the key, the TTL remains unchanged and the entry will
	// expire when the initial TTL has elapsed.
	Set(key interface{}, value interface{}) (err error)

	// SetWithTTl puts an entry into this map with a given TTL (time to live) value,
	// without returning the old value (which is more efficient than Put()).
	// The entry will expire and get evicted after the TTL. If the TTL is 0,
	// then the entry lives forever. If the TTL is negative, then the TTL
	// from the map configuration will be used (default: forever).
	// For example:
	// 	mp.SetWithTTL("testingKey1", "testingValue1", 5 * time. Second)
	// will expire and get evicted after 5 seconds whereas
	//  mp.SetWithTTL("testingKey1", "testingValue1", 5 * time. Millisecond)
	// will expire and get evicted after 5 milliseconds.
	SetWithTTL(key interface{}, value interface{}, ttl time.Duration) (err error)

	// PutIfAbsent associates the specified key with the given value
	// if it is not already associated.
	// This is equivalent to:
	//	if ok,_ := mp.ContainsKey(key) ; !ok {
	//		return mp.Put(key,value)
	//	}else{
	//		return mp.Get(key)
	//  }
	// except that the action is performed atomically.
	// Warning:
	// PutIfAbsent returns a clone of the previous value,
	// not the original (identically equal) value previously put
	// into the map.
	// PutIfAbsent returns the old value of the key.
	PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error)

	// PutAll copies all of the mappings from the specified map to this map. No atomicity guarantees are
	// given. In the case of a failure, some of the key-value tuples may get written, while others are not.
	PutAll(entries map[interface{}]interface{}) (err error)

	// Project applies the projection logic on all map entries and returns the result.
	// The given projection must be serializable via hazelcast serialization and have a counterpart on server side.
	// Project returns the result of the given projection.
	Project(projection interface{}) (result []interface{}, err error)

	// ProjectWithPredicate applies the projection logic on map entries filtered with the predicate and returns the result.
	// The given projection must be serializable via hazelcast serialization and have a counterpart on server side.
	// The given predicate must be serializable via hazelcast serialization and have a counterpart on server side.
	// ProjectWithPredicate returns the result of the given predicate and projection.
	ProjectWithPredicate(projection interface{}, predicate interface{}) (result []interface{}, err error)

	// KeySet returns a slice clone of the keys contained in this map.
	// Warning:
	// The slice is NOT backed by the map, so changes to the map are NOT reflected in the slice, and vice-versa.
	KeySet() (keySet []interface{}, err error)

	// KeySetWithPredicate queries the map based on the specified predicate and
	// returns the keys of matching entries.
	// Specified predicate runs on all members in parallel.
	// The slice is NOT backed by the map, so changes to the map are NOT reflected in the slice, and vice-versa.
	KeySetWithPredicate(predicate interface{}) (keySet []interface{}, err error)

	// Values returns a slice clone of the values contained in this map.
	// The slice is NOT backed by the map, so changes to the map are NOT reflected in the slice, and vice-versa.
	Values() (values []interface{}, err error)

	// ValuesWithPredicate queries the map based on the specified predicate and returns the values of matching entries.
	// Specified predicate runs on all members in parallel.
	// The slice is NOT backed by the map, so changes to the map are NOT reflected in the slice, and vice-versa.
	ValuesWithPredicate(predicate interface{}) (values []interface{}, err error)

	// EntrySet returns a slice of Pairs clone of the mappings contained in this map.
	// The slice is NOT backed by the map, so changes to the map are NOT reflected in the slice, and vice-versa.
	EntrySet() (resultPairs []Pair, err error)

	// EntrySetWithPredicate queries the map based on the specified predicate and returns the matching entries.
	// Specified predicate runs on all members in parallel.
	// The slice is NOT backed by the map, so changes to the map are NOT reflected in the slice, and vice-versa.
	EntrySetWithPredicate(predicate interface{}) (resultPairs []Pair, err error)

	// TryLock tries to acquire the lock for the specified key.
	// If the lock is not available then the current thread
	// does not wait and returns false immediately.
	// TryLock returns true if lock is acquired, false otherwise.
	TryLock(key interface{}) (locked bool, err error)

	// TryLockWithTimeout tries to acquire the lock for the specified key.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until one of two things happens:
	//	the lock is acquired by the current thread, or
	//	the specified waiting time elapses.
	// TryLockWithTimeout returns true if lock is acquired, false otherwise.
	TryLockWithTimeout(key interface{}, timeout time.Duration) (locked bool, err error)

	// TryLockWithTimeoutAndLease tries to acquire the lock for the specified key for the specified lease time.
	// After lease time, the lock will be released.
	// If the lock is not available, then
	// the current thread becomes disabled for thread scheduling
	// purposes and lies dormant until one of two things happens:
	//	the lock is acquired by the current thread, or
	//	the specified waiting time elapses.
	// TryLockWithTimeoutAndLease returns true if lock is acquired, false otherwise.
	TryLockWithTimeoutAndLease(key interface{}, timeout time.Duration, lease time.Duration) (locked bool, err error)

	// TryPut tries to put the given key and value into this map and returns immediately.
	// TryPut returns true if the put is successful, false otherwise.
	TryPut(key interface{}, value interface{}) (ok bool, err error)

	// TryPutWithTimeout tries to put the given key and value into this map within a specified
	// timeout value. If this method returns false, it means that
	// the caller thread could not acquire the lock for the key within the
	// timeout duration, thus the put operation is not successful.
	TryPutWithTimeout(key interface{}, value interface{}, timeout time.Duration) (ok bool, err error)

	// TryRemove tries to remove the entry with the given key from this map
	// within the specified timeout value. If the key is already locked by another
	// thread and/or member, then this operation will wait the timeout
	// amount for acquiring the lock.
	// TryRemove returns true if the remove is successful, false otherwise.
	TryRemove(key interface{}, timeout time.Duration) (ok bool, err error)

	// GetAll returns the entries for the given keys.
	// The returned map is NOT backed by the original map,
	// so changes to the original map are NOT reflected in the returned map, and vice-versa.
	GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error)

	// GetEntryView returns the EntryView for the specified key.
	// GetEntryView returns a clone of original mapping, modifying the returned value does not change
	// the actual value in the map. One should put modified value back to make changes visible to all nodes.
	GetEntryView(key interface{}) (entryView EntryView, err error)

	// PutTransient operates same as Put(), but
	// the entry will expire and get evicted after the TTL. If the TTL is 0,
	// then the entry lives forever. If the TTL is negative, then the TTL
	// from the configuration will be used (default: forever).
	PutTransient(key interface{}, value interface{}, ttl time.Duration) (err error)

	// AddEntryListener adds a continuous entry listener for this map.
	// To receive an event, listener should implement a corresponding interface for that event.
	// Supported listeners for Map:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * EntryMergedListener
	//  * EntryExpiredListener
	//  * MapEvictedListener
	//  * MapClearedListener
	// AddEntryListener returns uuid which is used as a key to remove the listener.
	AddEntryListener(listener interface{}, includeValue bool) (registrationID string, err error)

	// AddEntryListenerWithPredicate adds a continuous entry listener for this map filtered with the given predicate.
	// Supported listeners for Map:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * EntryMergedListener
	//  * EntryExpiredListener
	//  * MapEvictedListener
	//  * MapClearedListener
	// AddEntryListenerWithPredicate returns uuid which is used as a key to remove the listener.
	AddEntryListenerWithPredicate(listener interface{}, predicate interface{}, includeValue bool) (registrationID string, err error)

	// AddEntryListenerToKey adds a continuous entry listener for this map filtered with the given key.
	// Supported listeners for Map:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * EntryMergedListener
	//  * EntryExpiredListener
	//  * MapEvictedListener
	//  * MapClearedListener
	// AddEntryListenerToKey returns uuid which is used as a key to remove the listener.
	AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID string, err error)

	// AddEntryListenerToKeyWithPredicate adds a continuous entry listener for this map filtered with the given key and predicate.
	// Supported listeners for Map:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * EntryMergedListener
	//  * EntryExpiredListener
	//  * MapEvictedListener
	//  * MapClearedListener
	// AddEntryListenerToKeyWithPredicate returns uuid which is used as a key to remove the listener.
	AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{}, key interface{}, includeValue bool) (
		registrationID string, err error)

	// RemoveEntryListener removes the specified entry listener with the given registrationID.
	// RemoveEntryListener returns silently if there is no such listener added before.
	// RemoveEntryListener true if registration is removed, false otherwise.
	RemoveEntryListener(registrationID string) (removed bool, err error)

	// ExecuteOnKey applies the user defined EntryProcessor to the entry mapped by the key.
	// ExecuteOnKey returns the result of EntryProcessor's process method.
	// Entry_processor should be a stateful serializable struct which represents the EntryProcessor defined
	// on the server side.
	// This struct must have a serializable EntryProcessor counter part registered on server side with the actual
	// org.hazelcast.map.EntryProcessor implementation.
	ExecuteOnKey(key interface{}, entryProcessor interface{}) (result interface{}, err error)

	// ExecuteOnKeys applies the user defined EntryProcessor to the entries mapped by the slice of keys.
	// Returns the results mapped by each key in the slice.
	// Entry_processor should be a stateful serializable struct which represents the EntryProcessor defined
	// on the server side.
	// This struct must have a serializable EntryProcessor counter part registered on server side with the actual
	// org.hazelcast.map.EntryProcessor implementation.
	ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []Pair, err error)

	// ExecuteOnEntries applies the user defined EntryProcessor to all the entries in the map.
	// Returns the results mapped by each key in the map.
	// Entry_processor should be a stateful serializable struct which represents the EntryProcessor defined
	// on the server side.
	// This struct must have a serializable EntryProcessor counter part registered on server side with the actual
	// org.hazelcast.map.EntryProcessor implementation.
	ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []Pair, err error)

	// ExecuteOnEntriesWithPredicate applies the user defined EntryProcessor to entries in the map which satisfies
	// the predicate.
	// Returns the results mapped by each key in the map.
	// Entry_processor should be a stateful serializable struct which represents the EntryProcessor defined
	// on the server side.
	// This struct must have a serializable EntryProcessor counter part registered on server side with the actual
	// org.hazelcast.map.EntryProcessor implementation.
	ExecuteOnEntriesWithPredicate(entryProcessor interface{}, predicate interface{}) (keyToResultPairs []Pair, err error)
}
