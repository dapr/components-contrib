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

// ReplicatedMap is a map-like data structure with weak consistency
// and values locally stored on every node of the cluster.
//
// Whenever a value is written asynchronously, the new value will be internally
// distributed to all existing cluster members, and eventually every node will have
// the new value.
//
// When a new node joins the cluster, the new node initially will request existing
// values from older nodes and replicate them locally.
type ReplicatedMap interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// Put associates a given value to the specified key and replicates it to the
	// cluster. If there is an old value, it will be replaced by the specified
	// one and returned from the call.
	//
	// It returns a clone of the previous value,
	// not the original (identically equal) value previously put into the map.
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)

	// PutWithTTL associates a given value to the specified key and replicates it to the
	// cluster. If there is an old value, it will be replaced by the specified
	// one and returned from the call. In addition, you have to specify a ttl and its time unit
	// to define when the value is outdated and thus should be removed from the
	// replicated map.
	//
	// It returns a clone of the previous value,
	// not the original (identically equal) value previously put into the map.
	PutWithTTL(key interface{}, value interface{}, ttl time.Duration) (oldValue interface{}, err error)

	// PutAll copies all of the mappings from the specified map to this map.
	// The effect of this call is equivalent to that of calling put(k, v)
	// on this map once for each mapping from key k to value v in the specified
	// map. The behavior of this operation is undefined if the specified map is modified
	// while the operation is in progress.
	//
	// Any change in the returned map will not be reflected on server side.
	PutAll(entries map[interface{}]interface{}) (err error)

	// Get returns the value to which the specified key is mapped, or nil if this map
	// contains no mapping for the key. If this map permits nil values, then a return value of nil does not
	// necessarily indicate that the map contains no mapping for the key; it's also
	// possible that the map explicitly maps the key to nil.
	//
	// It returns a clone of the value,
	// not the original (identically equal) value put into the map.
	Get(key interface{}) (value interface{}, err error)

	// ContainsKey returns true if this map contains a mapping for the specified key.
	ContainsKey(key interface{}) (found bool, err error)

	// ContainsValue returns true if this map maps one or more keys to the specified value.
	ContainsValue(value interface{}) (found bool, err error)

	// Clear wipes data out of the replicated maps.
	// If some node fails on executing the operation, it is retried for at most
	// 5 times (on the failing nodes only).
	Clear() (err error)

	// Remove removes the mapping for a key from this map if it is present (optional operation).
	// Remove returns the value to which this map previously associated the key,
	// or nil if the map contained no mapping for the key. If this map permits nil values,
	// then a return value of nil does not necessarily indicate that the map contained
	// no mapping for the key; it's also possible that the map explicitly mapped the key to nil.
	// The map will not contain a mapping for the specified key once the call returns.
	Remove(key interface{}) (value interface{}, err error)

	// IsEmpty returns true if this map has no entries, false otherwise.
	IsEmpty() (empty bool, err error)

	// Size returns the number of key-value mappings in this map.
	Size() (size int32, err error)

	// Values returns a slice of values contained in this map.
	Values() (values []interface{}, err error)

	// KeySet returns a view of the key contained in this map.
	KeySet() (keySet []interface{}, err error)

	// EntrySet returns entries as a slice of key-value pairs.
	EntrySet() (resultPairs []Pair, err error)

	// AddEntryListener adds an entry listener for this map. The listener will be notified for all
	// map add/remove/update/evict events.
	// To receive an event, listener should implement a corresponding interface for that event.
	// Supported listeners for ReplicatedMap:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * MapClearedListener
	// AddEntryListener returns registration id of the listener.
	AddEntryListener(listener interface{}) (registrationID string, err error)

	// AddEntryListenerWithPredicate adds a continuous entry listener for this map. The listener will be notified for
	// map add/remove/update/evict events filtered by the given predicate.
	// To receive an event, listener should implement a corresponding interface for that event.
	// Supported listeners for ReplicatedMap:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * MapClearedListener
	// AddEntryListenerWithPredicate returns registration id of the listener.
	AddEntryListenerWithPredicate(listener interface{}, predicate interface{}) (registrationID string, err error)

	// AddEntryListenerToKey adds the specified entry listener for the specified key. The listener will be
	// notified for all add/remove/update/evict events of the specified key only.
	// To receive an event, listener should implement a corresponding interface for that event.
	// Supported listeners for ReplicatedMap:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * MapClearedListener
	// AddEntryListenerToKey returns registration id of the listener.
	AddEntryListenerToKey(listener interface{}, key interface{}) (registrationID string, err error)

	// AddEntryListenerToKeyWithPredicate adds a continuous entry listener for this map. The listener will be notified for
	// map add/remove/update/evict events filtered by the given predicate and key.
	// To receive an event, listener should implement a corresponding interface for that event.
	// Supported listeners for ReplicatedMap:
	//  * EntryAddedListener
	//  * EntryRemovedListener
	//  * EntryUpdatedListener
	//  * EntryEvictedListener
	//  * MapClearedListener
	// AddEntryListenerToKeyWithPredicate returns registration id of the listener.
	AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{}, key interface{}) (
		registrationID string, err error)

	// RemoveEntryListener removes the specified entry listener and returns silently if there was no such
	// listener added before.
	// It returns true if remove operation is successful, false if unsuccessful or this listener did not exist.
	RemoveEntryListener(registrationID string) (removed bool, err error)
}
