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

// Package core provides core API interfaces/classes.
package core

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
)

// Address represents an address of a member in the cluster.
type Address interface {
	fmt.Stringer
	// Host returns host of the member.
	Host() string

	// Port returns the port of the member.
	Port() int
}

// Member represents a member in the cluster with its address, uuid, lite member status and attributes.
type Member interface {
	fmt.Stringer
	// Address returns the address of this member.
	Address() Address

	// UUID returns the uuid of this member.
	UUID() string

	// IsLiteMember returns true if this member is a lite member.
	IsLiteMember() bool

	// Attributes returns configured attributes for this member.
	Attributes() map[string]string
}

// Pair represents Map entry pair.
type Pair interface {
	// Key returns key of entry.
	Key() interface{}

	// Values returns value of entry.
	Value() interface{}
}

// EntryView represents a readonly view of a map entry.
type EntryView interface {
	// Key returns the key of the entry.
	Key() interface{}

	// Value returns the value of the entry.
	Value() interface{}

	// Cost returns the cost in bytes of the entry.
	Cost() int64

	// CreationTime returns the creation time of the entry.
	CreationTime() time.Time

	// ExpirationTime returns the expiration time of the entry.
	ExpirationTime() time.Time

	// Hits returns the number of hits of the entry.
	Hits() int64

	// LastAccessTime returns the last access time for the entry.
	LastAccessTime() time.Time

	// LastStoredTime returns the last store time for the value.
	LastStoredTime() time.Time

	// LastUpdateTime returns the last time the value was updated.
	LastUpdateTime() time.Time

	// Version returns the version of the entry.
	Version() int64

	// EvictionCriteriaNumber returns the criteria number for eviction.
	EvictionCriteriaNumber() int64

	// TTL returns the last set time to live second.
	TTL() time.Duration
}

// AbstractMapEvent is base for a map event.
type AbstractMapEvent interface {
	// Name returns the name of the map for this event.
	Name() string

	// Member returns the member that fired this event.
	Member() Member

	// EventType returns the type of entry event.
	EventType() int32

	// String returns a string representation of this event.
	String() string
}

// EntryEvent is map entry event.
type EntryEvent interface {
	// AbstractMapEvent is base for a map event.
	AbstractMapEvent

	// Key returns the key of the entry event.
	Key() interface{}

	// Value returns the value of the entry event.
	Value() interface{}

	// OldValue returns the old value of the entry event.
	OldValue() interface{}

	// MergingValue returns the incoming merging value of the entry event.
	MergingValue() interface{}
}

// MapEvent is map events common contract.
type MapEvent interface {
	// AbstractMapEvent is base for a map event.
	AbstractMapEvent

	// NumberOfAffectedEntries returns the number of affected
	// entries by this event.
	NumberOfAffectedEntries() int32
}

// ItemEvent is List, Set and Queue events common contract.
type ItemEvent interface {
	// Name returns the name of List, Set or Queue.
	Name() string

	// Item returns the item of the event.
	Item() interface{}

	// EventType returns 1 if an item is added, 2 if an item is removed.
	EventType() int32

	// Member is the member that sent the event.
	Member() Member
}

// MemberAttributeEvent is an event for member attribute changes.
type MemberAttributeEvent interface {
	// OperationType returns the type of member attribute change for this MemberAttributeEvent
	OperationType() int32

	// Key returns the key for this MemberAttributeEvent.
	Key() string

	// Value returns the value for this MemberAttributeEvent
	Value() string

	// Member returns the member for this MemberAttributeEvent.
	Member() Member
}

// EntryAddedListener is invoked upon addition of an entry.
type EntryAddedListener interface {
	// EntryAdded is invoked upon addition of an entry.
	EntryAdded(event EntryEvent)
}

// EntryRemovedListener invoked upon removal of an entry.
type EntryRemovedListener interface {
	// EntryRemoved invoked upon removal of an entry.
	EntryRemoved(event EntryEvent)
}

// EntryUpdatedListener is invoked upon update of an entry.
type EntryUpdatedListener interface {
	// EntryUpdated is invoked upon update of an entry.
	EntryUpdated(event EntryEvent)
}

// EntryEvictedListener is invoked upon eviction of an entry.
type EntryEvictedListener interface {
	// EntryEvicted is invoked upon eviction of an entry.
	EntryEvicted(event EntryEvent)
}

// EntryMergedListener is invoked after WAN replicated entry is merged.
type EntryMergedListener interface {
	// EntryMerged is invoked after WAN replicated entry is merged.
	EntryMerged(event EntryEvent)
}

// EntryExpiredListener which is notified after removal of an entry due to the expiration-based-eviction.
type EntryExpiredListener interface {
	// EntryExpired is invoked upon expiration of an entry.
	EntryExpired(event EntryEvent)
}

// MapEvictedListener is invoked when all entries are evicted
// by Map.EvictAll method.
type MapEvictedListener interface {
	// MapEvicted is invoked when all entries are evicted
	// by Map.EvictAll method.
	MapEvicted(event MapEvent)
}

// MapClearedListener is invoked when all entries are removed
// by Map.Clear method.
type MapClearedListener interface {
	// MapCleared is invoked when all entries are removed
	// by Map.Clear method.
	MapCleared(event MapEvent)
}

// MemberAddedListener is invoked when a new member is added to the cluster.
type MemberAddedListener interface {
	// MemberAdded is invoked when a new member is added to the cluster.
	MemberAdded(member Member)
}

// MemberRemovedListener is invoked when an existing member leaves the cluster.
type MemberRemovedListener interface {
	// MemberRemoved is invoked when an existing member leaves the cluster.
	MemberRemoved(member Member)
}

const (
	// MemberAttributeOperationTypePut indicates an attribute being put.
	MemberAttributeOperationTypePut int32 = 1

	// MemberAttributeOperationTypeRemove indicates an attribute being removed.
	MemberAttributeOperationTypeRemove int32 = 2
)

// MemberAttributeChangedListener is invoked when attribute of a member has changed.
type MemberAttributeChangedListener interface {
	// MemberAttributeChanged is invoked when attribute of a member has changed.
	MemberAttributeChanged(event MemberAttributeEvent)
}

// LifecycleListener is a listener object for listening to lifecycle events of the Hazelcast instance.
type LifecycleListener interface {
	// LifecycleStateChanged is called when instance's state changes. No blocking calls should be made in this method.
	LifecycleStateChanged(string)
}

// MessageListener is a listener for Topic.
// Provided that a MessageListener is not registered twice, a MessageListener will never be called concurrently.
// So there is no need to provide thread-safety on internal state in the MessageListener. Also there is no need to enforce
// safe publication, the Topic is responsible for the memory consistency effects. In other words, there is no need to make
// internal fields of the MessageListener volatile or access them using synchronized blocks.
type MessageListener interface {
	// OnMessage is invoked when a message is received for the added topic. Note that topic guarantees message ordering.
	// Therefore there is only one thread invoking OnMessage.
	OnMessage(message Message) error
}

// Message is a message for Topic.
type Message interface {
	// MessageObject returns the published message.
	MessageObject() interface{}

	// PublishTime returns the time in milliseconds when the message is published.
	PublishTime() time.Time

	// PublishMember returns the member that published the message.
	// The member can be nil if:
	//    - The message was sent by a client and not a member.
	//    - The member, that sent the message, left the cluster before the message was processed.
	PublishingMember() Member
}

// ItemAddedListener is invoked when an item is added.
type ItemAddedListener interface {
	ItemAdded(event ItemEvent)
}

// ItemRemovedListener is invoked when an item is removed.
type ItemRemovedListener interface {
	ItemRemoved(event ItemEvent)
}

// LoadBalancer allows you to send operations to one of a number of endpoints(Members).
// It is up to the implementation to use different load balancing policies.
//
// If client is configured with smart routing,
// only the operations that are not key based will be routed to the endpoint returned by the LoadBalancer.
// If the client is not smart routing, LoadBalancer will not be used.
type LoadBalancer interface {
	// Init initializes LoadBalancer with the given cluster.
	// The given cluster is used to select members.
	Init(cluster Cluster)

	// Next returns the next member to route to.
	// It returns nil if no member is available.
	Next() Member
}

// HazelcastJSONValue is a wrapper for JSON formatted strings. It is
// preferred to store HazelcastJSONValue instead of string/[]byte for
// JSON. Users can run predicates/aggregations and use indexes on
// the attributes of the underlying JSON objects.
//
// HazelcastJSONValue is queried using Hazelcast's querying language.
//
// HazelcastJSONValue keeps given string as it is. Strings are not
// checked for being valid. Ill-formatted JSON strings may cause false
// positive or false negative results in queries.
type HazelcastJSONValue struct {
	jsonString []byte
}

// CreateHazelcastJSONValueFromString returns *HazelcastJSONValue initialized with the given string
func CreateHazelcastJSONValueFromString(jsonString string) *HazelcastJSONValue {
	return &HazelcastJSONValue{[]byte(jsonString)}
}

// CreateHazelcastJSONValue returns *HazelcastJSONValue constructed from the provided object
// if nil, channel, complex or function values are given, method returns error
func CreateHazelcastJSONValue(object interface{}) (*HazelcastJSONValue, error) {
	if nilutil.IsNil(object) {
		return nil, NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	byteArray, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return &HazelcastJSONValue{byteArray}, nil
}

// Unmarshal converts HazelcastJSONValue into given v object
// Returns error if JSON is not valid
func (h *HazelcastJSONValue) Unmarshal(v interface{}) error {
	return json.Unmarshal(h.jsonString, &v)
}

// ToString returns unaltered string that was used to create this object.
func (h *HazelcastJSONValue) ToString() string {
	return string(h.jsonString)
}

// StackTraceElement contains stacktrace information for server side exception.
type StackTraceElement interface {
	// DeclaringClass returns the fully qualified name of the class containing
	// the execution point represented by the stack trace element.
	DeclaringClass() string

	// MethodName returns the name of the method containing the execution point
	// represented by this stack trace element.
	MethodName() string

	// FileName returns the name of the file containing the execution point
	// represented by the stack trace element, or nil if
	// this information is unavailable.
	FileName() string

	// LineNumber returns the line number of the source line containing the
	// execution point represented by this stack trace element, or
	// a negative number if this information is unavailable. A value
	// of -2 indicates that the method containing the execution point
	// is a native method.
	LineNumber() int32
}

// ServerError contains error information that occurred in the server.
type ServerError interface {
	// ErrorCode returns the error code.
	ErrorCode() int32

	// ClassName returns the class name where error occurred.
	ClassName() string

	// Message returns the error message.
	Message() string

	// StackTrace returns a slice of StackTraceElement.
	StackTrace() []StackTraceElement

	// CauseErrorCode returns the cause error code.
	CauseErrorCode() int32

	// CauseClassName returns the cause class name.
	CauseClassName() string
}

// ReliableMessageListener is to better integrate with the reliable topic.
//
// If a regular MessageListener is registered on a reliable topic, the message listener works fine, but it can't do much
// more than listen to messages.
//
// If a ReliableMessageListener is registered on a normal topic, only the MessageListener methods will be called.
//
//  Durable Subscription
// The ReliableMessageListener allows you to control where you want to start processing a message when the listener is
// registered. This makes it possible to create a durable subscription by storing the sequence of the last message and
// using this sequenceId as the sequenceId to start from.
//
//  Error handling
// The ReliableMessageListener also gives the ability to deal with errors using the IsTerminal(error)
// method. If a plain MessageListener is used, then it won't terminate on errors and it will keep on running. But in some
// cases it is better to stop running.
//
//  Global order
// The ReliableMessageListener will always get all events in order (global order). It will not get duplicates and
// there will only be gaps if it is too slow. For more information see IsLossTolerant().
//
//  Delivery guarantees
// Because the ReliableMessageListener controls which item it wants to continue from upon restart, it is very easy to provide
// an at-least-once or at-most-once delivery guarantee. The storeSequence is always called before a message is processed;
// so it can be persisted on some non-volatile storage. When the RetrieveInitialSequence  returns the stored
// sequence, then an at-least-once delivery is implemented since the same item is now being processed twice. To implement
// an at-most-once delivery guarantee, add 1 to the stored sequence when the RetrieveInitialSequence is called.
type ReliableMessageListener interface {
	MessageListener

	// RetrieveInitialSequence retrieves the initial sequence from which this ReliableMessageListener should start.
	//
	// Return -1 if there is no initial sequence and you want to start from the next published message.
	//
	// If you intent to create a durable subscriber so you continue from where you stopped the previous
	// time, load the previous sequence and add 1. If you don't add one, then you will be receiving the
	// same message twice.
	RetrieveInitialSequence() int64
	// StoreSequence informs the ReliableMessageListener that it should store the sequence.
	// This method is called before the message is
	// processed. Can be used to make a durable subscription.
	StoreSequence(sequence int64)

	// IsLossTolerant checks if this ReliableMessageListener is able to deal with message loss.
	// Even though the reliable topic promises to
	// be reliable, it can be that a MessageListener is too slow. Eventually the message won't be available anymore.
	//
	// If the ReliableMessageListener is not loss tolerant and the topic detects that there are missing messages, it will
	// terminate the ReliableMessageListener.
	IsLossTolerant() bool

	// IsTerminal checks if the ReliableMessageListener should be terminated based on an error returned while calling
	// MessageListener.OnMessage().
	IsTerminal(err error) (bool, error)
}
