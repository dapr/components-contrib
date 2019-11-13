// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func mapAddEntryListenerWithPredicateCalculateSize(name string, predicate serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(predicate)
	dataSize += bufutil.BoolSizeInBytes
	dataSize += bufutil.Int32SizeInBytes
	dataSize += bufutil.BoolSizeInBytes
	return dataSize
}

// MapAddEntryListenerWithPredicateEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapAddEntryListenerWithPredicateEncodeRequest(name string, predicate serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, mapAddEntryListenerWithPredicateCalculateSize(name, predicate, includeValue, listenerFlags, localOnly))
	clientMessage.SetMessageType(mapAddEntryListenerWithPredicate)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendData(predicate)
	clientMessage.AppendBool(includeValue)
	clientMessage.AppendInt32(listenerFlags)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// MapAddEntryListenerWithPredicateDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapAddEntryListenerWithPredicateDecodeResponse(clientMessage *ClientMessage) func() (response string) {
	// Decode response from client message
	return func() (response string) {
		response = clientMessage.ReadString()
		return
	}
}

// MapAddEntryListenerWithPredicateHandleEventEntryFunc is the event handler function.
type MapAddEntryListenerWithPredicateHandleEventEntryFunc func(serialization.Data, serialization.Data, serialization.Data, serialization.Data, int32, string, int32)

// MapAddEntryListenerWithPredicateEventEntryDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func MapAddEntryListenerWithPredicateEventEntryDecode(clientMessage *ClientMessage) (
	key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid string, numberOfAffectedEntries int32) {

	if !clientMessage.ReadBool() {
		key = clientMessage.ReadData()
	}

	if !clientMessage.ReadBool() {
		value = clientMessage.ReadData()
	}

	if !clientMessage.ReadBool() {
		oldValue = clientMessage.ReadData()
	}

	if !clientMessage.ReadBool() {
		mergingValue = clientMessage.ReadData()
	}
	eventType = clientMessage.ReadInt32()
	uuid = clientMessage.ReadString()
	numberOfAffectedEntries = clientMessage.ReadInt32()
	return
}

// MapAddEntryListenerWithPredicateHandle handles the event with the given
// event handler function.
func MapAddEntryListenerWithPredicateHandle(clientMessage *ClientMessage,
	handleEventEntry MapAddEntryListenerWithPredicateHandleEventEntryFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == bufutil.EventEntry && handleEventEntry != nil {
		handleEventEntry(MapAddEntryListenerWithPredicateEventEntryDecode(clientMessage))
	}
}
