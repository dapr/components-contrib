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

func queueAddListenerCalculateSize(name string, includeValue bool, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.BoolSizeInBytes
	dataSize += bufutil.BoolSizeInBytes
	return dataSize
}

// QueueAddListenerEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func QueueAddListenerEncodeRequest(name string, includeValue bool, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, queueAddListenerCalculateSize(name, includeValue, localOnly))
	clientMessage.SetMessageType(queueAddListener)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendBool(includeValue)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// QueueAddListenerDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func QueueAddListenerDecodeResponse(clientMessage *ClientMessage) func() (response string) {
	// Decode response from client message
	return func() (response string) {
		response = clientMessage.ReadString()
		return
	}
}

// QueueAddListenerHandleEventItemFunc is the event handler function.
type QueueAddListenerHandleEventItemFunc func(serialization.Data, string, int32)

// QueueAddListenerEventItemDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func QueueAddListenerEventItemDecode(clientMessage *ClientMessage) (
	item serialization.Data, uuid string, eventType int32) {

	if !clientMessage.ReadBool() {
		item = clientMessage.ReadData()
	}
	uuid = clientMessage.ReadString()
	eventType = clientMessage.ReadInt32()
	return
}

// QueueAddListenerHandle handles the event with the given
// event handler function.
func QueueAddListenerHandle(clientMessage *ClientMessage,
	handleEventItem QueueAddListenerHandleEventItemFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == bufutil.EventItem && handleEventItem != nil {
		handleEventItem(QueueAddListenerEventItemDecode(clientMessage))
	}
}
