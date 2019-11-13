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
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func clientAddMembershipListenerCalculateSize(localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += bufutil.BoolSizeInBytes
	return dataSize
}

// ClientAddMembershipListenerEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func ClientAddMembershipListenerEncodeRequest(localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, clientAddMembershipListenerCalculateSize(localOnly))
	clientMessage.SetMessageType(clientAddMembershipListener)
	clientMessage.IsRetryable = false
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// ClientAddMembershipListenerDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func ClientAddMembershipListenerDecodeResponse(clientMessage *ClientMessage) func() (response string) {
	// Decode response from client message
	return func() (response string) {
		response = clientMessage.ReadString()
		return
	}
}

// ClientAddMembershipListenerHandleEventMemberFunc is the event handler function.
type ClientAddMembershipListenerHandleEventMemberFunc func(*Member, int32)

// ClientAddMembershipListenerHandleEventMemberListFunc is the event handler function.
type ClientAddMembershipListenerHandleEventMemberListFunc func([]*Member)

// ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc is the event handler function.
type ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc func(string, string, int32, string)

// ClientAddMembershipListenerEventMemberDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func ClientAddMembershipListenerEventMemberDecode(clientMessage *ClientMessage) (
	member *Member, eventType int32) {
	member = MemberCodecDecode(clientMessage)
	eventType = clientMessage.ReadInt32()
	return
}

// ClientAddMembershipListenerEventMemberListDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func ClientAddMembershipListenerEventMemberListDecode(clientMessage *ClientMessage) (
	members []*Member) {
	membersSize := clientMessage.ReadInt32()
	members = make([]*Member, membersSize)
	for membersIndex := 0; membersIndex < int(membersSize); membersIndex++ {
		membersItem := MemberCodecDecode(clientMessage)
		members[membersIndex] = membersItem
	}
	return
}

// ClientAddMembershipListenerEventMemberAttributeChangeDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func ClientAddMembershipListenerEventMemberAttributeChangeDecode(clientMessage *ClientMessage) (
	uuid string, key string, operationType int32, value string) {
	uuid = clientMessage.ReadString()
	key = clientMessage.ReadString()
	operationType = clientMessage.ReadInt32()

	if !clientMessage.ReadBool() {
		value = clientMessage.ReadString()
	}
	return
}

// ClientAddMembershipListenerHandle handles the event with the given
// event handler function.
func ClientAddMembershipListenerHandle(clientMessage *ClientMessage,
	handleEventMember ClientAddMembershipListenerHandleEventMemberFunc,
	handleEventMemberList ClientAddMembershipListenerHandleEventMemberListFunc,
	handleEventMemberAttributeChange ClientAddMembershipListenerHandleEventMemberAttributeChangeFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == bufutil.EventMember && handleEventMember != nil {
		handleEventMember(ClientAddMembershipListenerEventMemberDecode(clientMessage))
	}
	if messageType == bufutil.EventMemberList && handleEventMemberList != nil {
		handleEventMemberList(ClientAddMembershipListenerEventMemberListDecode(clientMessage))
	}
	if messageType == bufutil.EventMemberAttributeChange && handleEventMemberAttributeChange != nil {
		handleEventMemberAttributeChange(ClientAddMembershipListenerEventMemberAttributeChangeDecode(clientMessage))
	}
}
