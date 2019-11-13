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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func listGetCalculateSize(name string, index int32) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int32SizeInBytes
	return dataSize
}

// ListGetEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func ListGetEncodeRequest(name string, index int32) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, listGetCalculateSize(name, index))
	clientMessage.SetMessageType(listGet)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(index)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// ListGetDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func ListGetDecodeResponse(clientMessage *ClientMessage) func() (response serialization.Data) {
	// Decode response from client message
	return func() (response serialization.Data) {

		if !clientMessage.ReadBool() {
			response = clientMessage.ReadData()
		}
		return
	}
}
