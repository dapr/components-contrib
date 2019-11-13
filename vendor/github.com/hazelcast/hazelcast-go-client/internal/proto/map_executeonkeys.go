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

func mapExecuteOnKeysCalculateSize(name string, entryProcessor serialization.Data, keys []serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(entryProcessor)
	dataSize += bufutil.Int32SizeInBytes
	for _, keysItem := range keys {
		dataSize += dataCalculateSize(keysItem)
	}
	return dataSize
}

// MapExecuteOnKeysEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapExecuteOnKeysEncodeRequest(name string, entryProcessor serialization.Data, keys []serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, mapExecuteOnKeysCalculateSize(name, entryProcessor, keys))
	clientMessage.SetMessageType(mapExecuteOnKeys)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendData(entryProcessor)
	clientMessage.AppendInt32(int32(len(keys)))
	for _, keysItem := range keys {
		clientMessage.AppendData(keysItem)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// MapExecuteOnKeysDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapExecuteOnKeysDecodeResponse(clientMessage *ClientMessage) func() (response []*Pair) {
	// Decode response from client message
	return func() (response []*Pair) {
		responseSize := clientMessage.ReadInt32()
		response = make([]*Pair, responseSize)
		for responseIndex := 0; responseIndex < int(responseSize); responseIndex++ {
			responseItemKey := clientMessage.ReadData()
			responseItemValue := clientMessage.ReadData()
			var responseItem = &Pair{key: responseItemKey, value: responseItemValue}
			response[responseIndex] = responseItem
		}
		return
	}
}
