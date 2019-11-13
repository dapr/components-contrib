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

func mapPutAllCalculateSize(name string, entries []*Pair) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int32SizeInBytes
	for _, entriesItem := range entries {
		key := entriesItem.key.(serialization.Data)
		val := entriesItem.value.(serialization.Data)
		dataSize += dataCalculateSize(key)
		dataSize += dataCalculateSize(val)
	}
	return dataSize
}

// MapPutAllEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapPutAllEncodeRequest(name string, entries []*Pair) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, mapPutAllCalculateSize(name, entries))
	clientMessage.SetMessageType(mapPutAll)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(int32(len(entries)))
	for _, entriesItem := range entries {
		key := entriesItem.key.(serialization.Data)
		val := entriesItem.value.(serialization.Data)
		clientMessage.AppendData(key)
		clientMessage.AppendData(val)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// MapPutAllDecodeResponse(clientMessage *ClientMessage), this message has no parameters to decode
