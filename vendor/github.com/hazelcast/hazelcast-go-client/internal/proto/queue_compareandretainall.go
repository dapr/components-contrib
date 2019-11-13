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

func queueCompareAndRetainAllCalculateSize(name string, dataList []serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int32SizeInBytes
	for _, dataListItem := range dataList {
		dataSize += dataCalculateSize(dataListItem)
	}
	return dataSize
}

// QueueCompareAndRetainAllEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func QueueCompareAndRetainAllEncodeRequest(name string, dataList []serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, queueCompareAndRetainAllCalculateSize(name, dataList))
	clientMessage.SetMessageType(queueCompareAndRetainAll)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(int32(len(dataList)))
	for _, dataListItem := range dataList {
		clientMessage.AppendData(dataListItem)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// QueueCompareAndRetainAllDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func QueueCompareAndRetainAllDecodeResponse(clientMessage *ClientMessage) func() (response bool) {
	// Decode response from client message
	return func() (response bool) {
		response = clientMessage.ReadBool()
		return
	}
}
