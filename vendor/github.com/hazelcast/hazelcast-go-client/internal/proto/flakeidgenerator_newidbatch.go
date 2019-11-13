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

func flakeidgeneratorNewIdBatchCalculateSize(name string, batchSize int32) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int32SizeInBytes
	return dataSize
}

// FlakeIDGeneratorNewIDBatchEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func FlakeIDGeneratorNewIDBatchEncodeRequest(name string, batchSize int32) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, flakeidgeneratorNewIdBatchCalculateSize(name, batchSize))
	clientMessage.SetMessageType(flakeidgeneratorNewIdBatch)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt32(batchSize)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// FlakeIDGeneratorNewIDBatchDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func FlakeIDGeneratorNewIDBatchDecodeResponse(clientMessage *ClientMessage) func() (base int64, increment int64, batchSize int32) {
	// Decode response from client message
	return func() (base int64, increment int64, batchSize int32) {
		if clientMessage.IsComplete() {
			return
		}
		base = clientMessage.ReadInt64()
		increment = clientMessage.ReadInt64()
		batchSize = clientMessage.ReadInt32()
		return
	}
}
