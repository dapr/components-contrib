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

func ringbufferReadManyCalculateSize(name string, startSequence int64, minCount int32, maxCount int32, filter serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int64SizeInBytes
	dataSize += bufutil.Int32SizeInBytes
	dataSize += bufutil.Int32SizeInBytes
	dataSize += bufutil.BoolSizeInBytes
	if filter != nil {
		dataSize += dataCalculateSize(filter)
	}
	return dataSize
}

// RingbufferReadManyEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func RingbufferReadManyEncodeRequest(name string, startSequence int64, minCount int32, maxCount int32, filter serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ringbufferReadManyCalculateSize(name, startSequence, minCount, maxCount, filter))
	clientMessage.SetMessageType(ringbufferReadMany)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendInt64(startSequence)
	clientMessage.AppendInt32(minCount)
	clientMessage.AppendInt32(maxCount)
	clientMessage.AppendBool(filter == nil)
	if filter != nil {
		clientMessage.AppendData(filter)
	}
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// RingbufferReadManyDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func RingbufferReadManyDecodeResponse(clientMessage *ClientMessage) func() (readCount int32, items []serialization.Data, itemSeqs []int64, nextSeq int64) {
	// Decode response from client message
	return func() (readCount int32, items []serialization.Data, itemSeqs []int64, nextSeq int64) {
		readCount = clientMessage.ReadInt32()
		itemsSize := clientMessage.ReadInt32()
		items = make([]serialization.Data, itemsSize)
		for itemsIndex := 0; itemsIndex < int(itemsSize); itemsIndex++ {
			itemsItem := clientMessage.ReadData()
			items[itemsIndex] = itemsItem
		}
		if clientMessage.IsComplete() {
			return
		}

		if !clientMessage.ReadBool() {
			itemSeqsSize := clientMessage.ReadInt32()
			itemSeqs = make([]int64, itemSeqsSize)
			for itemSeqsIndex := 0; itemSeqsIndex < int(itemSeqsSize); itemSeqsIndex++ {
				itemSeqsItem := clientMessage.ReadInt64()
				itemSeqs[itemSeqsIndex] = itemSeqsItem
			}
		}
		if clientMessage.IsComplete() {
			return
		}
		nextSeq = clientMessage.ReadInt64()
		return
	}
}
