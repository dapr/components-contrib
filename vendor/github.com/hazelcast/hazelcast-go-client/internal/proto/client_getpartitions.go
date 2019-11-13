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

func clientGetPartitionsCalculateSize() int {
	// Calculates the request payload size
	dataSize := 0
	return dataSize
}

// ClientGetPartitionsEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func ClientGetPartitionsEncodeRequest() *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, clientGetPartitionsCalculateSize())
	clientMessage.SetMessageType(clientGetPartitions)
	clientMessage.IsRetryable = false
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// ClientGetPartitionsDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func ClientGetPartitionsDecodeResponse(clientMessage *ClientMessage) func() (partitions []*Pair, partitionStateVersion int32) {
	// Decode response from client message
	return func() (partitions []*Pair, partitionStateVersion int32) {
		partitionsSize := clientMessage.ReadInt32()
		partitions = make([]*Pair, partitionsSize)
		for partitionsIndex := 0; partitionsIndex < int(partitionsSize); partitionsIndex++ {
			partitionsItemKey := AddressCodecDecode(clientMessage)
			partitionsItemValueSize := clientMessage.ReadInt32()
			partitionsItemValue := make([]int32, partitionsItemValueSize)
			for partitionsItemValueIndex := 0; partitionsItemValueIndex < int(partitionsItemValueSize); partitionsItemValueIndex++ {
				partitionsItemValueItem := clientMessage.ReadInt32()
				partitionsItemValue[partitionsItemValueIndex] = partitionsItemValueItem
			}
			var partitionsItem = &Pair{key: partitionsItemKey, value: partitionsItemValue}
			partitions[partitionsIndex] = partitionsItem
		}
		if clientMessage.IsComplete() {
			return
		}
		partitionStateVersion = clientMessage.ReadInt32()
		return
	}
}
