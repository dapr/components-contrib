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

func pncounterAddCalculateSize(name string, delta int64, getBeforeUpdate bool, replicaTimestamps []*Pair, targetReplica *Address) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.Int64SizeInBytes
	dataSize += bufutil.BoolSizeInBytes
	dataSize += bufutil.Int32SizeInBytes
	for _, replicaTimestampsItem := range replicaTimestamps {
		key := replicaTimestampsItem.key.(string)
		val := replicaTimestampsItem.value.(int64)
		dataSize += stringCalculateSize(key)
		dataSize += int64CalculateSize(val)
	}
	dataSize += addressCalculateSize(targetReplica)
	return dataSize
}

// PNCounterAddEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func PNCounterAddEncodeRequest(name string, delta int64, getBeforeUpdate bool, replicaTimestamps []*Pair, targetReplica *Address) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, pncounterAddCalculateSize(name, delta, getBeforeUpdate, replicaTimestamps, targetReplica))
	clientMessage.SetMessageType(pncounterAdd)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendInt64(delta)
	clientMessage.AppendBool(getBeforeUpdate)
	clientMessage.AppendInt32(int32(len(replicaTimestamps)))
	for _, replicaTimestampsItem := range replicaTimestamps {
		key := replicaTimestampsItem.key.(string)
		val := replicaTimestampsItem.value.(int64)
		clientMessage.AppendString(key)
		clientMessage.AppendInt64(val)
	}
	AddressCodecEncode(clientMessage, targetReplica)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// PNCounterAddDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func PNCounterAddDecodeResponse(clientMessage *ClientMessage) func() (value int64, replicaTimestamps []*Pair, replicaCount int32) {
	// Decode response from client message
	return func() (value int64, replicaTimestamps []*Pair, replicaCount int32) {
		if clientMessage.IsComplete() {
			return
		}
		value = clientMessage.ReadInt64()
		replicaTimestampsSize := clientMessage.ReadInt32()
		replicaTimestamps = make([]*Pair, replicaTimestampsSize)
		for replicaTimestampsIndex := 0; replicaTimestampsIndex < int(replicaTimestampsSize); replicaTimestampsIndex++ {
			replicaTimestampsItemKey := clientMessage.ReadString()
			replicaTimestampsItemValue := clientMessage.ReadInt64()
			var replicaTimestampsItem = &Pair{key: replicaTimestampsItemKey, value: replicaTimestampsItemValue}
			replicaTimestamps[replicaTimestampsIndex] = replicaTimestampsItem
		}
		replicaCount = clientMessage.ReadInt32()
		return
	}
}
