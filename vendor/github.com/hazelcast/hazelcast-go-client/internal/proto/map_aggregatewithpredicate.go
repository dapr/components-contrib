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
)

func mapAggregateWithPredicateCalculateSize(name string, aggregator serialization.Data, predicate serialization.Data) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(aggregator)
	dataSize += dataCalculateSize(predicate)
	return dataSize
}

// MapAggregateWithPredicateEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MapAggregateWithPredicateEncodeRequest(name string, aggregator serialization.Data, predicate serialization.Data) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, mapAggregateWithPredicateCalculateSize(name, aggregator, predicate))
	clientMessage.SetMessageType(mapAggregateWithPredicate)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(name)
	clientMessage.AppendData(aggregator)
	clientMessage.AppendData(predicate)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

// MapAggregateWithPredicateDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MapAggregateWithPredicateDecodeResponse(clientMessage *ClientMessage) func() (response serialization.Data) {
	// Decode response from client message
	return func() (response serialization.Data) {
		if clientMessage.IsComplete() {
			return
		}

		if !clientMessage.ReadBool() {
			response = clientMessage.ReadData()
		}
		return
	}
}
