// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

package reliabletopic

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/util/timeutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type Message struct {
	publishTime      int64
	publisherAddress core.Address
	payload          serialization.Data
}

func NewMessage(payload serialization.Data, publisherAddr core.Address) *Message {
	return &Message{
		publishTime:      timeutil.GetCurrentTimeInMillis(),
		payload:          payload,
		publisherAddress: publisherAddr,
	}
}

func (r *Message) Payload() serialization.Data {
	return r.payload
}

func (r *Message) PublishTime() int64 {
	return r.publishTime
}

func (r *Message) PublisherAddress() core.Address {
	return r.publisherAddress
}

const (
	// FactoryID is the factory id of reliable topic message.
	FactoryID = -18

	// MessageClassID is the class id of reliable topic message.
	MessageClassID = 2
)

func (r *Message) FactoryID() (factoryID int32) {
	return FactoryID
}

func (r *Message) ClassID() (classID int32) {
	return MessageClassID
}

func (r *Message) WriteData(output serialization.DataOutput) (err error) {
	output.WriteInt64(r.publishTime)
	err = output.WriteObject(r.publisherAddress)
	if err != nil {
		return
	}
	output.WriteData(r.payload)
	return
}

func (r *Message) ReadData(input serialization.DataInput) (err error) {
	r.publishTime = input.ReadInt64()
	addrObj := input.ReadObject()
	if addr, ok := addrObj.(core.Address); ok {
		r.publisherAddress = addr
	}
	r.payload = input.ReadData()
	return input.Error()
}

type MessageFactory struct {
}

func NewMessageFactory() *MessageFactory {
	return &MessageFactory{}
}

func (r *MessageFactory) Create(id int32) (instance serialization.IdentifiedDataSerializable) {
	if id == MessageClassID {
		return &Message{}
	}
	return nil
}
