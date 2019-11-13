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

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type setProxy struct {
	*partitionSpecificProxy
}

func newSetProxy(client *HazelcastClient, serviceName string, name string) *setProxy {
	parSpecProxy := newPartitionSpecificProxy(client, serviceName, name)
	return &setProxy{parSpecProxy}
}

func (sp *setProxy) Add(item interface{}) (added bool, err error) {
	itemData, err := sp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := proto.SetAddEncodeRequest(sp.name, itemData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetAddDecodeResponse)
}

func (sp *setProxy) AddAll(items []interface{}) (changed bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.SetAddAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetAddAllDecodeResponse)
}

func (sp *setProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID string, err error) {
	err = sp.validateItemListener(listener)
	if err != nil {
		return
	}
	request := proto.SetAddListenerEncodeRequest(sp.name, includeValue, false)
	eventHandler := sp.createEventHandler(listener)
	return sp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID string) *proto.ClientMessage {
			return proto.SetRemoveListenerEncodeRequest(sp.name, registrationID)
		}, func(clientMessage *proto.ClientMessage) string {
			return proto.SetAddListenerDecodeResponse(clientMessage)()
		})

}

func (sp *setProxy) Clear() (err error) {
	request := proto.SetClearEncodeRequest(sp.name)
	_, err = sp.invoke(request)
	return err
}

func (sp *setProxy) Contains(item interface{}) (found bool, err error) {
	itemData, err := sp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := proto.SetContainsEncodeRequest(sp.name, itemData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetContainsDecodeResponse)
}

func (sp *setProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.SetContainsAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetContainsAllDecodeResponse)
}

func (sp *setProxy) IsEmpty() (empty bool, err error) {
	request := proto.SetIsEmptyEncodeRequest(sp.name)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetIsEmptyDecodeResponse)
}

func (sp *setProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := sp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := proto.SetRemoveEncodeRequest(sp.name, itemData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetRemoveDecodeResponse)
}

func (sp *setProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.SetCompareAndRemoveAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetCompareAndRemoveAllDecodeResponse)
}

func (sp *setProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemsData, err := sp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.SetCompareAndRetainAllEncodeRequest(sp.name, itemsData)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToBoolAndError(responseMessage, err, proto.SetCompareAndRetainAllDecodeResponse)
}

func (sp *setProxy) Size() (size int32, err error) {
	request := proto.SetSizeEncodeRequest(sp.name)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToInt32AndError(responseMessage, err, proto.SetSizeDecodeResponse)
}

func (sp *setProxy) RemoveItemListener(registrationID string) (removed bool, err error) {
	return sp.client.ListenerService.deregisterListener(registrationID, func(registrationID string) *proto.ClientMessage {
		return proto.SetRemoveListenerEncodeRequest(sp.name, registrationID)
	})
}

func (sp *setProxy) ToSlice() (items []interface{}, err error) {
	request := proto.SetGetAllEncodeRequest(sp.name)
	responseMessage, err := sp.invoke(request)
	return sp.decodeToInterfaceSliceAndError(responseMessage, err, proto.SetGetAllDecodeResponse)
}

func (sp *setProxy) createEventHandler(listener interface{}) func(clientMessage *proto.ClientMessage) {
	return func(clientMessage *proto.ClientMessage) {
		proto.SetAddListenerHandle(clientMessage, func(itemData serialization.Data, uuid string, eventType int32) {
			onItemEvent := sp.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
