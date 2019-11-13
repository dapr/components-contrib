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

type listProxy struct {
	*partitionSpecificProxy
}

func newListProxy(client *HazelcastClient, serviceName string, name string) *listProxy {
	parSpecProxy := newPartitionSpecificProxy(client, serviceName, name)
	return &listProxy{parSpecProxy}
}

func (lp *listProxy) Add(element interface{}) (changed bool, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := proto.ListAddEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListAddDecodeResponse)
}

func (lp *listProxy) AddAt(index int32, element interface{}) (err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return err
	}
	request := proto.ListAddWithIndexEncodeRequest(lp.name, index, elementData)
	_, err = lp.invoke(request)
	return err
}

func (lp *listProxy) AddAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := proto.ListAddAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListAddAllDecodeResponse)

}

func (lp *listProxy) AddAllAt(index int32, elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := proto.ListAddAllWithIndexEncodeRequest(lp.name, index, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListAddAllWithIndexDecodeResponse)
}

func (lp *listProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID string, err error) {
	err = lp.validateItemListener(listener)
	if err != nil {
		return
	}
	request := proto.ListAddListenerEncodeRequest(lp.name, includeValue, false)
	eventHandler := lp.createEventHandler(listener)
	return lp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID string) *proto.ClientMessage {
			return proto.ListRemoveListenerEncodeRequest(lp.name, registrationID)
		}, func(clientMessage *proto.ClientMessage) string {
			return proto.ListAddListenerDecodeResponse(clientMessage)()
		})
}

func (lp *listProxy) Clear() (err error) {
	request := proto.ListClearEncodeRequest(lp.name)
	_, err = lp.invoke(request)
	return err
}

func (lp *listProxy) Contains(element interface{}) (found bool, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := proto.ListContainsEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListContainsDecodeResponse)
}

func (lp *listProxy) ContainsAll(elements []interface{}) (foundAll bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := proto.ListContainsAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListContainsAllDecodeResponse)
}

func (lp *listProxy) Get(index int32) (element interface{}, err error) {
	request := proto.ListGetEncodeRequest(lp.name, index)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToObjectAndError(responseMessage, err, proto.ListGetDecodeResponse)
}

func (lp *listProxy) IndexOf(element interface{}) (index int32, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := proto.ListIndexOfEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInt32AndError(responseMessage, err, proto.ListIndexOfDecodeResponse)
}

func (lp *listProxy) IsEmpty() (empty bool, err error) {
	request := proto.ListIsEmptyEncodeRequest(lp.name)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListIsEmptyDecodeResponse)
}

func (lp *listProxy) LastIndexOf(element interface{}) (index int32, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := proto.ListLastIndexOfEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInt32AndError(responseMessage, err, proto.ListLastIndexOfDecodeResponse)
}

func (lp *listProxy) Remove(element interface{}) (changed bool, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := proto.ListRemoveEncodeRequest(lp.name, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListRemoveDecodeResponse)
}

func (lp *listProxy) RemoveAt(index int32) (previousElement interface{}, err error) {
	request := proto.ListRemoveWithIndexEncodeRequest(lp.name, index)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToObjectAndError(responseMessage, err, proto.ListRemoveWithIndexDecodeResponse)
}

func (lp *listProxy) RemoveAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := proto.ListCompareAndRemoveAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListCompareAndRemoveAllDecodeResponse)
}

func (lp *listProxy) RemoveItemListener(registrationID string) (removed bool, err error) {
	return lp.client.ListenerService.deregisterListener(registrationID, func(registrationID string) *proto.ClientMessage {
		return proto.ListRemoveListenerEncodeRequest(lp.name, registrationID)
	})
}

func (lp *listProxy) RetainAll(elements []interface{}) (changed bool, err error) {
	elementsData, err := lp.validateAndSerializeSlice(elements)
	if err != nil {
		return false, err
	}
	request := proto.ListCompareAndRetainAllEncodeRequest(lp.name, elementsData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToBoolAndError(responseMessage, err, proto.ListCompareAndRetainAllDecodeResponse)
}

func (lp *listProxy) Set(index int32, element interface{}) (previousElement interface{}, err error) {
	elementData, err := lp.validateAndSerialize(element)
	if err != nil {
		return nil, err
	}
	request := proto.ListSetEncodeRequest(lp.name, index, elementData)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToObjectAndError(responseMessage, err, proto.ListSetDecodeResponse)
}

func (lp *listProxy) Size() (size int32, err error) {
	request := proto.ListSizeEncodeRequest(lp.name)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInt32AndError(responseMessage, err, proto.ListSizeDecodeResponse)
}

func (lp *listProxy) SubList(start int32, end int32) (elements []interface{}, err error) {
	request := proto.ListSubEncodeRequest(lp.name, start, end)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInterfaceSliceAndError(responseMessage, err, proto.ListSubDecodeResponse)
}

func (lp *listProxy) ToSlice() (elements []interface{}, err error) {
	request := proto.ListGetAllEncodeRequest(lp.name)
	responseMessage, err := lp.invoke(request)
	return lp.decodeToInterfaceSliceAndError(responseMessage, err, proto.ListGetAllDecodeResponse)
}

func (lp *listProxy) createEventHandler(listener interface{}) func(clientMessage *proto.ClientMessage) {
	return func(clientMessage *proto.ClientMessage) {
		proto.ListAddListenerHandle(clientMessage, func(itemData serialization.Data, uuid string, eventType int32) {
			onItemEvent := lp.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
