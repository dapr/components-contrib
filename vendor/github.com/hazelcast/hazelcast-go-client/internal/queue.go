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
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/timeutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type queueProxy struct {
	*partitionSpecificProxy
}

func newQueueProxy(client *HazelcastClient, serviceName string, name string) *queueProxy {
	parSpecProxy := newPartitionSpecificProxy(client, serviceName, name)
	return &queueProxy{parSpecProxy}
}

func (qp *queueProxy) AddAll(items []interface{}) (changed bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.QueueAddAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueAddAllDecodeResponse)
}

func (qp *queueProxy) AddItemListener(listener interface{}, includeValue bool) (registrationID string, err error) {
	err = qp.validateItemListener(listener)
	if err != nil {
		return
	}
	request := proto.QueueAddListenerEncodeRequest(qp.name, includeValue, false)
	eventHandler := qp.createEventHandler(listener)
	return qp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID string) *proto.ClientMessage {
			return proto.QueueRemoveListenerEncodeRequest(qp.name, registrationID)
		}, func(clientMessage *proto.ClientMessage) string {
			return proto.QueueAddListenerDecodeResponse(clientMessage)()
		})
}

func (qp *queueProxy) Clear() (err error) {
	request := proto.QueueClearEncodeRequest(qp.name)
	_, err = qp.invoke(request)
	return err
}

func (qp *queueProxy) Contains(item interface{}) (found bool, err error) {
	elementData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := proto.QueueContainsEncodeRequest(qp.name, elementData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueContainsDecodeResponse)
}

func (qp *queueProxy) ContainsAll(items []interface{}) (foundAll bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.QueueContainsAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueContainsAllDecodeResponse)
}

func (qp *queueProxy) DrainTo(slice *[]interface{}) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(bufutil.NilSliceIsNotAllowed, nil)
	}
	request := proto.QueueDrainToEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	resultSlice, err := qp.decodeToInterfaceSliceAndError(responseMessage, err, proto.QueueDrainToDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (qp *queueProxy) DrainToWithMaxSize(slice *[]interface{}, maxElements int32) (movedAmount int32, err error) {
	if slice == nil {
		return 0, core.NewHazelcastNilPointerError(bufutil.NilSliceIsNotAllowed, nil)
	}
	request := proto.QueueDrainToMaxSizeEncodeRequest(qp.name, maxElements)
	responseMessage, err := qp.invoke(request)
	resultSlice, err := qp.decodeToInterfaceSliceAndError(responseMessage, err, proto.QueueDrainToMaxSizeDecodeResponse)
	if err != nil {
		return 0, err
	}
	*slice = append(*slice, resultSlice...)
	return int32(len(resultSlice)), nil
}

func (qp *queueProxy) IsEmpty() (empty bool, err error) {
	request := proto.QueueIsEmptyEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueIsEmptyDecodeResponse)
}

func (qp *queueProxy) Offer(item interface{}) (added bool, err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := proto.QueueOfferEncodeRequest(qp.name, itemData, 0)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueOfferDecodeResponse)
}

func (qp *queueProxy) OfferWithTimeout(item interface{}, timeout time.Duration) (added bool, err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	timeoutInMilliSeconds := timeutil.GetTimeInMilliSeconds(timeout)
	request := proto.QueueOfferEncodeRequest(qp.name, itemData, timeoutInMilliSeconds)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueOfferDecodeResponse)
}

func (qp *queueProxy) Peek() (item interface{}, err error) {
	request := proto.QueuePeekEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, proto.QueuePeekDecodeResponse)
}

func (qp *queueProxy) Poll() (item interface{}, err error) {
	request := proto.QueuePollEncodeRequest(qp.name, 0)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, proto.QueuePollDecodeResponse)
}

func (qp *queueProxy) PollWithTimeout(timeout time.Duration) (item interface{}, err error) {
	timeoutInMilliSeconds := timeutil.GetTimeInMilliSeconds(timeout)
	request := proto.QueuePollEncodeRequest(qp.name, timeoutInMilliSeconds)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, proto.QueuePollDecodeResponse)
}

func (qp *queueProxy) Put(item interface{}) (err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return err
	}
	request := proto.QueuePutEncodeRequest(qp.name, itemData)
	_, err = qp.invoke(request)
	return err
}

func (qp *queueProxy) RemainingCapacity() (remainingCapacity int32, err error) {
	request := proto.QueueRemainingCapacityEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToInt32AndError(responseMessage, err, proto.QueueRemainingCapacityDecodeResponse)
}

func (qp *queueProxy) Remove(item interface{}) (removed bool, err error) {
	itemData, err := qp.validateAndSerialize(item)
	if err != nil {
		return false, err
	}
	request := proto.QueueRemoveEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueRemoveDecodeResponse)
}

func (qp *queueProxy) RemoveAll(items []interface{}) (changed bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.QueueCompareAndRemoveAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueCompareAndRemoveAllDecodeResponse)
}

func (qp *queueProxy) RemoveItemListener(registrationID string) (removed bool, err error) {
	return qp.client.ListenerService.deregisterListener(registrationID, func(registrationID string) *proto.ClientMessage {
		return proto.QueueRemoveListenerEncodeRequest(qp.name, registrationID)
	})
}

func (qp *queueProxy) RetainAll(items []interface{}) (changed bool, err error) {
	itemData, err := qp.validateAndSerializeSlice(items)
	if err != nil {
		return false, err
	}
	request := proto.QueueCompareAndRetainAllEncodeRequest(qp.name, itemData)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToBoolAndError(responseMessage, err, proto.QueueCompareAndRetainAllDecodeResponse)
}

func (qp *queueProxy) Size() (size int32, err error) {
	request := proto.QueueSizeEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToInt32AndError(responseMessage, err, proto.QueueSizeDecodeResponse)
}

func (qp *queueProxy) Take() (item interface{}, err error) {
	request := proto.QueueTakeEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToObjectAndError(responseMessage, err, proto.QueueTakeDecodeResponse)
}

func (qp *queueProxy) ToSlice() (items []interface{}, err error) {
	request := proto.QueueIteratorEncodeRequest(qp.name)
	responseMessage, err := qp.invoke(request)
	return qp.decodeToInterfaceSliceAndError(responseMessage, err, proto.QueueIteratorDecodeResponse)
}

func (qp *queueProxy) createEventHandler(listener interface{}) func(clientMessage *proto.ClientMessage) {
	return func(clientMessage *proto.ClientMessage) {
		proto.QueueAddListenerHandle(clientMessage, func(itemData serialization.Data, uuid string, eventType int32) {
			onItemEvent := qp.createOnItemEvent(listener)
			onItemEvent(itemData, uuid, eventType)
		})
	}
}
