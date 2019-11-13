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

type multiMapProxy struct {
	*proxy
}

func newMultiMapProxy(client *HazelcastClient, serviceName string, name string) *multiMapProxy {
	return &multiMapProxy{&proxy{client, serviceName, name}}
}

func (mmp *multiMapProxy) Put(key interface{}, value interface{}) (increased bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := proto.MultiMapPutEncodeRequest(mmp.name, keyData, valueData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, proto.MultiMapPutDecodeResponse)
}

func (mmp *multiMapProxy) Get(key interface{}) (values []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := proto.MultiMapGetEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, proto.MultiMapGetDecodeResponse)
}

func (mmp *multiMapProxy) Remove(key interface{}, value interface{}) (removed bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := proto.MultiMapRemoveEntryEncodeRequest(mmp.name, keyData, valueData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, proto.MultiMapRemoveEntryDecodeResponse)

}

func (mmp *multiMapProxy) Delete(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := proto.MultiMapDeleteEncodeRequest(mmp.name, keyData, threadID)
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *multiMapProxy) RemoveAll(key interface{}) (oldValues []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := proto.MultiMapRemoveEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, proto.MultiMapRemoveDecodeResponse)
}

func (mmp *multiMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := proto.MultiMapContainsKeyEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, proto.MultiMapContainsKeyDecodeResponse)
}

func (mmp *multiMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := mmp.validateAndSerialize(value)
	if err != nil {
		return
	}
	request := proto.MultiMapContainsValueEncodeRequest(mmp.name, valueData)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToBoolAndError(responseMessage, err, proto.MultiMapContainsValueDecodeResponse)
}

func (mmp *multiMapProxy) ContainsEntry(key interface{}, value interface{}) (found bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := proto.MultiMapContainsEntryEncodeRequest(mmp.name, keyData, valueData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, proto.MultiMapContainsEntryDecodeResponse)
}

func (mmp *multiMapProxy) Clear() (err error) {
	request := proto.MultiMapClearEncodeRequest(mmp.name)
	_, err = mmp.invokeOnRandomTarget(request)
	return
}

func (mmp *multiMapProxy) Size() (size int32, err error) {
	request := proto.MultiMapSizeEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInt32AndError(responseMessage, err, proto.MultiMapSizeDecodeResponse)
}

func (mmp *multiMapProxy) ValueCount(key interface{}) (valueCount int32, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := proto.MultiMapValueCountEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInt32AndError(responseMessage, err, proto.MultiMapValueCountDecodeResponse)
}

func (mmp *multiMapProxy) Values() (values []interface{}, err error) {
	request := proto.MultiMapValuesEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, proto.MultiMapValuesDecodeResponse)
}

func (mmp *multiMapProxy) KeySet() (keySet []interface{}, err error) {
	request := proto.MultiMapKeySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, proto.MultiMapKeySetDecodeResponse)
}

func (mmp *multiMapProxy) EntrySet() (resultPairs []core.Pair, err error) {
	request := proto.MultiMapEntrySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToPairSliceAndError(responseMessage, err, proto.MultiMapEntrySetDecodeResponse)
}

func (mmp *multiMapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID string, err error) {
	err = mmp.validateEntryListener(listener)
	if err != nil {
		return
	}
	request := proto.MultiMapAddEntryListenerEncodeRequest(mmp.name, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandler(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID string) *proto.ClientMessage {
		return proto.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationID)
	}, func(clientMessage *proto.ClientMessage) string {
		return proto.MultiMapAddEntryListenerDecodeResponse(clientMessage)()
	})
}

func (mmp *multiMapProxy) AddEntryListenerToKey(listener interface{}, key interface{},
	includeValue bool) (registrationID string, err error) {
	err = mmp.validateEntryListener(listener)
	if err != nil {
		return
	}
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return "", err
	}
	request := proto.MultiMapAddEntryListenerToKeyEncodeRequest(mmp.name, keyData, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandlerToKey(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID string) *proto.ClientMessage {
		return proto.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationID)
	}, func(clientMessage *proto.ClientMessage) string {
		return proto.MultiMapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}

func (mmp *multiMapProxy) RemoveEntryListener(registrationID string) (removed bool, err error) {
	return mmp.client.ListenerService.deregisterListener(registrationID, func(registrationID string) *proto.ClientMessage {
		return proto.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationID)
	})
}

func (mmp *multiMapProxy) Lock(key interface{}) (err error) {
	return mmp.LockWithLeaseTime(key, -1)
}

func (mmp *multiMapProxy) LockWithLeaseTime(key interface{}, lease time.Duration) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	leaseInMillis := timeutil.GetTimeInMilliSeconds(lease)
	request := proto.MultiMapLockEncodeRequest(mmp.name, keyData, threadID, leaseInMillis,
		mmp.client.ProxyManager.nextReferenceID())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *multiMapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := proto.MultiMapIsLockedEncodeRequest(mmp.name, keyData)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, proto.MultiMapIsLockedDecodeResponse)
}

func (mmp *multiMapProxy) TryLock(key interface{}) (locked bool, err error) {
	return mmp.TryLockWithTimeout(key, 0)
}

func (mmp *multiMapProxy) TryLockWithTimeout(key interface{}, timeout time.Duration) (locked bool, err error) {
	return mmp.TryLockWithTimeoutAndLease(key, timeout, -1)
}

func (mmp *multiMapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout time.Duration,
	lease time.Duration) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeoutInMillis := timeutil.GetTimeInMilliSeconds(timeout)
	leaseInMillis := timeutil.GetTimeInMilliSeconds(lease)
	request := proto.MultiMapTryLockEncodeRequest(mmp.name, keyData, threadID, leaseInMillis, timeoutInMillis,
		mmp.client.ProxyManager.nextReferenceID())
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, proto.MultiMapTryLockDecodeResponse)
}

func (mmp *multiMapProxy) Unlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := proto.MultiMapUnlockEncodeRequest(mmp.name, keyData, threadID, mmp.client.ProxyManager.nextReferenceID())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *multiMapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := proto.MultiMapForceUnlockEncodeRequest(mmp.name, keyData, mmp.client.ProxyManager.nextReferenceID())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *multiMapProxy) onEntryEvent(keyData serialization.Data, oldValueData serialization.Data,
	valueData serialization.Data, mergingValueData serialization.Data, eventType int32, uuid string,
	numberOfAffectedEntries int32, listener interface{}) {
	member := mmp.client.ClusterService.GetMemberByUUID(uuid)
	key, _ := mmp.toObject(keyData)
	oldValue, _ := mmp.toObject(oldValueData)
	value, _ := mmp.toObject(valueData)
	mergingValue, _ := mmp.toObject(mergingValueData)
	entryEvent := proto.NewEntryEvent(mmp.name, member, eventType, key, oldValue, value, mergingValue)
	mapEvent := proto.NewMapEvent(mmp.name, member, eventType, numberOfAffectedEntries)
	switch eventType {
	case bufutil.EntryEventAdded:
		listener.(core.EntryAddedListener).EntryAdded(entryEvent)
	case bufutil.EntryEventRemoved:
		listener.(core.EntryRemovedListener).EntryRemoved(entryEvent)
	case bufutil.MapEventCleared:
		listener.(core.MapClearedListener).MapCleared(mapEvent)
	}
}

func (mmp *multiMapProxy) createEventHandler(listener interface{}) func(clientMessage *proto.ClientMessage) {
	return func(clientMessage *proto.ClientMessage) {
		proto.MultiMapAddEntryListenerHandle(clientMessage, func(key serialization.Data, oldValue serialization.Data,
			value serialization.Data, mergingValue serialization.Data, eventType int32, uuid string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
		})
	}
}

func (mmp *multiMapProxy) createEventHandlerToKey(listener interface{}) func(clientMessage *proto.ClientMessage) {
	return func(clientMessage *proto.ClientMessage) {
		proto.MultiMapAddEntryListenerToKeyHandle(clientMessage, func(key serialization.Data, oldValue serialization.Data,
			value serialization.Data, mergingValue serialization.Data, eventType int32, uuid string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
		})
	}
}
