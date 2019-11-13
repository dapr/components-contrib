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
	"fmt"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/colutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	threadID     = 1
	ttlUnlimited = 0
)

type proxy struct {
	client      *HazelcastClient
	serviceName string
	name        string
}

func (p *proxy) Destroy() (bool, error) {
	return p.client.ProxyManager.destroyProxy(p.serviceName, p.name)
}

func (p *proxy) isSmart() bool {
	return p.client.Config.NetworkConfig().IsSmartRouting()
}

func (p *proxy) Name() string {
	return p.name
}

func (p *proxy) PartitionKey() string {
	return p.name
}

func (p *proxy) ServiceName() string {
	return p.serviceName
}

func (p *proxy) validateAndSerialize(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, core.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *proxy) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data serialization.Data,
	arg2Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) {
		return nil, nil, core.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.toData(arg2)
	return
}

func (p *proxy) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data serialization.Data,
	arg2Data serialization.Data, arg3Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) || nilutil.IsNil(arg3) {
		return nil, nil, nil, core.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.toData(arg2)
	if err != nil {
		return
	}
	arg3Data, err = p.toData(arg3)
	return
}

func (p *proxy) validateAndSerializePredicate(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, core.NewHazelcastSerializationError(bufutil.NilPredicateIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *proxy) validateAndSerializeSlice(elements []interface{}) (elementsData []serialization.Data, err error) {
	if elements == nil {
		return nil, core.NewHazelcastSerializationError(bufutil.NilSliceIsNotAllowed, nil)
	}
	elementsData, err = colutil.ObjectToDataCollection(elements, p.client.SerializationService)
	return
}

func (p *proxy) validateItemListener(listener interface{}) (err error) {
	switch listener.(type) {
	case core.ItemAddedListener:
	case core.ItemRemovedListener:
	default:
		return core.NewHazelcastIllegalArgumentError("listener argument type must be one of ItemAddedListener,"+
			" ItemRemovedListener", nil)
	}
	return nil
}

func (p *proxy) validateEntryListener(listener interface{}) (err error) {
	argErr := core.NewHazelcastIllegalArgumentError(fmt.Sprintf("not a supported listener type: %v",
		reflect.TypeOf(listener)), nil)
	if p.serviceName == bufutil.ServiceNameReplicatedMap {
		switch listener.(type) {
		case core.EntryAddedListener:
		case core.EntryRemovedListener:
		case core.EntryUpdatedListener:
		case core.EntryEvictedListener:
		case core.MapClearedListener:
		default:
			err = argErr
		}
	} else if p.serviceName == bufutil.ServiceNameMultiMap {
		switch listener.(type) {
		case core.EntryAddedListener:
		case core.EntryRemovedListener:
		case core.MapClearedListener:
		default:
			err = argErr
		}
	}
	return
}

func (p *proxy) validateAndSerializeMapAndGetPartitions(entries map[interface{}]interface{}) (map[int32][]*proto.Pair, error) {
	if entries == nil {
		return nil, core.NewHazelcastNilPointerError(bufutil.NilMapIsNotAllowed, nil)
	}
	partitions := make(map[int32][]*proto.Pair)
	for key, value := range entries {
		keyData, valueData, err := p.validateAndSerialize2(key, value)
		if err != nil {
			return nil, err
		}
		pair := proto.NewPair(keyData, valueData)
		partitionID := p.client.PartitionService.GetPartitionID(keyData)
		partitions[partitionID] = append(partitions[partitionID], pair)
	}
	return partitions, nil
}

func (p *proxy) invokeOnKey(request *proto.ClientMessage, keyData serialization.Data) (*proto.ClientMessage, error) {
	return p.client.InvocationService.invokeOnKeyOwner(request, keyData).Result()
}

func (p *proxy) invokeOnRandomTarget(request *proto.ClientMessage) (*proto.ClientMessage, error) {
	return p.client.InvocationService.invokeOnRandomTarget(request).Result()
}

func (p *proxy) invokeOnPartition(request *proto.ClientMessage, partitionID int32) (*proto.ClientMessage, error) {
	return p.client.InvocationService.invokeOnPartitionOwner(request, partitionID).Result()
}

func (p *proxy) invokeOnAddress(request *proto.ClientMessage, address *proto.Address) (*proto.ClientMessage, error) {
	return p.client.InvocationService.invokeOnTarget(request, address).Result()
}

func (p *proxy) toObject(data serialization.Data) (interface{}, error) {
	return p.client.SerializationService.ToObject(data)
}

func (p *proxy) toData(object interface{}) (serialization.Data, error) {
	return p.client.SerializationService.ToData(object)
}

func (p *proxy) decodeToObjectAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() serialization.Data) (response interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return p.toObject(decodeFunc(responseMessage)())
}

func (p *proxy) decodeToBoolAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() bool) (response bool, err error) {
	if inputError != nil {
		return false, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *proxy) decodeToInterfaceSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []serialization.Data) (response []interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectCollection(decodeFunc(responseMessage)(), p.client.SerializationService)
}

func (p *proxy) decodeToPairSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []*proto.Pair) (response []core.Pair, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectPairCollection(decodeFunc(responseMessage)(), p.client.SerializationService)
}

func (p *proxy) decodeToInt32AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int32) (response int32, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *proxy) decodeToInt64AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int64) (response int64, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

type partitionSpecificProxy struct {
	*proxy
	partitionID int32
}

func newPartitionSpecificProxy(client *HazelcastClient, serviceName string, name string) *partitionSpecificProxy {
	parSpecProxy := &partitionSpecificProxy{proxy: &proxy{client, serviceName, name}}
	parSpecProxy.partitionID, _ = parSpecProxy.client.PartitionService.GetPartitionIDWithKey(parSpecProxy.PartitionKey())
	return parSpecProxy

}

func (parSpecProxy *partitionSpecificProxy) invoke(request *proto.ClientMessage) (*proto.ClientMessage, error) {
	return parSpecProxy.invokeOnPartition(request, parSpecProxy.partitionID)
}

func (p *proxy) createOnItemEvent(listener interface{}) func(itemData serialization.Data, uuid string, eventType int32) {
	return func(itemData serialization.Data, uuid string, eventType int32) {
		var item interface{}
		item, _ = p.toObject(itemData)
		member := p.client.ClusterService.GetMemberByUUID(uuid)
		itemEvent := proto.NewItemEvent(p.name, item, eventType, member.(*proto.Member))
		if eventType == bufutil.ItemAdded {
			if _, ok := listener.(core.ItemAddedListener); ok {
				listener.(core.ItemAddedListener).ItemAdded(itemEvent)
			}
		} else if eventType == bufutil.ItemRemoved {
			if _, ok := listener.(core.ItemRemovedListener); ok {
				listener.(core.ItemRemovedListener).ItemRemoved(itemEvent)
			}
		}
	}
}
