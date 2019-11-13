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

package colutil

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

func ObjectToDataCollection(objects []interface{}, service spi.SerializationService) ([]serialization.Data, error) {
	if objects == nil {
		return nil, core.NewHazelcastNilPointerError(bufutil.NilSliceIsNotAllowed, nil)
	}
	elementsData := make([]serialization.Data, len(objects))
	for index, element := range objects {
		if element == nil {
			return nil, core.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
		}
		elementData, err := service.ToData(element)
		if err != nil {
			return nil, err
		}
		elementsData[index] = elementData
	}
	return elementsData, nil
}

func DataToObjectCollection(dataSlice []serialization.Data, service spi.SerializationService) ([]interface{}, error) {
	if dataSlice == nil {
		return nil, core.NewHazelcastNilPointerError(bufutil.NilSliceIsNotAllowed, nil)
	}
	elements := make([]interface{}, len(dataSlice))
	for index, data := range dataSlice {
		element, err := service.ToObject(data)
		if err != nil {
			return nil, err
		}
		elements[index] = element
	}
	return elements, nil
}

func DataToObjectPairCollection(dataSlice []*proto.Pair, service spi.SerializationService) (pairSlice []core.Pair, err error) {
	pairSlice = make([]core.Pair, len(dataSlice))
	for index, pairData := range dataSlice {
		key, err := service.ToObject(pairData.Key().(serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := service.ToObject(pairData.Value().(serialization.Data))
		if err != nil {
			return nil, err
		}
		pairSlice[index] = core.Pair(proto.NewPair(key, value))
	}
	return
}
