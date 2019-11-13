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
	"strconv"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

// Service serializes user objects to Data and back to Object.
// Data is the internal representation of binary Data in Hazelcast.
type Service struct {
	serializationConfig *serialization.Config
	registry            map[int32]serialization.Serializer
	nameToID            map[string]int32
}

func NewService(serializationConfig *serialization.Config) (*Service, error) {
	v1 := Service{serializationConfig: serializationConfig, nameToID: make(map[string]int32),
		registry: make(map[int32]serialization.Serializer)}
	err := v1.registerDefaultSerializers()
	if err != nil {
		return nil, err
	}
	v1.registerCustomSerializers(serializationConfig.CustomSerializers())
	v1.registerGlobalSerializer(serializationConfig.GlobalSerializer())
	return &v1, nil
}

// ToData serializes an object to a Data.
// It can safely be called with a Data. In that case, that instance is returned.
// If it is called with nil, nil is returned.
func (s *Service) ToData(object interface{}) (serialization.Data, error) {
	if _, ok := object.(*Data); ok {
		return object.(*Data), nil
	}
	dataOutput := NewPositionalObjectDataOutput(1, s, s.serializationConfig.IsBigEndian())
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		return nil, err
	}
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.ID())
	err = serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer}, err
}

// ToObject deserializes the given Data to an object.
// It can safely be called on an object that is already deserialized. In that case, that instance
// is returned.
// If this is called with nil, nil is returned.
func (s *Service) ToObject(data serialization.Data) (interface{}, error) {
	if data == nil {
		return nil, nil
	}
	typeID := data.GetType()
	if typeID == 0 {
		return data, nil
	}
	serializer, ok := s.registry[typeID]
	if !ok {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable de-serializer for type %d", typeID), nil)
	}
	dataInput := NewObjectDataInput(data.Buffer(), DataOffset, s, s.serializationConfig.IsBigEndian())
	return serializer.Read(dataInput)
}

func (s *Service) WriteObject(output serialization.DataOutput, object interface{}) error {
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		return err
	}
	output.WriteInt32(serializer.ID())
	return serializer.Write(output, object)
}

func (s *Service) ReadObject(input serialization.DataInput) (interface{}, error) {
	serializerID := input.ReadInt32()
	if input.Error() != nil {
		return nil, input.Error()
	}
	serializer := s.registry[serializerID]
	return serializer.Read(input)
}

func (s *Service) FindSerializerFor(obj interface{}) (serialization.Serializer, error) {
	var serializer serialization.Serializer
	if obj == nil {
		serializer = s.registry[s.nameToID["nil"]]
	}

	if serializer == nil {
		serializer = s.lookUpDefaultSerializer(obj)
	}

	if serializer == nil {
		serializer = s.lookUpCustomSerializer(obj)
	}

	if serializer == nil {
		serializer = s.lookUpGlobalSerializer()
	}

	if serializer == nil {
		serializer = s.registry[s.nameToID["!gob"]]
	}

	if serializer == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable serializer for %v", obj), nil)
	}
	return serializer, nil
}

func (s *Service) registerDefaultSerializers() error {
	s.registerSerializer(&ByteSerializer{})
	s.nameToID["uint8"] = ConstantTypeByte

	s.registerSerializer(&BoolSerializer{})
	s.nameToID["bool"] = ConstantTypeBool

	s.registerSerializer(&UInteger16Serializer{})
	s.nameToID["uint16"] = ConstantTypeUInteger16

	s.registerSerializer(&Integer16Serializer{})
	s.nameToID["int16"] = ConstantTypeInteger16

	s.registerSerializer(&Integer32Serializer{})
	s.nameToID["int32"] = ConstantTypeInteger32

	s.registerSerializer(&Integer64Serializer{})
	s.nameToID["int64"] = ConstantTypeInteger64

	s.registerSerializer(&Float32Serializer{})
	s.nameToID["float32"] = ConstantTypeFloat32

	s.registerSerializer(&Float64Serializer{})
	s.nameToID["float64"] = ConstantTypeFloat64

	s.registerSerializer(&StringSerializer{})
	s.nameToID["string"] = ConstantTypeString

	s.registerSerializer(&NilSerializer{})
	s.nameToID["nil"] = ConstantTypeNil

	s.registerSerializer(&ByteArraySerializer{})
	s.nameToID["[]uint8"] = ConstantTypeByteArray

	s.registerSerializer(&BoolArraySerializer{})
	s.nameToID["[]bool"] = ConstantTypeBoolArray

	s.registerSerializer(&UInteger16ArraySerializer{})
	s.nameToID["[]uint16"] = ConstantTypeUInteger16Array

	s.registerSerializer(&Integer16ArraySerializer{})
	s.nameToID["[]int16"] = ConstantTypeInteger16Array

	s.registerSerializer(&Integer32ArraySerializer{})
	s.nameToID["[]int32"] = ConstantTypeInteger32Array

	s.registerSerializer(&Integer64ArraySerializer{})
	s.nameToID["[]int64"] = ConstantTypeInteger64Array

	s.registerSerializer(&Float32ArraySerializer{})
	s.nameToID["[]float32"] = ConstantTypeFloat32Array

	s.registerSerializer(&Float64ArraySerializer{})
	s.nameToID["[]float64"] = ConstantTypeFloat64Array

	s.registerSerializer(&StringArraySerializer{})
	s.nameToID["[]string"] = ConstantTypeStringArray

	s.registerSerializer(&HazelcastJSONSerializer{})
	s.nameToID[reflect.TypeOf(&core.HazelcastJSONValue{}).String()] = JSONSerializationType

	s.registerSerializer(&GobSerializer{})
	s.nameToID["!gob"] = GoGobSerializationType

	err := s.registerIdentifiedFactories()
	if err != nil {
		return err
	}

	portableSerializer := NewPortableSerializer(s, s.serializationConfig.PortableFactories(),
		s.serializationConfig.PortableVersion())

	s.registerClassDefinitions(portableSerializer, s.serializationConfig.ClassDefinitions())
	s.registerSerializer(portableSerializer)
	s.nameToID["!portable"] = ConstantTypePortable
	return nil

}

func (s *Service) registerCustomSerializers(customSerializers map[reflect.Type]serialization.Serializer) {
	for _, customSerializer := range customSerializers {
		s.registerSerializer(customSerializer)
	}
}

func (s *Service) registerSerializer(serializer serialization.Serializer) error {
	if s.registry[serializer.ID()] != nil {
		return core.NewHazelcastSerializationError("this serializer is already in the registry", nil)
	}
	s.registry[serializer.ID()] = serializer
	return nil
}

func (s *Service) registerClassDefinitions(portableSerializer *PortableSerializer,
	classDefinitions []serialization.ClassDefinition) {
	for _, cd := range classDefinitions {
		portableSerializer.portableContext.RegisterClassDefinition(cd)
	}
}

func (s *Service) registerGlobalSerializer(globalSerializer serialization.Serializer) {
	if globalSerializer != nil {
		s.registerSerializer(globalSerializer)
	}
}

func (s *Service) getIDByObject(obj interface{}) (int32, bool) {
	typ := reflect.TypeOf(obj).String()
	if typ == "int" || typ == "[]int" {
		typ = typ + strconv.Itoa(64)
	}
	if val, ok := s.nameToID[typ]; ok {
		return val, true
	}
	return 0, false
}

func (s *Service) lookUpDefaultSerializer(obj interface{}) serialization.Serializer {
	var serializer serialization.Serializer
	if isIdentifiedDataSerializable(obj) {
		return s.registry[s.nameToID["identified"]]
	}
	if isPortableSerializable(obj) {
		return s.registry[s.nameToID["!portable"]]
	}
	id, found := s.getIDByObject(obj)
	if !found {
		return nil
	}
	serializer = s.registry[id]
	return serializer
}

func (s *Service) lookUpCustomSerializer(obj interface{}) serialization.Serializer {
	for key, val := range s.serializationConfig.CustomSerializers() {
		if key.Kind() == reflect.Interface {
			if reflect.TypeOf(obj).Implements(key) {
				return val
			}
		} else {
			if reflect.TypeOf(obj) == key {
				return val
			}
		}
	}
	return nil
}

func (s *Service) lookUpGlobalSerializer() serialization.Serializer {
	return s.serializationConfig.GlobalSerializer()
}

func (s *Service) registerIdentifiedFactories() error {
	factories := make(map[int32]serialization.IdentifiedDataSerializableFactory)
	for id := range s.serializationConfig.DataSerializableFactories() {
		factories[id] = s.serializationConfig.DataSerializableFactories()[id]
	}

	err := s.registerSerializer(NewIdentifiedDataSerializableSerializer(factories))
	if err != nil {
		return err
	}
	s.nameToID["identified"] = ConstantTypeDataSerializable
	return nil
}

func isIdentifiedDataSerializable(obj interface{}) bool {
	_, ok := obj.(serialization.IdentifiedDataSerializable)
	return ok
}

func isPortableSerializable(obj interface{}) bool {
	_, ok := obj.(serialization.Portable)
	return ok
}
