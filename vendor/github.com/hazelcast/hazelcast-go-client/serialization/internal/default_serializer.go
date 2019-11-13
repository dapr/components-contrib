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
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type NilSerializer struct{}

func (*NilSerializer) ID() int32 {
	return ConstantTypeNil
}

func (*NilSerializer) Read(input serialization.DataInput) (interface{}, error) {
	return nil, nil
}

func (*NilSerializer) Write(output serialization.DataOutput, i interface{}) error {
	// Empty method
	return nil
}

type IdentifiedDataSerializableSerializer struct {
	factories map[int32]serialization.IdentifiedDataSerializableFactory
}

func NewIdentifiedDataSerializableSerializer(
	factories map[int32]serialization.IdentifiedDataSerializableFactory) *IdentifiedDataSerializableSerializer {
	return &IdentifiedDataSerializableSerializer{factories: factories}
}

func (*IdentifiedDataSerializableSerializer) ID() int32 {
	return ConstantTypeDataSerializable
}

func (idss *IdentifiedDataSerializableSerializer) Read(input serialization.DataInput) (interface{}, error) {
	isIdentified := input.ReadBool()
	if input.Error() != nil {
		return nil, input.Error()
	}
	if !isIdentified {
		return nil, core.NewHazelcastSerializationError("native clients do not support DataSerializable,"+
			" please use IdentifiedDataSerializable", nil)
	}
	factoryID := input.ReadInt32()
	classID := input.ReadInt32()
	if input.Error() != nil {
		return nil, input.Error()
	}
	factory := idss.factories[factoryID]
	if factory == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("there is no IdentifiedDataSerializable factory with ID: %d",
			factoryID), nil)
	}
	var object = factory.Create(classID)
	if object == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("%v is not able to create an instance for ID: %v on factory ID: %v",
			reflect.TypeOf(factory), classID, factoryID), nil)
	}
	err := object.ReadData(input)
	return object, err
}

func (*IdentifiedDataSerializableSerializer) Write(output serialization.DataOutput, i interface{}) error {
	r := i.(serialization.IdentifiedDataSerializable)
	output.WriteBool(true)
	output.WriteInt32(r.FactoryID())
	output.WriteInt32(r.ClassID())
	return r.WriteData(output)
}

type ByteSerializer struct{}

func (*ByteSerializer) ID() int32 {
	return ConstantTypeByte
}

func (*ByteSerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadByte()
	return res, input.Error()
}

func (*ByteSerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteByte(i.(byte))
	return nil
}

type BoolSerializer struct{}

func (*BoolSerializer) ID() int32 {
	return ConstantTypeBool
}

func (*BoolSerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadBool()
	return res, input.Error()
}

func (*BoolSerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteBool(i.(bool))
	return nil
}

type UInteger16Serializer struct{}

func (*UInteger16Serializer) ID() int32 {
	return ConstantTypeUInteger16
}

func (*UInteger16Serializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadUInt16()
	return res, input.Error()
}

func (*UInteger16Serializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteUInt16(i.(uint16))
	return nil
}

type Integer16Serializer struct{}

func (*Integer16Serializer) ID() int32 {
	return ConstantTypeInteger16
}

func (*Integer16Serializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadInt16()
	return res, input.Error()
}

func (*Integer16Serializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteInt16(i.(int16))
	return nil
}

type Integer32Serializer struct{}

func (*Integer32Serializer) ID() int32 {
	return ConstantTypeInteger32
}

func (*Integer32Serializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadInt32()
	return res, input.Error()
}

func (*Integer32Serializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteInt32(i.(int32))
	return nil
}

type Integer64Serializer struct{}

func (*Integer64Serializer) ID() int32 {
	return ConstantTypeInteger64
}

func (*Integer64Serializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadInt64()
	return res, input.Error()
}

func (*Integer64Serializer) Write(output serialization.DataOutput, i interface{}) error {
	val, ok := i.(int64)
	if !ok {
		val = int64(i.(int))
	}
	output.WriteInt64(val)
	return nil
}

type Float32Serializer struct{}

func (*Float32Serializer) ID() int32 {
	return ConstantTypeFloat32
}

func (*Float32Serializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadFloat32()
	return res, input.Error()
}

func (*Float32Serializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteFloat32(i.(float32))
	return nil
}

type Float64Serializer struct{}

func (*Float64Serializer) ID() int32 {
	return ConstantTypeFloat64
}

func (*Float64Serializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadFloat64()
	return res, input.Error()
}

func (*Float64Serializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteFloat64(i.(float64))
	return nil
}

type StringSerializer struct{}

func (*StringSerializer) ID() int32 {
	return ConstantTypeString
}

func (*StringSerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadUTF()
	return res, input.Error()
}

func (*StringSerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteUTF(i.(string))
	return nil
}

type ByteArraySerializer struct{}

func (*ByteArraySerializer) ID() int32 {
	return ConstantTypeByteArray
}

func (*ByteArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadByteArray()
	return res, input.Error()
}

func (*ByteArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteByteArray(i.([]byte))
	return nil
}

type BoolArraySerializer struct{}

func (*BoolArraySerializer) ID() int32 {
	return ConstantTypeBoolArray
}

func (*BoolArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadBoolArray()
	return res, input.Error()
}

func (*BoolArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteBoolArray(i.([]bool))
	return nil
}

type UInteger16ArraySerializer struct{}

func (*UInteger16ArraySerializer) ID() int32 {
	return ConstantTypeUInteger16Array
}

func (*UInteger16ArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadUInt16Array()
	return res, input.Error()

}

func (*UInteger16ArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteUInt16Array(i.([]uint16))
	return nil
}

type Integer16ArraySerializer struct{}

func (*Integer16ArraySerializer) ID() int32 {
	return ConstantTypeInteger16Array
}

func (*Integer16ArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadInt16Array()
	return res, input.Error()
}

func (*Integer16ArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteInt16Array(i.([]int16))
	return nil
}

type Integer32ArraySerializer struct{}

func (*Integer32ArraySerializer) ID() int32 {
	return ConstantTypeInteger32Array
}

func (*Integer32ArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadInt32Array()
	return res, input.Error()
}

func (*Integer32ArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteInt32Array(i.([]int32))
	return nil
}

type Integer64ArraySerializer struct{}

func (*Integer64ArraySerializer) ID() int32 {
	return ConstantTypeInteger64Array
}

func (*Integer64ArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadInt64Array()
	return res, input.Error()
}

func (*Integer64ArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	val, ok := i.([]int64)
	if !ok {
		tmp := i.([]int)
		length := len(tmp)
		val = make([]int64, length)
		for k := 0; k < length; k++ {
			val[k] = int64(tmp[k])
		}
	}
	output.WriteInt64Array(val)
	return nil
}

type Float32ArraySerializer struct{}

func (*Float32ArraySerializer) ID() int32 {
	return ConstantTypeFloat32Array
}

func (*Float32ArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadFloat32Array()
	return res, input.Error()
}

func (*Float32ArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteFloat32Array(i.([]float32))
	return nil
}

type Float64ArraySerializer struct{}

func (*Float64ArraySerializer) ID() int32 {
	return ConstantTypeFloat64Array
}

func (*Float64ArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadFloat64Array()
	return res, input.Error()
}

func (*Float64ArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteFloat64Array(i.([]float64))
	return nil
}

type StringArraySerializer struct{}

func (*StringArraySerializer) ID() int32 {
	return ConstantTypeStringArray
}

func (*StringArraySerializer) Read(input serialization.DataInput) (interface{}, error) {
	res := input.ReadUTFArray()
	return res, input.Error()
}

func (*StringArraySerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteUTFArray(i.([]string))
	return nil
}

type GobSerializer struct{}

func (*GobSerializer) ID() int32 {
	return GoGobSerializationType
}

func (*GobSerializer) Read(input serialization.DataInput) (interface{}, error) {
	var network bytes.Buffer
	data := input.ReadData()
	if input.Error() != nil {
		return nil, input.Error()
	}
	network.Write(data.Buffer())
	dec := gob.NewDecoder(&network)
	var result interface{}
	err := dec.Decode(&result)
	return result, err
}

func (*GobSerializer) Write(output serialization.DataOutput, i interface{}) error {
	var network bytes.Buffer
	t := reflect.TypeOf(i)
	v := reflect.New(t).Elem().Interface()
	gob.Register(v)
	enc := gob.NewEncoder(&network)
	err := enc.Encode(&i)
	if err != nil {
		return err
	}
	output.WriteData(&Data{network.Bytes()})
	return nil
}

type HazelcastJSONSerializer struct{}

func (*HazelcastJSONSerializer) ID() (id int32) {
	return JSONSerializationType
}

func (*HazelcastJSONSerializer) Read(input serialization.DataInput) (object interface{}, err error) {
	obj := input.ReadUTF()
	return core.CreateHazelcastJSONValueFromString(obj), input.Error()
}

func (*HazelcastJSONSerializer) Write(output serialization.DataOutput, object interface{}) (err error) {
	value := object.(*core.HazelcastJSONValue)
	output.WriteUTF(value.ToString())
	return nil
}
