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
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/internal/classdef"
)

type DefaultPortableWriter struct {
	serializer      *PortableSerializer
	output          serialization.PositionalDataOutput
	classDefinition serialization.ClassDefinition
	begin           int32
	offset          int32
}

func NewDefaultPortableWriter(serializer *PortableSerializer, output serialization.PositionalDataOutput,
	classDefinition serialization.ClassDefinition) *DefaultPortableWriter {
	begin := output.Position()
	output.WriteZeroBytes(4)
	output.WriteInt32(int32(classDefinition.FieldCount()))
	offset := output.Position()
	fieldIndexesLength := (classDefinition.FieldCount() + 1) * bufutil.Int32SizeInBytes
	output.WriteZeroBytes(fieldIndexesLength)
	return &DefaultPortableWriter{serializer, output, classDefinition, begin, offset}
}

func (pw *DefaultPortableWriter) WriteByte(fieldName string, value byte) {
	pw.setPosition(fieldName, classdef.TypeByte)
	pw.output.WriteByte(value)
}

func (pw *DefaultPortableWriter) WriteBool(fieldName string, value bool) {
	pw.setPosition(fieldName, classdef.TypeBool)
	pw.output.WriteBool(value)
}

func (pw *DefaultPortableWriter) WriteUInt16(fieldName string, value uint16) {
	pw.setPosition(fieldName, classdef.TypeUint16)
	pw.output.WriteUInt16(value)
}

func (pw *DefaultPortableWriter) WriteInt16(fieldName string, value int16) {
	pw.setPosition(fieldName, classdef.TypeInt16)
	pw.output.WriteInt16(value)
}

func (pw *DefaultPortableWriter) WriteInt32(fieldName string, value int32) {
	pw.setPosition(fieldName, classdef.TypeInt32)
	pw.output.WriteInt32(value)
}

func (pw *DefaultPortableWriter) WriteInt64(fieldName string, value int64) {
	pw.setPosition(fieldName, classdef.TypeInt64)
	pw.output.WriteInt64(value)
}

func (pw *DefaultPortableWriter) WriteFloat32(fieldName string, value float32) {
	pw.setPosition(fieldName, classdef.TypeFloat32)
	pw.output.WriteFloat32(value)
}

func (pw *DefaultPortableWriter) WriteFloat64(fieldName string, value float64) {
	pw.setPosition(fieldName, classdef.TypeFloat64)
	pw.output.WriteFloat64(value)
}

func (pw *DefaultPortableWriter) WriteUTF(fieldName string, value string) {
	pw.setPosition(fieldName, classdef.TypeUTF)
	pw.output.WriteUTF(value)
}

func (pw *DefaultPortableWriter) WritePortable(fieldName string, portable serialization.Portable) error {
	fieldDefinition := pw.setPosition(fieldName, classdef.TypePortable)
	isNullPortable := portable == nil
	pw.output.WriteBool(isNullPortable)
	pw.output.WriteInt32(fieldDefinition.FactoryID())
	pw.output.WriteInt32(fieldDefinition.ClassID())
	if !isNullPortable {
		err := pw.serializer.WriteObject(pw.output, portable)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pw *DefaultPortableWriter) WriteNilPortable(fieldName string, factoryID int32, classID int32) error {
	pw.setPosition(fieldName, classdef.TypePortable)
	pw.output.WriteBool(true)
	pw.output.WriteInt32(factoryID)
	pw.output.WriteInt32(classID)
	return nil
}

func (pw *DefaultPortableWriter) WriteByteArray(fieldName string, array []byte) {
	pw.setPosition(fieldName, classdef.TypeByteArray)
	pw.output.WriteByteArray(array)
}

func (pw *DefaultPortableWriter) WriteBoolArray(fieldName string, array []bool) {
	pw.setPosition(fieldName, classdef.TypeBoolArray)
	pw.output.WriteBoolArray(array)
}

func (pw *DefaultPortableWriter) WriteUInt16Array(fieldName string, array []uint16) {
	pw.setPosition(fieldName, classdef.TypeUint16Array)
	pw.output.WriteUInt16Array(array)
}

func (pw *DefaultPortableWriter) WriteInt16Array(fieldName string, array []int16) {
	pw.setPosition(fieldName, classdef.TypeInt16Array)
	pw.output.WriteInt16Array(array)
}

func (pw *DefaultPortableWriter) WriteInt32Array(fieldName string, array []int32) {
	pw.setPosition(fieldName, classdef.TypeInt32Array)
	pw.output.WriteInt32Array(array)
}

func (pw *DefaultPortableWriter) WriteInt64Array(fieldName string, array []int64) {
	pw.setPosition(fieldName, classdef.TypeInt64Array)
	pw.output.WriteInt64Array(array)
}

func (pw *DefaultPortableWriter) WriteFloat32Array(fieldName string, array []float32) {
	pw.setPosition(fieldName, classdef.TypeFloat32Array)
	pw.output.WriteFloat32Array(array)
}

func (pw *DefaultPortableWriter) WriteFloat64Array(fieldName string, array []float64) {
	pw.setPosition(fieldName, classdef.TypeFloat64Array)
	pw.output.WriteFloat64Array(array)
}

func (pw *DefaultPortableWriter) WriteUTFArray(fieldName string, array []string) {
	pw.setPosition(fieldName, classdef.TypeUTFArray)
	pw.output.WriteUTFArray(array)
}

func (pw *DefaultPortableWriter) WritePortableArray(fieldName string, portableArray []serialization.Portable) error {
	var innerOffset int32
	var sample serialization.Portable
	fieldDefinition := pw.setPosition(fieldName, classdef.TypePortableArray)
	var length int32
	if portableArray != nil {
		length = int32(len(portableArray))
	} else {
		length = bufutil.NilArrayLength
	}
	pw.output.WriteInt32(length)
	pw.output.WriteInt32(fieldDefinition.FactoryID())
	pw.output.WriteInt32(fieldDefinition.ClassID())

	if length > 0 {
		innerOffset = pw.output.Position()
		pw.output.WriteZeroBytes(int(length) * 4)
		for i := int32(0); i < length; i++ {
			sample = portableArray[i]
			posVal := pw.output.Position()
			pw.output.PWriteInt32(innerOffset+i*bufutil.Int32SizeInBytes, posVal)
			err := pw.serializer.WriteObject(pw.output, sample)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (pw *DefaultPortableWriter) setPosition(fieldName string, fieldType int32) serialization.FieldDefinition {
	field := pw.classDefinition.Field(fieldName)
	pos := pw.output.Position()
	index := field.Index()
	pw.output.PWriteInt32(pw.offset+index*bufutil.Int32SizeInBytes, pos)
	pw.output.WriteInt16(int16(len(fieldName)))
	pw.output.WriteBytes(fieldName)
	pw.output.WriteByte(byte(fieldType))
	return field
}

func (pw *DefaultPortableWriter) End() {
	position := pw.output.Position()
	pw.output.PWriteInt32(pw.begin, position)
}
