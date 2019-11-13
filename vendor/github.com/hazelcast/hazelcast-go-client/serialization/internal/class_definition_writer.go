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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/classdef"
)

type ClassDefinitionWriter struct {
	portableContext        *PortableContext
	classDefinitionBuilder *classdef.ClassDefinitionBuilder
}

func NewClassDefinitionWriter(portableContext *PortableContext, factoryID int32, classID int32,
	version int32) *ClassDefinitionWriter {
	return &ClassDefinitionWriter{portableContext,
		classdef.NewClassDefinitionBuilder(factoryID, classID, version)}
}

func (cdw *ClassDefinitionWriter) WriteByte(fieldName string, value byte) {
	cdw.classDefinitionBuilder.AddByteField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteBool(fieldName string, value bool) {
	cdw.classDefinitionBuilder.AddBoolField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteUInt16(fieldName string, value uint16) {
	cdw.classDefinitionBuilder.AddUInt16Field(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteInt16(fieldName string, value int16) {
	cdw.classDefinitionBuilder.AddInt16Field(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteInt32(fieldName string, value int32) {
	cdw.classDefinitionBuilder.AddInt32Field(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteInt64(fieldName string, value int64) {
	cdw.classDefinitionBuilder.AddInt64Field(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteFloat32(fieldName string, value float32) {
	cdw.classDefinitionBuilder.AddFloat32Field(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteFloat64(fieldName string, value float64) {
	cdw.classDefinitionBuilder.AddFloat64Field(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteUTF(fieldName string, value string) {
	cdw.classDefinitionBuilder.AddUTFField(fieldName)
}

func (cdw *ClassDefinitionWriter) WritePortable(fieldName string, portable serialization.Portable) error {
	if portable == nil {
		return core.NewHazelcastSerializationError("cannot write nil portable without explicitly registering class definition", nil)
	}
	nestedCD, err := cdw.portableContext.LookUpOrRegisterClassDefiniton(portable)
	if err != nil {
		return err
	}
	cdw.classDefinitionBuilder.AddPortableField(fieldName, nestedCD)
	return nil
}

func (cdw *ClassDefinitionWriter) WriteNilPortable(fieldName string, factoryID int32, classID int32) error {
	var version int32
	nestedCD := cdw.portableContext.LookUpClassDefinition(factoryID, classID, version)
	if nestedCD == nil {
		return core.NewHazelcastSerializationError("cannot write nil portable without explicitly registering class definition", nil)
	}
	cdw.classDefinitionBuilder.AddPortableField(fieldName, nestedCD)
	return nil
}

func (cdw *ClassDefinitionWriter) WriteByteArray(fieldName string, value []byte) {
	cdw.classDefinitionBuilder.AddByteArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteBoolArray(fieldName string, value []bool) {
	cdw.classDefinitionBuilder.AddBoolArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteUInt16Array(fieldName string, value []uint16) {
	cdw.classDefinitionBuilder.AddUInt16ArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteInt16Array(fieldName string, value []int16) {
	cdw.classDefinitionBuilder.AddInt16ArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteInt32Array(fieldName string, value []int32) {
	cdw.classDefinitionBuilder.AddInt32ArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteInt64Array(fieldName string, value []int64) {
	cdw.classDefinitionBuilder.AddInt64ArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteFloat32Array(fieldName string, value []float32) {
	cdw.classDefinitionBuilder.AddFloat32ArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteFloat64Array(fieldName string, value []float64) {
	cdw.classDefinitionBuilder.AddFloat64ArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WriteUTFArray(fieldName string, value []string) {
	cdw.classDefinitionBuilder.AddUTFArrayField(fieldName)
}

func (cdw *ClassDefinitionWriter) WritePortableArray(fieldName string, portables []serialization.Portable) error {
	if portables == nil {
		return core.NewHazelcastSerializationError("non nil value expected", nil)
	}
	if len(portables) == 0 || portables == nil {
		return core.NewHazelcastSerializationError("cannot write empty array", nil)
	}
	var sample = portables[0]
	var nestedCD, err = cdw.portableContext.LookUpOrRegisterClassDefiniton(sample)
	if err != nil {
		return nil
	}

	cdw.classDefinitionBuilder.AddPortableArrayField(fieldName, nestedCD)
	return nil
}

func (cdw *ClassDefinitionWriter) registerAndGet() (serialization.ClassDefinition, error) {
	cd := cdw.classDefinitionBuilder.Build()
	return cdw.portableContext.RegisterClassDefinition(cd)
}
