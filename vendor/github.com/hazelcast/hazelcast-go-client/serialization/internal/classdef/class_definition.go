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

package classdef

import "github.com/hazelcast/hazelcast-go-client/serialization"

type ClassDefinitionImpl struct {
	factoryID int32
	classID   int32
	version   int32
	fields    map[string]serialization.FieldDefinition
}

func NewClassDefinitionImpl(factoryID int32, classID int32, version int32) *ClassDefinitionImpl {
	return &ClassDefinitionImpl{factoryID, classID, version, make(map[string]serialization.FieldDefinition)}
}

func (cd *ClassDefinitionImpl) FactoryID() int32 {
	return cd.factoryID
}

func (cd *ClassDefinitionImpl) ClassID() int32 {
	return cd.classID
}

func (cd *ClassDefinitionImpl) Version() int32 {
	return cd.version
}

func (cd *ClassDefinitionImpl) Field(name string) serialization.FieldDefinition {
	return cd.fields[name]
}

func (cd *ClassDefinitionImpl) FieldCount() int {
	return len(cd.fields)
}

func (cd *ClassDefinitionImpl) AddFieldDefinition(definition serialization.FieldDefinition) {
	cd.fields[definition.Name()] = definition
}

type FieldDefinitionImpl struct {
	index     int32
	fieldName string
	fieldType int32
	factoryID int32
	classID   int32
	version   int32
}

func NewFieldDefinitionImpl(index int32, fieldName string, fieldType int32, factoryID int32,
	classID int32, version int32) serialization.FieldDefinition {
	return &FieldDefinitionImpl{index, fieldName, fieldType, factoryID, classID, version}
}

func (fd *FieldDefinitionImpl) Type() int32 {
	return fd.fieldType
}

func (fd *FieldDefinitionImpl) Name() string {
	return fd.fieldName
}

func (fd *FieldDefinitionImpl) Index() int32 {
	return fd.index
}

func (fd *FieldDefinitionImpl) ClassID() int32 {
	return fd.classID
}

func (fd *FieldDefinitionImpl) FactoryID() int32 {
	return fd.factoryID
}

func (fd *FieldDefinitionImpl) Version() int32 {
	return fd.version
}

const (
	TypePortable = iota
	TypeByte
	TypeBool
	TypeUint16
	TypeInt16
	TypeInt32
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeUTF
	TypePortableArray
	TypeByteArray
	TypeBoolArray
	TypeUint16Array
	TypeInt16Array
	TypeInt32Array
	TypeInt64Array
	TypeFloat32Array
	TypeFloat64Array
	TypeUTFArray
)
