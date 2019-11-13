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

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/internal/classdef"
)

type MorphingPortableReader struct {
	*DefaultPortableReader
}

func NewMorphingPortableReader(portableSerializer *PortableSerializer, input serialization.DataInput,
	classDefinition serialization.ClassDefinition) *MorphingPortableReader {
	return &MorphingPortableReader{NewDefaultPortableReader(portableSerializer, input, classDefinition)}
}

func (mpr *MorphingPortableReader) ReadByte(fieldName string) byte {
	if mpr.err != nil {
		return 0
	}
	var res byte
	res, mpr.err = mpr.readByte(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readByte(fieldName string) (byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeByte)
	if err != nil {
		return 0, err
	}
	return mpr.DefaultPortableReader.readByte(fieldName)
}

func (mpr *MorphingPortableReader) ReadBool(fieldName string) bool {
	if mpr.err != nil {
		return false
	}
	var res bool
	res, mpr.err = mpr.readBool(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readBool(fieldName string) (bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return false, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeBool)
	if err != nil {
		return false, err
	}
	return mpr.DefaultPortableReader.readBool(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16(fieldName string) uint16 {
	if mpr.err != nil {
		return 0
	}
	var res uint16
	res, mpr.err = mpr.readUInt16(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readUInt16(fieldName string) (uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUint16)
	if err != nil {
		return 0, err
	}
	return mpr.DefaultPortableReader.ReadUInt16(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadInt16(fieldName string) int16 {
	if mpr.err != nil {
		return 0
	}
	var res int16
	res, mpr.err = mpr.readInt16(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readInt16(fieldName string) (int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeInt16:
		return mpr.DefaultPortableReader.ReadInt16(fieldName), mpr.err
	case classdef.TypeByte:
		ret := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int16(ret), mpr.err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeInt16)
	}
}

func (mpr *MorphingPortableReader) ReadInt32(fieldName string) int32 {
	if mpr.err != nil {
		return 0
	}
	var res int32
	res, mpr.err = mpr.readInt32(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readInt32(fieldName string) (int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeInt32:
		return mpr.DefaultPortableReader.ReadInt32(fieldName), mpr.err
	case classdef.TypeByte:
		ret := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int32(ret), mpr.err
	case classdef.TypeUint16:
		ret := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int32(ret), mpr.err
	case classdef.TypeInt16:
		ret := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int32(ret), mpr.err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeInt32)
	}
}

func (mpr *MorphingPortableReader) ReadInt64(fieldName string) int64 {
	if mpr.err != nil {
		return 0
	}
	var res int64
	res, mpr.err = mpr.readInt64(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readInt64(fieldName string) (int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeInt64:
		return mpr.DefaultPortableReader.ReadInt64(fieldName), mpr.err
	case classdef.TypeInt32:
		ret := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return int64(ret), mpr.err
	case classdef.TypeByte:
		ret := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int64(ret), mpr.err
	case classdef.TypeUint16:
		ret := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int64(ret), mpr.err
	case classdef.TypeInt16:
		ret := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int64(ret), mpr.err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeInt64)
	}
}

func (mpr *MorphingPortableReader) ReadFloat32(fieldName string) float32 {
	if mpr.err != nil {
		return 0
	}
	var res float32
	res, mpr.err = mpr.readFloat32(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readFloat32(fieldName string) (float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeFloat32:
		return mpr.DefaultPortableReader.ReadFloat32(fieldName), mpr.err
	case classdef.TypeInt32:
		ret := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float32(ret), mpr.err
	case classdef.TypeByte:
		ret := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float32(ret), mpr.err
	case classdef.TypeUint16:
		ret := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float32(ret), mpr.err
	case classdef.TypeInt16:
		ret := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float32(ret), mpr.err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeFloat32)
	}
}

func (mpr *MorphingPortableReader) ReadFloat64(fieldName string) float64 {
	if mpr.err != nil {
		return 0
	}
	var res float64
	res, mpr.err = mpr.readFloat64(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readFloat64(fieldName string) (float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeFloat64:
		return mpr.DefaultPortableReader.ReadFloat64(fieldName), mpr.err
	case classdef.TypeInt64:
		ret := mpr.DefaultPortableReader.ReadInt64(fieldName)
		return float64(ret), mpr.err
	case classdef.TypeFloat32:
		ret := mpr.DefaultPortableReader.ReadFloat32(fieldName)
		return float64(ret), mpr.err
	case classdef.TypeInt32:
		ret := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float64(ret), mpr.err
	case classdef.TypeByte:
		ret := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float64(ret), mpr.err
	case classdef.TypeUint16:
		ret := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float64(ret), mpr.err
	case classdef.TypeInt16:
		ret := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float64(ret), mpr.err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeFloat64)
	}
}

func (mpr *MorphingPortableReader) ReadUTF(fieldName string) string {
	if mpr.err != nil {
		return ""
	}
	var res string
	res, mpr.err = mpr.readUTF(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readUTF(fieldName string) (string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return "", nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUTF)
	if err != nil {
		return "", err
	}
	return mpr.DefaultPortableReader.ReadUTF(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadPortable(fieldName string) serialization.Portable {
	if mpr.err != nil {
		return nil
	}
	var res serialization.Portable
	res, mpr.err = mpr.readPortable(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readPortable(fieldName string) (serialization.Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypePortable)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadPortable(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadByteArray(fieldName string) []byte {
	if mpr.err != nil {
		return nil
	}
	var res []byte
	res, mpr.err = mpr.readByteArray(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readByteArray(fieldName string) ([]byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeByteArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadByteArray(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadBoolArray(fieldName string) []bool {
	if mpr.err != nil {
		return nil
	}
	var res []bool
	res, mpr.err = mpr.readBoolArray(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readBoolArray(fieldName string) ([]bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeBoolArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadBoolArray(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadUInt16Array(fieldName string) []uint16 {
	if mpr.err != nil {
		return nil
	}
	var res []uint16
	res, mpr.err = mpr.readUInt16Array(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readUInt16Array(fieldName string) ([]uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUint16Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadUInt16Array(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadInt16Array(fieldName string) []int16 {
	if mpr.err != nil {
		return nil
	}
	var res []int16
	res, mpr.err = mpr.readInt16Array(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readInt16Array(fieldName string) ([]int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeInt16Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt16Array(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadInt32Array(fieldName string) []int32 {
	if mpr.err != nil {
		return nil
	}
	var res []int32
	res, mpr.err = mpr.readInt32Array(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readInt32Array(fieldName string) ([]int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeInt32Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt32Array(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadInt64Array(fieldName string) []int64 {
	if mpr.err != nil {
		return nil
	}
	var res []int64
	res, mpr.err = mpr.readInt64Array(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readInt64Array(fieldName string) ([]int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeInt64Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt64Array(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadFloat32Array(fieldName string) []float32 {
	if mpr.err != nil {
		return nil
	}
	var res []float32
	res, mpr.err = mpr.readFloat32Array(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readFloat32Array(fieldName string) ([]float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeFloat32Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadFloat32Array(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadFloat64Array(fieldName string) []float64 {
	if mpr.err != nil {
		return nil
	}
	var res []float64
	res, mpr.err = mpr.readFloat64Array(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readFloat64Array(fieldName string) ([]float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeFloat64Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadFloat64Array(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadUTFArray(fieldName string) []string {
	if mpr.err != nil {
		return nil
	}
	var res []string
	res, mpr.err = mpr.readUTFArray(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readUTFArray(fieldName string) ([]string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUTFArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadUTFArray(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) ReadPortableArray(fieldName string) []serialization.Portable {
	if mpr.err != nil {
		return nil
	}
	var res []serialization.Portable
	res, mpr.err = mpr.readPortableArray(fieldName)
	return res
}

func (mpr *MorphingPortableReader) readPortableArray(fieldName string) ([]serialization.Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypePortableArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadPortableArray(fieldName), mpr.err
}

func (mpr *MorphingPortableReader) createIncompatibleClassChangeError(fd serialization.FieldDefinition,
	expectedType int32) error {
	return core.NewHazelcastSerializationError(fmt.Sprintf("incompatible to read %v from %v while reading field : %v",
		TypeByID(expectedType), TypeByID(fd.Type()), fd.Name()), nil)
}

func (mpr *MorphingPortableReader) validateTypeCompatibility(fd serialization.FieldDefinition, expectedType int32) error {
	if fd.Type() != expectedType {
		return mpr.createIncompatibleClassChangeError(fd, expectedType)
	}
	return nil
}
