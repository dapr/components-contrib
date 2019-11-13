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

// Package serialization serializes user objects to Data and back to Object.
// Data is the internal representation of binary data in Hazelcast.
package serialization

// IdentifiedDataSerializableFactory is used to create IdentifiedDataSerializable instances during deserialization.
type IdentifiedDataSerializableFactory interface {
	// Creates an IdentifiedDataSerializable instance using given type ID.
	Create(id int32) (instance IdentifiedDataSerializable)
}

// IdentifiedDataSerializable is a serialization method as an alternative to standard Gob serialization.
// Each IdentifiedDataSerializable is created by a registered IdentifiedDataSerializableFactory.
type IdentifiedDataSerializable interface {
	// FactoryID returns IdentifiedDataSerializableFactory factory ID for this struct.
	FactoryID() (factoryID int32)

	// ClassID returns type identifier for this struct. It should be unique per IdentifiedDataSerializableFactory.
	ClassID() (classID int32)

	// WriteData writes object fields to output stream.
	WriteData(output DataOutput) (err error)

	// ReadData reads fields from the input stream.
	ReadData(input DataInput) (err error)
}

// Portable provides an alternative serialization method. Instead of relying on reflection, each Portable is
// created by a registered PortableFactory.
// Portable serialization has the following advantages:
//
// * Supporting multiversion of the same object type.
//
// * Fetching individual fields without having to rely on reflection.
//
// * Querying and indexing support without deserialization and/or reflection.
type Portable interface {
	// FactoryID returns PortableFactory ID for this portable struct.
	FactoryID() (factoryID int32)

	// ClassID returns type identifier for this portable struct. Class ID should be unique per PortableFactory.
	ClassID() (classID int32)

	// WritePortable serializes this portable object using PortableWriter.
	WritePortable(writer PortableWriter) (err error)

	// ReadPortable reads portable fields using PortableReader.
	ReadPortable(reader PortableReader) (err error)
}

// VersionedPortable is an extension to Portable
// to support per class version instead of a global serialization version.
type VersionedPortable interface {
	Portable

	// Version returns version for this Portable struct.
	Version() (version int32)
}

// PortableFactory is used to create Portable instances during deserialization.
type PortableFactory interface {
	// Create creates a Portable instance using given class ID and
	// returns portable instance or nil if class ID is not known by this factory.
	Create(classID int32) (instance Portable)
}

// Serializer is base interface of serializers.
type Serializer interface {
	// ID returns id of serializer.
	ID() (id int32)

	// Read reads an object from ObjectDataInput.
	Read(input DataInput) (object interface{}, err error)

	// Write writes an object to ObjectDataOutput.
	Write(output DataOutput, object interface{}) (err error)
}

// Data is the basic unit of serialization. It stores binary form of an object serialized
// by serialization service's ToData() method.
type Data interface {
	// Buffer returns byte array representation of internal binary format.
	Buffer() []byte

	// GetType returns serialization type of binary form.
	GetType() int32

	// TotalSize returns the total size of Data in bytes.
	TotalSize() int

	// DataSize returns size of internal binary data in bytes.
	DataSize() int

	// GetPartitionHash returns partition hash calculated for serialized object.
	GetPartitionHash() int32
}

// DataOutput provides serialization methods.
type DataOutput interface {
	// Position returns the head position in the byte array.
	Position() int32

	// SetPosition sets the head position in the byte array.
	SetPosition(pos int32)

	// WriteByte writes a byte.
	WriteByte(v byte)

	// WriteBool writes a bool.
	WriteBool(v bool)

	// WriteUInt16 writes an uint16.
	WriteUInt16(v uint16)

	// WriteInt16 writes an int16.
	WriteInt16(v int16)

	// WriteInt32 writes an int32.
	WriteInt32(v int32)

	// WriteInt64 writes an int64.
	WriteInt64(v int64)

	// WriteFloat32 writes a float32.
	WriteFloat32(v float32)

	// WriteFloat64 writes a float64.
	WriteFloat64(v float64)

	// WriteUTF writes a string in UTF-8 format.
	WriteUTF(v string)

	// WriteObject writes an object.
	WriteObject(i interface{}) error

	// WriteData writes an Data.
	WriteData(data Data)

	// WriteByteArray writes a []byte.
	WriteByteArray(v []byte)

	// WriteBoolArray writes a []bool.
	WriteBoolArray(v []bool)

	// WriteUInt16Array writes an []uint16.
	WriteUInt16Array(v []uint16)

	// WriteInt16Array writes an []int16.
	WriteInt16Array(v []int16)

	// WriteInt32Array writes an []int32.
	WriteInt32Array(v []int32)

	// WriteInt64Array writes an []int64.
	WriteInt64Array(v []int64)

	// WriteFloat32Array writes a []float32.
	WriteFloat32Array(v []float32)

	// WriteFloat64Array writes a []float64.
	WriteFloat64Array(v []float64)

	// WriteUTFArray writes a []string in UTF-8 format.
	WriteUTFArray(v []string)

	// WriteBytes writes a string's characters.
	WriteBytes(bytes string)

	// WriteZeroBytes writes zero bytes as given length.
	WriteZeroBytes(count int)
}

// PositionalDataOutput provides some serialization methods for a specific position.
type PositionalDataOutput interface {
	// DataOutput provides serialization methods.
	DataOutput

	// PWriteByte writes a byte to a specific position.
	PWriteByte(position int32, v byte)

	// PWriteBool writes a bool to a specific position.
	PWriteBool(position int32, v bool)

	// PWriteUInt16 writes an uint16 to a specific position.
	PWriteUInt16(position int32, v uint16)

	// PWriteInt16 writes an int16 to a specific position.
	PWriteInt16(position int32, v int16)

	// PWriteInt32 writes an int32 to a specific position.
	PWriteInt32(position int32, v int32)

	// PWriteInt64 writes an int64 to a specific position.
	PWriteInt64(position int32, v int64)

	// PWriteFloat32 writes a float32 to a specific position.
	PWriteFloat32(position int32, v float32)

	// PWriteFloat64 writes a float64 to a specific position.
	PWriteFloat64(position int32, v float64)
}

// DataInput provides deserialization methods.
// If any of the methods results in an error, all following methods will return the zero value
// for that type immediately.
// Example usage:
//  field1 = input.ReadUTF()
//  field2 = input.ReadUTF()
//  return input.Error()
type DataInput interface {
	// Error returns the first error encountered by DataInput.
	Error() error

	// Position returns the head position in the byte array.
	Position() int32

	// SetPosition sets the head position in the byte array.
	SetPosition(pos int32)

	// ReadByte returns byte read .
	// It returns zero if an error is set previously.
	ReadByte() byte

	// ReadBool returns bool read.
	// It returns false if an error is set previously.
	ReadBool() bool

	// ReadUInt16 returns uint16 read.
	// It returns zero if an error is set previously.
	ReadUInt16() uint16

	// ReadInt16 returns int16 read.
	// It returns zero if an error is set previously.
	ReadInt16() int16

	// ReadInt32 returns int32 read.
	// It returns zero if an error is set previously.
	ReadInt32() int32

	// ReadInt64 returns int64 read.
	// It returns zero if an error is set previously.
	ReadInt64() int64

	// ReadFloat32 returns float32 read.
	// It returns zero if an error is set previously.
	ReadFloat32() float32

	// ReadFloat64 returns float64 read.
	// It returns zero if an error is set previously.
	ReadFloat64() float64

	// ReadUTF returns string read.
	// It returns empty string if an error is set previously.
	ReadUTF() string

	// ReadObject returns object read.
	// It returns nil if an error is set previously.
	ReadObject() interface{}

	// ReadData returns Data read.
	// It returns nil if an error is set previously.
	ReadData() Data

	// ReadByteArray returns []byte read.
	// It returns nil if an error is set previously.
	ReadByteArray() []byte

	// ReadBoolArray returns []bool read.
	// It returns nil if an error is set previously.
	ReadBoolArray() []bool

	// ReadUInt16Array returns []uint16 read.
	// It returns nil if an error is set previously.
	ReadUInt16Array() []uint16

	// ReadInt16Array returns []int16 read.
	// It returns nil if an error is set previously.
	ReadInt16Array() []int16

	// ReadInt32Array returns []int32 read.
	// It returns nil if an error is set previously.
	ReadInt32Array() []int32

	// ReadInt64Array returns []int64 read.
	// It returns nil if an error is set previously.
	ReadInt64Array() []int64

	// ReadFloat32Array returns []float32 read.
	// It returns nil if an error is set previously.
	ReadFloat32Array() []float32

	// ReadFloat64Array returns []float64 read.
	// It returns nil if an error is set previously.
	ReadFloat64Array() []float64

	// ReadUTFArray returns []string read.
	// It returns nil if an error is set previously.
	ReadUTFArray() []string
}

// PortableWriter provides a mean of writing portable fields to a binary in form of go primitives
// arrays of go primitives, nested portable fields and array of portable fields.
type PortableWriter interface {
	// WriteByte writes a byte with fieldName.
	WriteByte(fieldName string, value byte)

	// WriteBool writes a bool with fieldName.
	WriteBool(fieldName string, value bool)

	// WriteUInt16 writes a uint16 with fieldName.
	WriteUInt16(fieldName string, value uint16)

	// WriteInt16 writes a int16 with fieldName.
	WriteInt16(fieldName string, value int16)

	// WriteInt32 writes a int32 with fieldName.
	WriteInt32(fieldName string, value int32)

	// WriteInt64 writes a int64 with fieldName.
	WriteInt64(fieldName string, value int64)

	// WriteFloat32 writes a float32 with fieldName.
	WriteFloat32(fieldName string, value float32)

	// WriteFloat64 writes a float64 with fieldName.
	WriteFloat64(fieldName string, value float64)

	// WriteUTF writes a string in UTF-8 format with fieldName.
	WriteUTF(fieldName string, value string)

	// WritePortable writes a Portable with fieldName.
	WritePortable(fieldName string, value Portable) error

	// WriteNilPortable writes a NilPortable with fieldName, factoryID and classID.
	WriteNilPortable(fieldName string, factoryID int32, classID int32) error

	// WriteByteArray writes a []byte with fieldName.
	WriteByteArray(fieldName string, value []byte)

	// WriteBoolArray writes a []bool with fieldName.
	WriteBoolArray(fieldName string, value []bool)

	// WriteUInt16Array writes a []uint16 with fieldName.
	WriteUInt16Array(fieldName string, value []uint16)

	// WriteInt16Array writes a []int16 with fieldName.
	WriteInt16Array(fieldName string, value []int16)

	// WriteInt32Array writes a []int32 with fieldName.
	WriteInt32Array(fieldName string, value []int32)

	// WriteInt64Array writes a []int64 with fieldName.
	WriteInt64Array(fieldName string, value []int64)

	// WriteFloat32Array writes a []float32 with fieldName.
	WriteFloat32Array(fieldName string, value []float32)

	// WriteFloat64Array writes a []float64 with fieldName.
	WriteFloat64Array(fieldName string, value []float64)

	// WriteUTFArray writes a []string in UTF-8 format with fieldName.
	WriteUTFArray(fieldName string, value []string)

	// WritePortableArray writes a []Portable with fieldName.
	WritePortableArray(fieldName string, value []Portable) error
}

// PortableReader provides a mean of reading portable fields from a binary in form of go primitives
// arrays of go primitives, nested portable fields and array of portable fields.
// Example usage:
// 	s.id = reader.ReadInt16("id")
//  s.age = reader.ReadInt32("age")
//  return reader.Error()
type PortableReader interface {

	// Error returns the first error encountered by Portable Reader.
	Error() error

	// ReadByte takes fieldName name of the field and returns the byte value read.
	// It returns zero if an error is set previously.
	ReadByte(fieldName string) byte

	// ReadBool takes fieldName name of the field and returns the bool value read.
	// It returns false if an error is set previously.
	ReadBool(fieldName string) bool

	// ReadUInt16 takes fieldName name of the field and returns the uint16 value read.
	// It returns zero if an error is set previously.
	ReadUInt16(fieldName string) uint16

	// ReadInt16 takes fieldName name of the field and returns the int16 value read.
	// It returns zero if an error is set previously.
	ReadInt16(fieldName string) int16

	// ReadInt32 takes fieldName name of the field and returns the int32 value read.
	// It returns zero if an error is set previously.
	ReadInt32(fieldName string) int32

	// ReadInt64 takes fieldName name of the field and returns the int64 value read.
	// It returns zero if an error is set previously.
	ReadInt64(fieldName string) int64

	// ReadFloat32 takes fieldName name of the field and returns the float32 value read.
	// It returns zero if an error is set previously.
	ReadFloat32(fieldName string) float32

	// ReadFloat64 takes fieldName name of the field and returns the float64 value read.
	// It returns zero if an error is set previously.
	ReadFloat64(fieldName string) float64

	// ReadUTF takes fieldName name of the field and returns the string value read.
	// It returns empty string if an error is set previously.
	ReadUTF(fieldName string) string

	// ReadPortable takes fieldName name of the field and returns the Portable value read.
	// It returns nil if an error is set previously.
	ReadPortable(fieldName string) Portable

	// ReadByteArray takes fieldName name of the field and returns the []byte value read.
	// It returns nil if an error is set previously.
	ReadByteArray(fieldName string) []byte

	// ReadBoolArray takes fieldName name of the field and returns the []bool value read.
	// It returns nil if an error is set previously.
	ReadBoolArray(fieldName string) []bool

	// ReadUInt16Array takes fieldName name of the field and returns the []uint16 value read.
	// It returns nil if an error is set previously.
	ReadUInt16Array(fieldName string) []uint16

	// ReadInt16Array takes fieldName name of the field and returns the []int16 value read.
	// It returns nil if an error is set previously.
	ReadInt16Array(fieldName string) []int16

	// ReadInt32Array takes fieldName name of the field and returns the []int32 value read.
	// It returns nil if an error is set previously.
	ReadInt32Array(fieldName string) []int32

	// ReadInt64Array takes fieldName name of the field and returns the []int64 value read.
	// It returns nil if an error is set previously.
	ReadInt64Array(fieldName string) []int64

	// ReadFloat32Array takes fieldName name of the field and returns the []float32 value read.
	// It returns nil if an error is set previously.
	ReadFloat32Array(fieldName string) []float32

	// ReadFloat64Array takes fieldName name of the field and returns the []float64 value read.
	// It returns nil if an error is set previously.
	ReadFloat64Array(fieldName string) []float64

	// ReadUTFArray takes fieldName name of the field and returns the []string value read.
	// It returns nil if an error is set previously.
	ReadUTFArray(fieldName string) []string

	// ReadPortableArray takes fieldName name of the field and returns the []Portable value read.
	// It returns nil if an error is set previously.
	ReadPortableArray(fieldName string) []Portable
}

// ClassDefinition defines a class schema for Portable structs.
type ClassDefinition interface {
	// FactoryID returns factory ID of struct.
	FactoryID() int32

	// ClassID returns class ID of struct.
	ClassID() int32

	// Version returns version of struct.
	Version() int32

	// Field returns field definition of field by given name.
	Field(name string) FieldDefinition

	// FieldCount returns the number of fields in struct.
	FieldCount() int
}

// FieldDefinition defines name, type, index of a field.
type FieldDefinition interface {
	// Type returns field type.
	Type() int32

	// Name returns field name.
	Name() string

	// Index returns field index.
	Index() int32

	// ClassID returns class ID of this field's struct.
	ClassID() int32

	// FactoryID returns factory ID of this field's struct.
	FactoryID() int32

	// Version returns version of this field's struct.
	Version() int32
}
