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

package serialization

import (
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/core"
)

// Config contains the serialization configuration of a Hazelcast instance.
type Config struct {
	// isBigEndian is the byte order bool. If true, it means BigEndian, otherwise LittleEndian.
	isBigEndian bool

	// dataSerializableFactories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
	dataSerializableFactories map[int32]IdentifiedDataSerializableFactory

	// portableFactories is a map of factory IDs and corresponding Portable factories.
	portableFactories map[int32]PortableFactory

	// portableVersion will be used to differentiate two versions of the same struct that have changes on the struct,
	// like adding/removing a field or changing a type of a field.
	portableVersion int32

	// customSerializers is a map of object types and corresponding custom serializers.
	customSerializers map[reflect.Type]Serializer

	// globalSerializer is the serializer that will be used if no other serializer is applicable.
	globalSerializer Serializer

	// classDefinitions contains ClassDefinitions for portable structs.
	classDefinitions []ClassDefinition
}

// NewConfig returns a Config with default values.
func NewConfig() *Config {
	return &Config{
		isBigEndian:               true,
		dataSerializableFactories: make(map[int32]IdentifiedDataSerializableFactory),
		portableFactories:         make(map[int32]PortableFactory),
		portableVersion:           0,
		customSerializers:         make(map[reflect.Type]Serializer),
	}
}

// IsBigEndian returns isBigEndian bool value.
func (sc *Config) IsBigEndian() bool {
	return sc.isBigEndian
}

// DataSerializableFactories returns a map of factory IDs and corresponding IdentifiedDataSerializable factories.
func (sc *Config) DataSerializableFactories() map[int32]IdentifiedDataSerializableFactory {
	return sc.dataSerializableFactories
}

// PortableFactories returns a map of factory IDs and corresponding Portable factories.
func (sc *Config) PortableFactories() map[int32]PortableFactory {
	return sc.portableFactories
}

// PortableVersion returns version of a portable struct.
func (sc *Config) PortableVersion() int32 {
	return sc.portableVersion
}

// CustomSerializers returns a map of object types and corresponding custom serializers.
func (sc *Config) CustomSerializers() map[reflect.Type]Serializer {
	return sc.customSerializers
}

// GlobalSerializer returns the global serializer.
func (sc *Config) GlobalSerializer() Serializer {
	return sc.globalSerializer
}

// ClassDefinitions returns registered class definitions of portable structs.
func (sc *Config) ClassDefinitions() []ClassDefinition {
	return sc.classDefinitions
}

// SetByteOrder sets the byte order. If true, it means BigEndian, otherwise LittleEndian.
func (sc *Config) SetByteOrder(isBigEndian bool) {
	sc.isBigEndian = isBigEndian
}

// AddDataSerializableFactory adds an IdentifiedDataSerializableFactory for a given factory ID.
func (sc *Config) AddDataSerializableFactory(factoryID int32, f IdentifiedDataSerializableFactory) {
	sc.dataSerializableFactories[factoryID] = f
}

// AddPortableFactory adds a PortableFactory for a given factory ID.
func (sc *Config) AddPortableFactory(factoryID int32, pf PortableFactory) {
	sc.portableFactories[factoryID] = pf
}

// AddClassDefinition registers class definitions explicitly.
func (sc *Config) AddClassDefinition(classDefinition ...ClassDefinition) {
	sc.classDefinitions = append(sc.classDefinitions, classDefinition...)
}

// SetPortableVersion sets the portable version.
func (sc *Config) SetPortableVersion(version int32) {
	sc.portableVersion = version
}

// AddCustomSerializer adds a custom serializer for a given type. It can be an interface type or a struct type.
func (sc *Config) AddCustomSerializer(typ reflect.Type, serializer Serializer) error {
	if serializer.ID() > 0 {
		sc.customSerializers[typ] = serializer
	} else {
		return core.NewHazelcastSerializationError("custom serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}

// SetGlobalSerializer sets the global serializer.
func (sc *Config) SetGlobalSerializer(serializer Serializer) error {
	if serializer.ID() > 0 {
		sc.globalSerializer = serializer
	} else {
		return core.NewHazelcastSerializationError("global serializer should have its typeId greater than or equal to 1", nil)
	}
	return nil
}
