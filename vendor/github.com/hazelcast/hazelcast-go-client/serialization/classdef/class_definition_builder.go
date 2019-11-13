package classdef

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/internal/classdef"
)

// ClassDefinitionBuilder is used to build and register class definitions manually.
type ClassDefinitionBuilder struct {
	factoryID        int32
	classID          int32
	version          int32
	fieldDefinitions map[string]serialization.FieldDefinition
	index            int32
	done             bool
}

// NewClassDefinitionBuilder returns a ClassDefinitionBuilder.
// You can use a default portableVersion (0) for non-versioned classes.
// Make sure to specify the portableVersion compatible with
// portableVersion in the serialization.Service.
func NewClassDefinitionBuilder(factoryID int32, classID int32, version int32) *ClassDefinitionBuilder {
	return &ClassDefinitionBuilder{factoryID, classID, version, make(map[string]serialization.FieldDefinition), 0, false}
}

// AddByteField adds byte field to class definition.
func (cdb *ClassDefinitionBuilder) AddByteField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeByte,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddBoolField adds bool field to class definition.
func (cdb *ClassDefinitionBuilder) AddBoolField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeBool,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddUInt16Field adds uint16 field to class definition.
func (cdb *ClassDefinitionBuilder) AddUInt16Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeUint16,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddInt16Field adds int16 field to class definition.
func (cdb *ClassDefinitionBuilder) AddInt16Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeInt16,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddInt32Field adds int32 field to class definition.
func (cdb *ClassDefinitionBuilder) AddInt32Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeInt32,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddInt64Field adds int64 field to class definition.
func (cdb *ClassDefinitionBuilder) AddInt64Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeInt64,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddFloat32Field adds float32 field to class definition.
func (cdb *ClassDefinitionBuilder) AddFloat32Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeFloat32,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddFloat64Field adds float64 field to class definition.
func (cdb *ClassDefinitionBuilder) AddFloat64Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeFloat64,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddUTFField adds UTF field to class definition.
func (cdb *ClassDefinitionBuilder) AddUTFField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeUTF,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddPortableField adds Portable field to class definition.
func (cdb *ClassDefinitionBuilder) AddPortableField(fieldName string, def serialization.ClassDefinition) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	if def.ClassID() == 0 {
		return core.NewHazelcastIllegalArgumentError("Portable class id cannot be zero", nil)
	}

	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypePortable,
		def.FactoryID(), def.ClassID(), cdb.version)
	cdb.index++
	return nil
}

// AddByteArrayField adds []byte field to class definition.
func (cdb *ClassDefinitionBuilder) AddByteArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeByteArray,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddBoolArrayField adds []bool field to class definition.
func (cdb *ClassDefinitionBuilder) AddBoolArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeBoolArray,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddUInt16ArrayField adds []uint16 field to class definition.
func (cdb *ClassDefinitionBuilder) AddUInt16ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeUint16Array,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddInt16ArrayField adds []int16 field to class definition.
func (cdb *ClassDefinitionBuilder) AddInt16ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeInt16Array,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddInt32ArrayField adds []int32 field to class definition.
func (cdb *ClassDefinitionBuilder) AddInt32ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeInt32Array,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddInt64ArrayField adds []int64 field to class definition.
func (cdb *ClassDefinitionBuilder) AddInt64ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeInt64Array,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddFloat32ArrayField adds []float32 field to class definition.
func (cdb *ClassDefinitionBuilder) AddFloat32ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeFloat32Array,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddFloat64ArrayField adds []float64 field to class definition.
func (cdb *ClassDefinitionBuilder) AddFloat64ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeFloat64Array,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddUTFArrayField adds []string field to class definition.
func (cdb *ClassDefinitionBuilder) AddUTFArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypeUTFArray,
		0, 0, cdb.version)
	cdb.index++
	return nil
}

// AddPortableArrayField adds []Portable field to class definition.
func (cdb *ClassDefinitionBuilder) AddPortableArrayField(fieldName string, def serialization.ClassDefinition) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	if def.ClassID() == 0 {
		return core.NewHazelcastIllegalArgumentError("Portable class id cannot be zero", nil)
	}

	cdb.fieldDefinitions[fieldName] = classdef.NewFieldDefinitionImpl(cdb.index, fieldName, classdef.TypePortableArray,
		def.FactoryID(), def.ClassID(), cdb.version)
	cdb.index++
	return nil
}

// AddField adds a field to class definition.
func (cdb *ClassDefinitionBuilder) AddField(fieldDefinition serialization.FieldDefinition) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	if cdb.index != fieldDefinition.Index() {
		return core.NewHazelcastIllegalArgumentError("invalid field index", nil)
	}
	cdb.index++
	cdb.fieldDefinitions[fieldDefinition.Name()] = fieldDefinition
	return nil
}

// Build returns the built class definition.
func (cdb *ClassDefinitionBuilder) Build() serialization.ClassDefinition {
	cdb.done = true
	cd := classdef.NewClassDefinitionImpl(cdb.factoryID, cdb.classID, cdb.version)
	for _, fd := range cdb.fieldDefinitions {
		cd.AddFieldDefinition(fd)
	}
	return cd
}

func (cdb *ClassDefinitionBuilder) check() error {
	if cdb.done {
		return core.NewHazelcastSerializationError(fmt.Sprintf("ClassDefinition is already built for %v", cdb.classID), nil)
	}
	return nil
}
