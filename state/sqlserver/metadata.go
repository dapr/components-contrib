/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/ptr"
)

const (
	connectionStringKey  = "connectionString"
	tableNameKey         = "tableName"
	metadataTableNameKey = "metadataTableName"
	schemaKey            = "schema"
	keyTypeKey           = "keyType"
	keyLengthKey         = "keyLength"
	indexedPropertiesKey = "indexedProperties"
	keyColumnName        = "Key"
	rowVersionColumnName = "RowVersion"
	databaseNameKey      = "databaseName"
	cleanupIntervalKey   = "cleanupIntervalInSeconds"

	defaultKeyLength       = 200
	defaultSchema          = "dbo"
	defaultDatabase        = "dapr"
	defaultTable           = "state"
	defaultMetaTable       = "dapr_metadata"
	defaultCleanupInterval = time.Hour
)

type sqlServerMetadata struct {
	ConnectionString  string
	DatabaseName      string
	TableName         string
	MetadataTableName string
	Schema            string
	KeyType           string
	KeyLength         int
	IndexedProperties string
	CleanupInterval   *time.Duration `mapstructure:"cleanupIntervalInSeconds"`

	// Internal properties
	keyTypeParsed           KeyType
	keyLengthParsed         int
	indexedPropertiesParsed []IndexedProperty
}

func newMetadata() sqlServerMetadata {
	return sqlServerMetadata{
		TableName:         defaultTable,
		Schema:            defaultSchema,
		DatabaseName:      defaultDatabase,
		KeyLength:         defaultKeyLength,
		MetadataTableName: defaultMetaTable,
		CleanupInterval:   ptr.Of(defaultCleanupInterval),
	}
}

func (m *sqlServerMetadata) Parse(meta map[string]string) error {
	err := metadata.DecodeMetadata(meta, &m)
	if err != nil {
		return err
	}
	if m.ConnectionString == "" {
		return errors.New("missing connection string")
	}

	if !isValidSQLName(m.TableName) {
		return fmt.Errorf("invalid table name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	if !isValidSQLName(m.MetadataTableName) {
		return fmt.Errorf("invalid metadata table name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	if !isValidSQLName(m.DatabaseName) {
		return fmt.Errorf("invalid database name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	err = m.setKeyType()
	if err != nil {
		return err
	}

	if !isValidSQLName(m.Schema) {
		return fmt.Errorf("invalid schema name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	err = m.setIndexedProperties()
	if err != nil {
		return err
	}

	// Cleanup interval
	if m.CleanupInterval != nil {
		// Non-positive value from meta means disable auto cleanup.
		if *m.CleanupInterval <= 0 {
			if meta[cleanupIntervalKey] == "" {
				// Unfortunately the mapstructure decoder decodes an empty string to 0, a missing key would be nil however
				m.CleanupInterval = ptr.Of(defaultCleanupInterval)
			} else {
				m.CleanupInterval = nil
			}
		}
	}

	return nil
}

// Validates and returns the key type.
func (m *sqlServerMetadata) setKeyType() error {
	if m.KeyType != "" {
		kt, err := KeyTypeFromString(m.KeyType)
		if err != nil {
			return err
		}

		m.keyTypeParsed = kt
	} else {
		m.keyTypeParsed = StringKeyType
	}

	if m.keyTypeParsed != StringKeyType {
		return nil
	}

	if m.KeyLength <= 0 {
		return fmt.Errorf("invalid key length value of %d", m.KeyLength)
	} else {
		m.keyLengthParsed = m.KeyLength
	}

	return nil
}

// Sets the validated index properties.
func (m *sqlServerMetadata) setIndexedProperties() error {
	if m.IndexedProperties == "" {
		return nil
	}

	var indexedProperties []IndexedProperty
	err := json.Unmarshal([]byte(m.IndexedProperties), &indexedProperties)
	if err != nil {
		return err
	}

	err = m.validateIndexedProperties(indexedProperties)
	if err != nil {
		return err
	}

	m.indexedPropertiesParsed = indexedProperties

	return nil
}

// Validates that all the mandator index properties are supplied and that the
// values are valid.
func (m *sqlServerMetadata) validateIndexedProperties(indexedProperties []IndexedProperty) error {
	for _, p := range indexedProperties {
		if p.ColumnName == "" {
			return errors.New("indexed property column cannot be empty")
		}

		if p.Property == "" {
			return errors.New("indexed property name cannot be empty")
		}

		if p.Type == "" {
			return errors.New("indexed property type cannot be empty")
		}

		if !isValidSQLName(p.ColumnName) {
			return fmt.Errorf("invalid indexed property column name, accepted characters are (A-Z, a-z, 0-9, _)")
		}

		if !isValidIndexedPropertyName(p.Property) {
			return fmt.Errorf("invalid indexed property name, accepted characters are (A-Z, a-z, 0-9, _, ., [, ])")
		}

		if !isValidIndexedPropertyType(p.Type) {
			return fmt.Errorf("invalid indexed property type, accepted characters are (A-Z, a-z, 0-9, _, (, ))")
		}
	}

	return nil
}

func isLetterOrNumber(c rune) bool {
	return unicode.IsNumber(c) || unicode.IsLetter(c)
}

func isValidSQLName(s string) bool {
	for _, c := range s {
		if !(isLetterOrNumber(c) || (c == '_')) {
			return false
		}
	}

	return true
}

func isValidIndexedPropertyName(s string) bool {
	for _, c := range s {
		if !(isLetterOrNumber(c) || (c == '_') || (c == '.') || (c == '[') || (c == ']')) {
			return false
		}
	}

	return true
}

func isValidIndexedPropertyType(s string) bool {
	for _, c := range s {
		if !(isLetterOrNumber(c) || (c == '(') || (c == ')')) {
			return false
		}
	}

	return true
}
