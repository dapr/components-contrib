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

	sqlserverAuth "github.com/dapr/components-contrib/common/authentication/sqlserver"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

const (
	keyColumnName        = "Key"
	rowVersionColumnName = "RowVersion"

	defaultKeyLength       = 200
	defaultTable           = "state"
	defaultMetaTable       = "dapr_metadata"
	defaultCleanupInterval = time.Hour

	// defaultBulkGetChunkSize is the default per-chunk size for BulkGet
	// requests. Requests with this many keys or fewer execute as a single
	// query.
	defaultBulkGetChunkSize = 1000
	// maxBulkGetChunkSize is the safe upper bound for keys-per-chunk on SQL
	// Server. SQL Server allows up to 2100 parameters in a single
	// parameterized query (sp_executesql limit); we leave headroom for any
	// auxiliary parameters and clamp callers above this value.
	maxBulkGetChunkSize = 2000
)

type sqlServerMetadata struct {
	sqlserverAuth.SQLServerAuthMetadata `mapstructure:",squash"`

	TableName         string
	MetadataTableName string
	KeyType           string
	KeyLength         int
	IndexedProperties string
	CleanupInterval   *time.Duration `mapstructure:"cleanupInterval" mapstructurealiases:"cleanupIntervalInSeconds"`
	// BulkGetChunkSize controls the maximum number of keys included in each
	// internal SQL query issued by BulkGet. When the number of requested
	// keys exceeds this value, BulkGet issues multiple chunked queries
	// sequentially (not in parallel) and merges the results. Values <= 0
	// default to defaultBulkGetChunkSize; values above maxBulkGetChunkSize
	// are clamped. See normalizeBulkGetChunkSize.
	BulkGetChunkSize int `mapstructure:"bulkGetChunkSize"`

	// Internal properties
	keyTypeParsed           KeyType
	keyLengthParsed         int
	indexedPropertiesParsed []IndexedProperty
}

func newMetadata() sqlServerMetadata {
	return sqlServerMetadata{
		TableName:         defaultTable,
		KeyLength:         defaultKeyLength,
		MetadataTableName: defaultMetaTable,
		CleanupInterval:   ptr.Of(defaultCleanupInterval),
	}
}

// normalizeBulkGetChunkSize applies defaults and clamping to the configured
// chunk size. Values <= 0 default to defaultBulkGetChunkSize; values above
// SQL Server's safe parameter cap are clamped to maxBulkGetChunkSize with a
// warning.
func normalizeBulkGetChunkSize(log logger.Logger, configured int) int {
	if configured <= 0 {
		return defaultBulkGetChunkSize
	}
	if configured > maxBulkGetChunkSize {
		if log != nil {
			log.Warnf("bulkGetChunkSize %d exceeds SQL Server safe parameter cap of %d; clamping to %d",
				configured, maxBulkGetChunkSize, maxBulkGetChunkSize)
		}
		return maxBulkGetChunkSize
	}
	return configured
}

func (m *sqlServerMetadata) Parse(meta map[string]string) error {
	// Reset first
	m.SQLServerAuthMetadata.Reset()

	// Decode the metadata
	err := metadata.DecodeMetadata(meta, &m)
	if err != nil {
		return err
	}

	// Validate and parse the auth metadata
	err = m.SQLServerAuthMetadata.Validate(meta)
	if err != nil {
		return err
	}

	// Validate and sanitize more values
	if !sqlserverAuth.IsValidSQLName(m.TableName) {
		return errors.New("invalid table name, accepted characters are (A-Z, a-z, 0-9, _)")
	}
	if !sqlserverAuth.IsValidSQLName(m.MetadataTableName) {
		return errors.New("invalid metadata table name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	err = m.setKeyType()
	if err != nil {
		return err
	}
	err = m.setIndexedProperties()
	if err != nil {
		return err
	}

	// Cleanup interval
	if m.CleanupInterval != nil {
		// Non-positive value from meta means disable auto cleanup.
		if *m.CleanupInterval <= 0 {
			val, _ := metadata.GetMetadataProperty(meta, "cleanupInterval", "cleanupIntervalInSeconds")
			if val == "" {
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

		if !sqlserverAuth.IsValidSQLName(p.ColumnName) {
			return errors.New("invalid indexed property column name, accepted characters are (A-Z, a-z, 0-9, _)")
		}

		if !isValidIndexedPropertyName(p.Property) {
			return errors.New("invalid indexed property name, accepted characters are (A-Z, a-z, 0-9, _, ., [, ])")
		}

		if !isValidIndexedPropertyType(p.Type) {
			return errors.New("invalid indexed property type, accepted characters are (A-Z, a-z, 0-9, _, (, ))")
		}
	}

	return nil
}

func isValidIndexedPropertyName(s string) bool {
	for _, c := range s {
		if !(sqlserverAuth.IsLetterOrNumber(c) || (c == '_') || (c == '.') || (c == '[') || (c == ']')) {
			return false
		}
	}

	return true
}

func isValidIndexedPropertyType(s string) bool {
	for _, c := range s {
		if !(sqlserverAuth.IsLetterOrNumber(c) || (c == '(') || (c == ')')) {
			return false
		}
	}

	return true
}
