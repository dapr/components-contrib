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

package sqlite

import (
	"fmt"
	"time"

	authSqlite "github.com/dapr/components-contrib/internal/authentication/sqlite"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

const (
	defaultTableName         = "state"
	defaultMetadataTableName = "metadata"
	defaultCleanupInternal   = time.Duration(0) // Disabled by default
)

type sqliteMetadataStruct struct {
	authSqlite.SqliteAuthMetadata `mapstructure:",squash"`

	TableName         string        `mapstructure:"tableName"`
	MetadataTableName string        `mapstructure:"metadataTableName"`
	CleanupInterval   time.Duration `mapstructure:"cleanupInterval" mapstructurealiases:"cleanupIntervalInSeconds"`
}

func (m *sqliteMetadataStruct) InitWithMetadata(meta state.Metadata) error {
	// Reset the object
	m.reset()

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Validate and sanitize input
	err = m.SqliteAuthMetadata.Validate()
	if err != nil {
		return err
	}
	if !authSqlite.ValidIdentifier(m.TableName) {
		return fmt.Errorf("invalid identifier: %s", m.TableName)
	}
	if !authSqlite.ValidIdentifier(m.MetadataTableName) {
		return fmt.Errorf("invalid identifier: %s", m.MetadataTableName)
	}

	return nil
}

// Reset the object
func (m *sqliteMetadataStruct) reset() {
	m.SqliteAuthMetadata.Reset()

	m.TableName = defaultTableName
	m.MetadataTableName = defaultMetadataTableName
	m.CleanupInterval = defaultCleanupInternal
}
