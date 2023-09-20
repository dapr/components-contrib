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

package postgresql

import (
	"fmt"
	"time"

	pgauth "github.com/dapr/components-contrib/internal/authentication/postgresql"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/ptr"
)

const (
	cleanupIntervalKey = "cleanupIntervalInSeconds"
	timeoutKey         = "timeoutInSeconds"

	defaultTableName         = "state"
	defaultMetadataTableName = "dapr_metadata"
	defaultCleanupInternal   = 3600 // In seconds = 1 hour
	defaultTimeout           = 20   // Default timeout for network requests, in seconds
)

type pgMetadata struct {
	pgauth.PostgresAuthMetadata `mapstructure:",squash"`

	TableName         string         `mapstructure:"tableName"`         // Could be in the format "schema.table" or just "table"
	MetadataTableName string         `mapstructure:"metadataTableName"` // Could be in the format "schema.table" or just "table"
	Timeout           time.Duration  `mapstructure:"timeoutInSeconds"`
	CleanupInterval   *time.Duration `mapstructure:"cleanupIntervalInSeconds"`
}

func (m *pgMetadata) InitWithMetadata(meta state.Metadata, azureADEnabled bool) error {
	// Reset the object
	m.PostgresAuthMetadata.Reset()
	m.TableName = defaultTableName
	m.MetadataTableName = defaultMetadataTableName
	m.CleanupInterval = ptr.Of(defaultCleanupInternal * time.Second)
	m.Timeout = defaultTimeout * time.Second

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Validate and sanitize input
	err = m.PostgresAuthMetadata.InitWithMetadata(meta.Properties, azureADEnabled)
	if err != nil {
		return err
	}

	// Timeout
	if m.Timeout < 1*time.Second {
		return fmt.Errorf("invalid value for '%s': must be greater than 0", timeoutKey)
	}

	// Cleanup interval
	// Non-positive value from meta means disable auto cleanup.
	if m.CleanupInterval != nil && *m.CleanupInterval <= 0 {
		if meta.Properties[cleanupIntervalKey] == "" {
			// Unfortunately the mapstructure decoder decodes an empty string to 0, a missing key would be nil however
			m.CleanupInterval = ptr.Of(defaultCleanupInternal * time.Second)
		} else {
			m.CleanupInterval = nil
		}
	}

	return nil
}
