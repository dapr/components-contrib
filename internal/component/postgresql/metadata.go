/*
Copyright 2021 The Dapr Authors
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

type postgresMetadataStruct struct {
	ConnectionString      string
	ConnectionMaxIdleTime time.Duration
	TableName             string // Could be in the format "schema.table" or just "table"
	MetadataTableName     string // Could be in the format "schema.table" or just "table"

	Timeout         time.Duration  `mapstructure:"timeoutInSeconds"`
	CleanupInterval *time.Duration `mapstructure:"cleanupIntervalInSeconds"`
}

func (m *postgresMetadataStruct) InitWithMetadata(meta state.Metadata) error {
	// Reset the object
	m.ConnectionString = ""
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
	if m.ConnectionString == "" {
		return errMissingConnectionString
	}

	// Timeout
	if m.Timeout < 1*time.Second {
		return fmt.Errorf("invalid value for '%s': must be greater than 0", timeoutKey)
	}

	// Cleanup interval
	if m.CleanupInterval != nil {
		// Non-positive value from meta means disable auto cleanup.
		if *m.CleanupInterval > 0 {
		} else {
			m.CleanupInterval = nil
		}
	}

	return nil
}
