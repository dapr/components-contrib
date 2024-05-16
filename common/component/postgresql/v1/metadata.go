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
	"errors"
	"time"

	"github.com/dapr/components-contrib/common/authentication/aws"
	pgauth "github.com/dapr/components-contrib/common/authentication/postgresql"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

const (
	defaultTableName         = "state"
	defaultMetadataTableName = "dapr_metadata"
	defaultCleanupInternal   = time.Hour
	defaultTimeout           = 20 * time.Second // Default timeout for network requests
)

type pgMetadata struct {
	pgauth.PostgresAuthMetadata `mapstructure:",squash"`

	TableName         string         `mapstructure:"tableName"`         // Could be in the format "schema.table" or just "table"
	MetadataTableName string         `mapstructure:"metadataTableName"` // Could be in the format "schema.table" or just "table"
	Timeout           time.Duration  `mapstructure:"timeout" mapstructurealiases:"timeoutInSeconds"`
	CleanupInterval   *time.Duration `mapstructure:"cleanupInterval" mapstructurealiases:"cleanupIntervalInSeconds"`

	aws.AWSIAM `mapstructure:",squash"`
}

func (m *pgMetadata) InitWithMetadata(meta state.Metadata, opts pgauth.InitWithMetadataOpts) error {
	// Reset the object
	m.PostgresAuthMetadata.Reset()
	m.TableName = defaultTableName
	m.MetadataTableName = defaultMetadataTableName
	m.CleanupInterval = ptr.Of(defaultCleanupInternal)
	m.Timeout = defaultTimeout

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Validate and sanitize input
	err = m.PostgresAuthMetadata.InitWithMetadata(meta.Properties, opts)
	if err != nil {
		return err
	}

	// Timeout
	if m.Timeout < 1*time.Second {
		return errors.New("invalid value for 'timeout': must be greater than 1s")
	}

	// Cleanup interval
	// Non-positive value from meta means disable auto cleanup.
	// We need to do this check because an empty string and "0" are treated differently by DecodeMetadata
	v, ok := meta.GetProperty("cleanupInterval", "cleanupIntervalInSeconds")
	if ok && v == "" {
		// Handle the case of an empty string, but present
		m.CleanupInterval = ptr.Of(defaultCleanupInternal)
	} else if (ok && v == "0") || (m.CleanupInterval != nil && *m.CleanupInterval <= 0) {
		m.CleanupInterval = nil
	}

	return nil
}
