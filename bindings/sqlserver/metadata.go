/*
Copyright 2024 The Dapr Authors
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
	"time"

	sqlserverAuth "github.com/dapr/components-contrib/common/authentication/sqlserver"
	"github.com/dapr/kit/metadata"
)

// sqlServerMetadata contains the metadata for the SQL Server output binding.
type sqlServerMetadata struct {
	// Connection string / Azure AD auth, shared with the SQL Server state store.
	sqlserverAuth.SQLServerAuthMetadata `mapstructure:",squash"`

	// MaxIdleConns is the maximum number of connections in the idle connection pool.
	MaxIdleConns int `mapstructure:"maxIdleConns"`

	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns int `mapstructure:"maxOpenConns"`

	// ConnMaxLifetime is the maximum amount of time a connection may be reused.
	ConnMaxLifetime time.Duration `mapstructure:"connMaxLifetime"`

	// ConnMaxIdleTime is the maximum amount of time a connection may be idle.
	ConnMaxIdleTime time.Duration `mapstructure:"connMaxIdleTime"`
}

// Parse the metadata, validating the connection/auth properties.
func (m *sqlServerMetadata) Parse(meta map[string]string) error {
	// Reset first, so repeated calls to Parse start from a clean state.
	m.Reset()

	err := metadata.DecodeMetadata(meta, &m)
	if err != nil {
		return err
	}

	// Validates the connection string and, if using Azure AD, the Azure environment options.
	return m.Validate(meta)
}
