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
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/components-contrib/internal/authentication/azure"
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
	ConnectionString      string         `mapstructure:"connectionString"`
	ConnectionMaxIdleTime time.Duration  `mapstructure:"connectionMaxIdleTime"`
	TableName             string         `mapstructure:"tableName"`         // Could be in the format "schema.table" or just "table"
	MetadataTableName     string         `mapstructure:"metadataTableName"` // Could be in the format "schema.table" or just "table"
	Timeout               time.Duration  `mapstructure:"timeoutInSeconds"`
	CleanupInterval       *time.Duration `mapstructure:"cleanupIntervalInSeconds"`
	MaxConns              int            `mapstructure:"maxConns"`
	UseAzureAD            bool           `mapstructure:"useAzureAD"`

	// Set to true if the component can support authentication with Azure AD.
	// This is different from the "useAzureAD" property above, which is provided by the user and instructs the component to authenticate using Azure AD.
	azureADEnabled bool
	azureEnv       azure.EnvironmentSettings
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
		if *m.CleanupInterval <= 0 {
			if meta.Properties[cleanupIntervalKey] == "" {
				// unfortunately the mapstructure decoder decodes an empty string to 0, a missing key would be nil however
				m.CleanupInterval = ptr.Of(defaultCleanupInternal * time.Second)
			} else {
				m.CleanupInterval = nil
			}
		}
	}

	// Populate the Azure environment if using Azure AD
	if m.azureADEnabled && m.UseAzureAD {
		m.azureEnv, err = azure.NewEnvironmentSettings(meta.Properties)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetPgxPoolConfig returns the pgxpool.Config object that contains the credentials for connecting to Postgres.
func (m *postgresMetadataStruct) GetPgxPoolConfig() (*pgxpool.Config, error) {
	// Get the config from the connection string
	config, err := pgxpool.ParseConfig(m.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if m.ConnectionMaxIdleTime > 0 {
		config.MaxConnIdleTime = m.ConnectionMaxIdleTime
	}
	if m.MaxConns > 1 {
		config.MaxConns = int32(m.MaxConns)
	}

	// Check if we should use Azure AD
	if m.azureADEnabled && m.UseAzureAD {
		tokenCred, errToken := m.azureEnv.GetTokenCredential()
		if errToken != nil {
			return nil, errToken
		}

		// Reset the password
		config.ConnConfig.Password = ""

		/*// For Azure AD, using SSL is required
		// If not already enabled, configure TLS without certificate validation
		if config.ConnConfig.TLSConfig == nil {
			config.ConnConfig.TLSConfig = &tls.Config{
				//nolint:gosec
				InsecureSkipVerify: true,
			}
		}*/

		// We need to retrieve the token every time we attempt a new connection
		// This is because tokens expire, and connections can drop and need to be re-established at any time
		// Fortunately, we can do this with the "BeforeConnect" hook
		config.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
			at, err := tokenCred.GetToken(ctx, policy.TokenRequestOptions{
				Scopes: []string{
					m.azureEnv.Cloud.Services[azure.ServiceOSSRDBMS].Audience + "/.default",
				},
			})
			if err != nil {
				return err
			}

			cc.Password = at.Token
			return nil
		}
	}

	return config, nil
}
