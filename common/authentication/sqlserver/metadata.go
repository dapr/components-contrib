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
	"context"
	"errors"
	"fmt"
	"unicode"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/dapr/components-contrib/common/authentication/azure"
)

// SQLServerAuthMetadata contains the auth metadata for a SQL Server component.
type SQLServerAuthMetadata struct {
	ConnectionString string `mapstructure:"connectionString" mapstructurealiases:"url"`
	DatabaseName     string `mapstructure:"databaseName" mapstructurealiases:"database"`
	SchemaName       string `mapstructure:"schemaName" mapstructurealiases:"schema"`
	UseAzureAD       bool   `mapstructure:"useAzureAD"`

	azureEnv azure.EnvironmentSettings
}

// Reset the object
func (m *SQLServerAuthMetadata) Reset() {
	m.ConnectionString = ""
	m.DatabaseName = "dapr"
	m.SchemaName = "dbo"
	m.UseAzureAD = false
}

// Validate the auth metadata and returns an error if it's not valid.
func (m *SQLServerAuthMetadata) Validate(meta map[string]string) (err error) {
	// Validate and sanitize input
	if m.ConnectionString == "" {
		return errors.New("missing connection string")
	}
	if !IsValidSQLName(m.DatabaseName) {
		return errors.New("invalid database name, accepted characters are (A-Z, a-z, 0-9, _)")
	}
	if !IsValidSQLName(m.SchemaName) {
		return errors.New("invalid schema name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	// If using Azure AD
	if m.UseAzureAD {
		m.azureEnv, err = azure.NewEnvironmentSettings(meta)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetConnector returns the connector from the connection string or Azure AD.
// The returned connector can be used with sql.OpenDB.
func (m *SQLServerAuthMetadata) GetConnector(setDatabase bool) (*mssql.Connector, bool, error) {
	// Parse the connection string
	config, err := msdsn.Parse(m.ConnectionString)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// If setDatabase is true and the configuration (i.e. the connection string) does not contain a database, add it
	// This is for backwards-compatibility reasons
	if setDatabase && config.Database == "" {
		config.Database = m.DatabaseName
	}

	// We need to check if the configuration has a database because the migrator needs it
	hasDatabase := config.Database != ""

	// Configure Azure AD authentication if needed
	if m.UseAzureAD {
		tokenCred, errToken := m.azureEnv.GetTokenCredential()
		if errToken != nil {
			return nil, false, errToken
		}

		conn, err := mssql.NewSecurityTokenConnector(config, func(ctx context.Context) (string, error) {
			at, err := tokenCred.GetToken(ctx, policy.TokenRequestOptions{
				Scopes: []string{
					m.azureEnv.Cloud.Services[azure.ServiceAzureSQL].Audience,
				},
			})
			if err != nil {
				return "", err
			}
			return at.Token, nil
		})
		return conn, hasDatabase, err
	}

	conn := mssql.NewConnectorConfig(config)
	return conn, hasDatabase, nil
}

func IsLetterOrNumber(c rune) bool {
	return unicode.IsNumber(c) || unicode.IsLetter(c)
}

func IsValidSQLName(s string) bool {
	for _, c := range s {
		if !(IsLetterOrNumber(c) || (c == '_')) {
			return false
		}
	}

	return true
}
