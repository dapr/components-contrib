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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/common/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
)

// PostgresAuthMetadata contains authentication metadata for PostgreSQL components.
type PostgresAuthMetadata struct {
	ConnectionString      string        `mapstructure:"connectionString" mapstructurealiases:"url"`
	ConnectionMaxIdleTime time.Duration `mapstructure:"connectionMaxIdleTime"`
	MaxConns              int           `mapstructure:"maxConns"`
	UseAzureAD            bool          `mapstructure:"useAzureAD"`
	UseAWSIAM             bool          `mapstructure:"useAWSIAM"`
	QueryExecMode         string        `mapstructure:"queryExecMode"`

	azureEnv azure.EnvironmentSettings
	awsEnv   aws.EnvironmentSettings
}

// Reset the object.
func (m *PostgresAuthMetadata) Reset() {
	m.ConnectionString = ""
	m.ConnectionMaxIdleTime = 0
	m.MaxConns = 0
	m.UseAzureAD = false
	m.UseAWSIAM = false
	m.QueryExecMode = ""
}

type InitWithMetadataOpts struct {
	AzureADEnabled bool
	AWSIAMEnabled  bool
}

// InitWithMetadata inits the object with metadata from the user.
// Set azureADEnabled to true if the component can support authentication with Azure AD.
// This is different from the "useAzureAD" property from the user, which is provided by the user and instructs the component to authenticate using Azure AD.
func (m *PostgresAuthMetadata) InitWithMetadata(meta map[string]string, opts InitWithMetadataOpts) (err error) {
	// Validate input
	if m.ConnectionString == "" {
		return errors.New("missing connection string")
	}
	switch {
	case opts.AzureADEnabled && m.UseAzureAD:
		// Populate the Azure environment if using Azure AD
		m.azureEnv, err = azure.NewEnvironmentSettings(meta)
		if err != nil {
			return err
		}
	case opts.AWSIAMEnabled && m.UseAWSIAM:
		// Populate the AWS environment if using AWS IAM
		m.awsEnv, err = aws.NewEnvironmentSettings(meta)
		if err != nil {
			return err
		}
	default:
		// Make sure these are false
		m.UseAzureAD = false
		m.UseAWSIAM = false
	}

	return nil
}

func (m *PostgresAuthMetadata) ValidateAwsIamFields() (string, string, string, error) {
	awsRegion, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "AWSRegion")
	if awsRegion == "" {
		return "", "", "", errors.New("metadata property AWSRegion is missing")
	}
	// Note: access key and secret keys can be optional
	// in the event users are leveraging the credential files for an access token.
	awsAccessKey, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "AWSAccessKey")
	awsSecretKey, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "AWSSecretKey")
	return awsRegion, awsAccessKey, awsSecretKey, nil
}

// GetPgxPoolConfig returns the pgxpool.Config object that contains the credentials for connecting to PostgreSQL.
func (m *PostgresAuthMetadata) GetPgxPoolConfig() (*pgxpool.Config, error) {
	// Get the config from the connection string
	config, err := pgxpool.ParseConfig(m.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if m.ConnectionMaxIdleTime > 0 {
		config.MaxConnIdleTime = m.ConnectionMaxIdleTime
	}
	if m.MaxConns > 1 {
		config.MaxConns = int32(m.MaxConns) //nolint:gosec
	}

	if m.QueryExecMode != "" {
		queryExecModes := map[string]pgx.QueryExecMode{
			"cache_statement": pgx.QueryExecModeCacheStatement,
			"cache_describe":  pgx.QueryExecModeCacheDescribe,
			"describe_exec":   pgx.QueryExecModeDescribeExec,
			"exec":            pgx.QueryExecModeExec,
			"simple_protocol": pgx.QueryExecModeSimpleProtocol,
		}
		if mode, ok := queryExecModes[m.QueryExecMode]; ok {
			config.ConnConfig.DefaultQueryExecMode = mode
		} else {
			return nil, fmt.Errorf("invalid queryExecMode metadata value: %s", m.QueryExecMode)
		}
	}

	switch {
	case m.UseAzureAD:
		// Use Azure AD
		tokenCred, errToken := m.azureEnv.GetTokenCredential()
		if errToken != nil {
			return nil, errToken
		}

		// Reset the password
		config.ConnConfig.Password = ""

		// We need to retrieve the token every time we attempt a new connection
		// This is because tokens expire, and connections can drop and need to be re-established at any time
		// Fortunately, we can do this with the "BeforeConnect" hook
		config.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
			at, errGetAccessToken := tokenCred.GetToken(ctx, policy.TokenRequestOptions{
				Scopes: []string{
					m.azureEnv.Cloud.Services[azure.ServiceOSSRDBMS].Audience + "/.default",
				},
			})
			if errGetAccessToken != nil {
				return errGetAccessToken
			}

			cc.Password = at.Token
			return nil
		}
	case m.UseAWSIAM:
		// We should use AWS IAM
		awsRegion, awsAccessKey, awsSecretKey, err := m.ValidateAwsIamFields()
		if err != nil {
			err = fmt.Errorf("failed to validate AWS IAM authentication fields: %w", err)
			return nil, err
		}

		awsOpts := aws.AWSIAMAuthOptions{
			PoolConfig:       config,
			ConnectionString: m.ConnectionString,
			Region:           awsRegion,
			AccessKey:        awsAccessKey,
			SecretKey:        awsSecretKey,
		}

		err = awsOpts.InitiateAWSIAMAuth()
		if err != nil {
			err = fmt.Errorf("failed to initiate AWS IAM authentication rotation: %w", err)
			return nil, err
		}
	}

	return config, nil
}
