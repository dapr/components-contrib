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
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/components-contrib/common/authentication/azure"
	awsAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// PostgresAuthMetadata contains authentication metadata for PostgreSQL components.
type PostgresAuthMetadata struct {
	ConnectionString      string        `mapstructure:"connectionString" mapstructurealiases:"url"`
	Host                  string        `mapstructure:"host"`
	HostAddr              string        `mapstructure:"hostaddr"`
	Port                  string        `mapstructure:"port"`
	Database              string        `mapstructure:"database"`
	User                  string        `mapstructure:"user"`
	Password              string        `mapstructure:"password"`
	SslRootCert           string        `mapstructure:"sslRootCert"`
	ConnectionMaxIdleTime time.Duration `mapstructure:"connectionMaxIdleTime"`
	MaxConns              int           `mapstructure:"maxConns"`
	UseAzureAD            bool          `mapstructure:"useAzureAD"`
	UseAWSIAM             bool          `mapstructure:"useAWSIAM"`
	QueryExecMode         string        `mapstructure:"queryExecMode"`

	azureEnv azure.EnvironmentSettings
	awsEnv   awsAuth.EnvironmentSettings
}

// Reset the object.
func (m *PostgresAuthMetadata) Reset() {
	m.ConnectionString = ""
	m.Host = ""
	m.HostAddr = ""
	m.Port = ""
	m.Database = ""
	m.User = ""
	m.Password = ""
	m.SslRootCert = ""
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
	_, err = m.buildConnectionString()
	if err != nil {
		return err
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
		m.awsEnv, err = awsAuth.NewEnvironmentSettings(meta)
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

// buildConnectionString builds the connection string from the metadata.
// It supports both DSN-style and URL-style connection strings.
// Metadata fields override existing values in the connection string.
func (m *PostgresAuthMetadata) buildConnectionString() (string, error) {
	metadata := m.getConnectionStringMetadata()
	if strings.HasPrefix(m.ConnectionString, "postgres://") || strings.HasPrefix(m.ConnectionString, "postgresql://") {
		return m.buildURLConnectionString(metadata)
	}
	return m.buildDSNConnectionString(metadata)
}

func (m *PostgresAuthMetadata) buildDSNConnectionString(metadata map[string]string) (string, error) {
	connectionString := ""
	parts := strings.Split(m.ConnectionString, " ")
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			key := kv[0]
			if value, ok := metadata[key]; ok {
				connectionString += fmt.Sprintf("%s=%s ", key, value)
				delete(metadata, key)
			} else {
				connectionString += fmt.Sprintf("%s=%s ", key, kv[1])
			}
		}
	}
	for k, v := range metadata {
		connectionString += fmt.Sprintf("%s=%s ", k, v)
	}

	if connectionString == "" {
		return "", errors.New("failed to build connection string")
	}

	return strings.TrimSpace(connectionString), nil
}

func (m *PostgresAuthMetadata) getConnectionStringMetadata() map[string]string {
	metadata := make(map[string]string)
	if m.User != "" {
		metadata["user"] = m.User
	}
	if m.Host != "" {
		metadata["host"] = m.Host
	}
	if m.HostAddr != "" {
		metadata["hostaddr"] = m.HostAddr
	}
	if m.Port != "" {
		metadata["port"] = m.Port
	}
	if m.Database != "" {
		metadata["database"] = m.Database
	}
	if m.Password != "" {
		metadata["password"] = m.Password
	}
	if m.SslRootCert != "" {
		metadata["sslrootcert"] = m.SslRootCert
	}
	return metadata
}

func (m *PostgresAuthMetadata) buildURLConnectionString(metadata map[string]string) (string, error) {
	u, err := url.Parse(m.ConnectionString)
	if err != nil {
		return "", fmt.Errorf("invalid URL connection string: %w", err)
	}

	var username string
	var password string
	if u.User != nil {
		username = u.User.Username()
		pw, set := u.User.Password()
		if set {
			password = pw
		}
	}

	if val, ok := metadata["user"]; ok {
		username = val
	}
	if val, ok := metadata["password"]; ok {
		password = val
	}
	if username != "" {
		u.User = url.UserPassword(username, password)
	}

	if val, ok := metadata["host"]; ok {
		u.Host = val
	}
	if val, ok := metadata["hostaddr"]; ok {
		u.Host = val
	}
	if m.Port != "" {
		u.Host = fmt.Sprintf("%s:%s", u.Host, m.Port)
	}

	if val, ok := metadata["database"]; ok {
		u.Path = "/" + strings.TrimPrefix(val, "/")
	}

	q := u.Query()
	if val, ok := metadata["sslrootcert"]; ok {
		q.Set("sslrootcert", val)
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func (m *PostgresAuthMetadata) BuildAwsIamOptions(logger logger.Logger, properties map[string]string) (*awsAuth.Options, error) {
	awsRegion, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "AWSRegion")
	region, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "region")
	if region == "" {
		region = awsRegion
	}
	if region == "" {
		return nil, errors.New("metadata properties 'region' or 'AWSRegion' is missing")
	}

	// Note: access key and secret keys can be optional
	// in the event users are leveraging the credential files for an access token.
	awsAccessKey, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "AWSAccessKey")
	// This is needed as we remove the awsAccessKey field to use the builtin AWS profile 'accessKey' field instead.
	accessKey, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "AccessKey")
	if awsAccessKey == "" || accessKey != "" {
		awsAccessKey = accessKey
	}
	awsSecretKey, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "AWSSecretKey")
	// This is needed as we remove the awsSecretKey field to use the builtin AWS profile 'secretKey' field instead.
	secretKey, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "SecretKey")
	if awsSecretKey == "" || secretKey != "" {
		awsSecretKey = secretKey
	}
	sessionToken, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "sessionToken")
	assumeRoleArn, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "assumeRoleArn")
	sessionName, _ := metadata.GetMetadataProperty(m.awsEnv.Metadata, "sessionName")
	if sessionName == "" {
		sessionName = "DaprDefaultSession"
	}
	return &awsAuth.Options{
		Region:                region,
		AccessKey:             awsAccessKey,
		SecretKey:             awsSecretKey,
		SessionToken:          sessionToken,
		AssumeRoleArn:         assumeRoleArn,
		AssumeRoleSessionName: sessionName,

		Logger:     logger,
		Properties: properties,
	}, nil
}

// GetPgxPoolConfig returns the pgxpool.Config object that contains the credentials for connecting to PostgreSQL.
func (m *PostgresAuthMetadata) GetPgxPoolConfig() (*pgxpool.Config, error) {
	connectionString, err := m.buildConnectionString()
	if err != nil {
		return nil, err
	}
	config, err := pgxpool.ParseConfig(connectionString)
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
	}

	return config, nil
}
