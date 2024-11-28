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

package aws

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/kit/logger"
)

type EnvironmentSettings struct {
	Metadata map[string]string
}

type AWSIAM struct {
	// Ignored by metadata parser because included in built-in authentication profile
	// Access key to use for accessing PostgreSQL.
	AWSAccessKey string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	// Secret key to use for accessing PostgreSQL.
	AWSSecretKey string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
	// AWS region in which PostgreSQL is deployed.
	AWSRegion string `json:"awsRegion" mapstructure:"awsRegion"`
}

// TODO: Delete in Dapr 1.17 so we can move all IAM fields to use the defaults of:
// accessKey and secretKey and region as noted in the docs, and Options struct above.
type DeprecatedKafkaIAM struct {
	Region         string `json:"awsRegion" mapstructure:"awsRegion"`
	AccessKey      string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	SecretKey      string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
	SessionToken   string `json:"awsSessionToken" mapstructure:"awsSessionToken"`
	IamRoleArn     string `json:"awsIamRoleArn" mapstructure:"awsIamRoleArn"`
	StsSessionName string `json:"awsStsSessionName" mapstructure:"awsStsSessionName"`
}

type AWSIAMAuthOptions struct {
	PoolConfig       *pgxpool.Config `json:"poolConfig" mapstructure:"poolConfig"`
	ConnectionString string          `json:"connectionString" mapstructure:"connectionString"`
	Region           string          `json:"region" mapstructure:"region"`
	AccessKey        string          `json:"accessKey" mapstructure:"accessKey"`
	SecretKey        string          `json:"secretKey" mapstructure:"secretKey"`
}

type Options struct {
	Logger     logger.Logger
	Properties map[string]string

	PoolConfig       *pgxpool.Config `json:"poolConfig" mapstructure:"poolConfig"`
	ConnectionString string          `json:"connectionString" mapstructure:"connectionString"`

	// TODO: in Dapr 1.17 rm the alias on regions as we rm the aws prefixed one.
	// Docs have it just as region, but most metadata fields show the aws prefix...
	Region        string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion"`
	AccessKey     string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey     string `json:"secretKey" mapstructure:"secretKey"`
	SessionName   string `mapstructure:"sessionName"`
	AssumeRoleARN string `mapstructure:"assumeRoleArn"`

	Endpoint     string
	SessionToken string
}

func GetConfig(opts Options) *aws.Config {
	cfg := aws.NewConfig()

	switch {
	case opts.Region != "":
		cfg.WithRegion(opts.Region)
	case opts.Endpoint != "":
		cfg.WithEndpoint(opts.Endpoint)
	}

	return cfg
}

//nolint:interfacebloat
type Provider interface {
	S3() *S3Clients
	DynamoDB() *DynamoDBClients
	Sqs() *SqsClients
	Sns() *SnsClients
	SnsSqs() *SnsSqsClients
	SecretManager() *SecretManagerClients
	ParameterStore() *ParameterStoreClients
	Kinesis() *KinesisClients
	Ses() *SesClients

	Kafka(KafkaOptions) (*KafkaClients, error)

	Close() error
}

func NewProvider(ctx context.Context, opts Options, cfg *aws.Config) (Provider, error) {
	if isX509Auth(opts.Properties) {
		return newX509(ctx, opts, cfg)
	}
	return newStaticIAM(ctx, opts, cfg)
}

// NewEnvironmentSettings returns a new EnvironmentSettings configured for a given AWS resource.
func NewEnvironmentSettings(md map[string]string) (EnvironmentSettings, error) {
	es := EnvironmentSettings{
		Metadata: md,
	}

	return es, nil
}

func (opts *Options) GetAccessToken(ctx context.Context) (string, error) {
	dbEndpoint := opts.PoolConfig.ConnConfig.Host + ":" + strconv.Itoa(int(opts.PoolConfig.ConnConfig.Port))
	var authenticationToken string

	// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html
	// Default to load default config through aws credentials file (~/.aws/credentials)
	awsCfg, err := config.LoadDefaultConfig(ctx)
	// Note: in the event of an error with invalid config or failed to load config,
	// then we fall back to using the access key and secret key.
	switch {
	case errors.Is(err, config.SharedConfigAssumeRoleError{}.Err),
		errors.Is(err, config.SharedConfigLoadError{}.Err),
		errors.Is(err, config.SharedConfigProfileNotExistError{}.Err):
		// Validate if access key and secret access key are provided
		if opts.AccessKey == "" || opts.SecretKey == "" {
			return "", fmt.Errorf("failed to load default configuration for AWS using accessKey and secretKey: %w", err)
		}

		// Set credentials explicitly
		awsCfg := v2creds.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey, "")
		authenticationToken, err = auth.BuildAuthToken(
			ctx, dbEndpoint, opts.Region, opts.PoolConfig.ConnConfig.User, awsCfg)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}

		return authenticationToken, nil
	case err != nil:
		return "", errors.New("failed to load default AWS authentication configuration")
	}

	authenticationToken, err = auth.BuildAuthToken(
		ctx, dbEndpoint, opts.Region, opts.PoolConfig.ConnConfig.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
	}

	return authenticationToken, nil
}

func (opts *Options) InitiateAWSIAMAuth() error {
	// Set max connection lifetime to 8 minutes in postgres connection pool configuration.
	// Note: this will refresh connections before the 15 min expiration on the IAM AWS auth token,
	// while leveraging the BeforeConnect hook to recreate the token in time dynamically.
	opts.PoolConfig.MaxConnLifetime = time.Minute * 8

	// Setup connection pool config needed for AWS IAM authentication
	opts.PoolConfig.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {
		// Manually reset auth token with aws and reset the config password using the new iam token
		pwd, errGetAccessToken := opts.GetAccessToken(ctx)
		if errGetAccessToken != nil {
			return fmt.Errorf("failed to refresh access token for iam authentication with PostgreSQL: %w", errGetAccessToken)
		}

		pgConfig.Password = pwd
		opts.PoolConfig.ConnConfig.Password = pwd

		return nil
	}

	return nil
}

// Coalesce is a helper function to return the first non-empty string from the inputs
// This helps us to migrate away from the deprecated duplicate aws auth profile metadata fields in Dapr 1.17.
func Coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
