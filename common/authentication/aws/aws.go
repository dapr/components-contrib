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
	"fmt"
	"strconv"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/kit/logger"
)

type EnvironmentSettings struct {
	Metadata map[string]string
}

func GetConfigV2(accessKey string, secretKey string, sessionToken string, region string, endpoint string) (awsv2.Config, error) {
	optFns := []func(*config.LoadOptions) error{}
	if region != "" {
		optFns = append(optFns, config.WithRegion(region))
	}

	if accessKey != "" && secretKey != "" {
		provider := v2creds.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
		optFns = append(optFns, config.WithCredentialsProvider(provider))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		return awsv2.Config{}, err
	}

	if endpoint != "" {
		awsCfg.BaseEndpoint = &endpoint
	}

	return awsCfg, nil
}

func GetClient(accessKey string, secretKey string, sessionToken string, region string, endpoint string) (*session.Session, error) {
	awsConfig := aws.NewConfig()

	if region != "" {
		awsConfig = awsConfig.WithRegion(region)
	}

	if accessKey != "" && secretKey != "" {
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, sessionToken))
	}

	if endpoint != "" {
		awsConfig = awsConfig.WithEndpoint(endpoint)
	}

	awsSession, err := session.NewSessionWithOptions(session.Options{
		Config:            *awsConfig,
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}

	userAgentHandler := request.NamedHandler{
		Name: "UserAgentHandler",
		Fn:   request.MakeAddToUserAgentHandler("dapr", logger.DaprVersion),
	}
	awsSession.Handlers.Build.PushBackNamed(userAgentHandler)

	return awsSession, nil
}

// NewEnvironmentSettings returns a new EnvironmentSettings configured for a given AWS resource.
func NewEnvironmentSettings(md map[string]string) (EnvironmentSettings, error) {
	es := EnvironmentSettings{
		Metadata: md,
	}

	return es, nil
}

type AWSIAM struct {
	// Ignored by metadata parser because included in built-in authentication profile
	// Access key to use for accessing PostgreSQL.
	AWSAccessKey string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	// Secret key to use for accessing PostgreSQL.
	AWSSecretKey string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
	// Optional session token to use when using access key and secret key
	AWSSessionToken string `json:"awsSessionToken" mapstructure:"awsSessionToken"`
	// AWS region in which PostgreSQL is deployed.
	AWSRegion string `json:"awsRegion" mapstructure:"awsRegion"`
	// Optional role to assume
	AWSIamRoleArn string `json:"awsIamRoleArn" mapstructure:"awsIamRoleArn"`
	// Optional session name to use when assuming a role
	AWSStsSessionName string `json:"awsStsSessionName" mapstructure:"awsStsSessionName"`
}

type AWSIAMAuthOptions struct {
	PoolConfig       *pgxpool.Config `json:"poolConfig" mapstructure:"poolConfig"`
	ConnectionString string          `json:"connectionString" mapstructure:"connectionString"`
	Region           string          `json:"region" mapstructure:"region"`
	AccessKey        string          `json:"accessKey" mapstructure:"accessKey"`
	SecretKey        string          `json:"secretKey" mapstructure:"secretKey"`
	SessionToken     string          `json:"sessionToken" mapstructure:"sessionToken"`

	AssumeIamRoleArn         string `json:"assumeIamRoleArn" mapstructure:"assumeIamRoleArn"`
	AssumeIamRoleSessionName string `json:"assumeIamRoleSessionName" mapstructure:"assumeIamRoleSessionName"`
}

func (opts *AWSIAMAuthOptions) GetAccessToken(ctx context.Context) (string, error) {
	dbEndpoint := opts.PoolConfig.ConnConfig.Host + ":" + strconv.Itoa(int(opts.PoolConfig.ConnConfig.Port))

	// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html

	if opts.AccessKey != "" && opts.SecretKey != "" {
		// Set credentials explicitly
		awsCfg := v2creds.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey, opts.SessionToken)
		authenticationToken, err := auth.BuildAuthToken(
			ctx, dbEndpoint, opts.Region, opts.PoolConfig.ConnConfig.User, awsCfg)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}

		return authenticationToken, nil
	}

	if opts.AssumeIamRoleArn != "" {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
		}
		stsClient := sts.NewFromConfig(awsCfg)

		assumeRoleCfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(opts.Region),
			config.WithCredentialsProvider(
				awsv2.NewCredentialsCache(
					stscreds.NewAssumeRoleProvider(stsClient, opts.AssumeIamRoleArn, func(aro *stscreds.AssumeRoleOptions) {
						if opts.AssumeIamRoleSessionName != "" {
							aro.RoleSessionName = opts.AssumeIamRoleSessionName
						}
					}),
				),
			),
		)
		if err != nil {
			return "", fmt.Errorf("failed to assume aws role %w", err)
		}

		authenticationToken, err := auth.BuildAuthToken(
			ctx, dbEndpoint, opts.Region, opts.PoolConfig.ConnConfig.User, assumeRoleCfg.Credentials)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}
		return authenticationToken, nil
	}

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
	}

	authenticationToken, err := auth.BuildAuthToken(
		ctx, dbEndpoint, opts.Region, opts.PoolConfig.ConnConfig.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
	}

	return authenticationToken, nil
}

func (opts *AWSIAMAuthOptions) InitiateAWSIAMAuth() error {
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
