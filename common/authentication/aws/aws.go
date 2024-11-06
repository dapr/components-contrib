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
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

type EnvironmentSettings struct {
	Metadata map[string]string
}

type AWS struct {
	mu     sync.RWMutex
	logger logger.Logger

	x509Auth *x509Auth

	region       string
	endpoint     string
	accessKey    string
	secretKey    string
	sessionToken string
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

type Options struct {
	Logger     logger.Logger
	Properties map[string]string

	PoolConfig       *pgxpool.Config `json:"poolConfig" mapstructure:"poolConfig"`
	ConnectionString string          `json:"connectionString" mapstructure:"connectionString"`
	Region           string          `json:"region" mapstructure:"region"`
	AccessKey        string          `json:"accessKey" mapstructure:"accessKey"`
	SecretKey        string          `json:"secretKey" mapstructure:"secretKey"`
	SessionToken     string          `json:"sessionToken" mapstructure:"sessionToken"`
	Endpoint         string          `json:"endpoint" mapstructure:"endpoint"`
}

func New(opts Options) (*AWS, error) {
	var x509AuthConfig x509Auth
	if err := kitmd.DecodeMetadata(opts.Properties, &x509AuthConfig); err != nil {
		return nil, err
	}

	return &AWS{
		x509Auth:     &x509AuthConfig,
		logger:       opts.Logger,
		region:       opts.Region,
		accessKey:    opts.AccessKey,
		secretKey:    opts.SecretKey,
		sessionToken: opts.SessionToken,
		endpoint:     opts.Endpoint,
	}, nil
}

func (a *AWS) GetConfig() *aws.Config {
	cfg := aws.NewConfig()

	if a.region != "" {
		cfg.WithRegion(a.region)
	}

	return cfg
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

func (a *AWS) GetClient(ctx context.Context) (*session.Session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch {
	// IAM Roles Anywhere option
	case a.x509Auth.TrustAnchorArn != nil && a.x509Auth.AssumeRoleArn != nil:
		a.logger.Debug("using X.509 RolesAnywhere authentication using Dapr SVID")
		session, err := a.getX509Client(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create X.509 RolesAnywhere client")
		}
		// start a session refresher background goroutine to keep rotating the temporary creds
		// use background context to keep alive
		go a.startSessionRefresher(context.Background())

		return session, nil
	default:
		a.logger.Debugf("using AWS session client...")
		return a.getTokenClient()
	}
}

func (a *AWS) getTokenClient() (*session.Session, error) {
	awsConfig := aws.NewConfig()

	if a.region != "" {
		awsConfig = awsConfig.WithRegion(a.region)
	}

	if a.accessKey != "" && a.secretKey != "" {
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(a.accessKey, a.secretKey, a.sessionToken))
	}

	if a.endpoint != "" {
		awsConfig = awsConfig.WithEndpoint(a.endpoint)
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
