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
	"sync"
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

type StaticAuth struct {
	mu     sync.RWMutex
	logger logger.Logger

	region       *string
	endpoint     *string
	accessKey    *string
	secretKey    *string
	sessionToken string

	assumeRoleARN *string
	sessionName   string

	session *session.Session
	cfg     *aws.Config
	clients *Clients
}

func newStaticIAM(_ context.Context, opts Options, cfg *aws.Config) (*StaticAuth, error) {
	auth := &StaticAuth{
		logger: opts.Logger,
		cfg: func() *aws.Config {
			// if nil is passed or it's just a default cfg,
			// then we use the options to build the aws cfg.
			if cfg != nil && cfg != aws.NewConfig() {
				return cfg
			}
			return GetConfig(opts)
		}(),
		clients: newClients(),
	}

	if opts.Region != "" {
		auth.region = &opts.Region
	}
	if opts.Endpoint != "" {
		auth.endpoint = &opts.Endpoint
	}
	if opts.AccessKey != "" {
		auth.accessKey = &opts.AccessKey
	}
	if opts.SecretKey != "" {
		auth.secretKey = &opts.SecretKey
	}
	if opts.SessionToken != "" {
		auth.sessionToken = opts.SessionToken
	}
	if opts.AssumeRoleARN != "" {
		auth.assumeRoleARN = &opts.AssumeRoleARN
	}
	if opts.SessionName != "" {
		auth.sessionName = opts.SessionName
	}

	initialSession, err := auth.createSession()
	if err != nil {
		return nil, fmt.Errorf("failed to get token client: %v", err)
	}

	auth.session = initialSession

	return auth, nil
}

// This is to be used only for test purposes to inject mocked clients
func (a *StaticAuth) WithMockClients(clients *Clients) {
	a.clients = clients
}

func (a *StaticAuth) S3() *S3Clients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.s3 != nil {
		return a.clients.s3
	}

	s3Clients := S3Clients{}
	a.clients.s3 = &s3Clients
	a.clients.s3.New(a.session)
	return a.clients.s3
}

func (a *StaticAuth) DynamoDB() *DynamoDBClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.Dynamo != nil {
		return a.clients.Dynamo
	}

	clients := DynamoDBClients{}
	a.clients.Dynamo = &clients
	a.clients.Dynamo.New(a.session)

	return a.clients.Dynamo
}

func (a *StaticAuth) Sqs() *SqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.sqs != nil {
		return a.clients.sqs
	}

	clients := SqsClients{}
	a.clients.sqs = &clients
	a.clients.sqs.New(a.session)

	return a.clients.sqs
}

func (a *StaticAuth) Sns() *SnsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.sns != nil {
		return a.clients.sns
	}

	clients := SnsClients{}
	a.clients.sns = &clients
	a.clients.sns.New(a.session)
	return a.clients.sns
}

func (a *StaticAuth) SnsSqs() *SnsSqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.snssqs != nil {
		return a.clients.snssqs
	}

	clients := SnsSqsClients{}
	a.clients.snssqs = &clients
	a.clients.snssqs.New(a.session)
	return a.clients.snssqs
}

func (a *StaticAuth) SecretManager() *SecretManagerClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.Secret != nil {
		return a.clients.Secret
	}

	clients := SecretManagerClients{}
	a.clients.Secret = &clients
	a.clients.Secret.New(a.session)
	return a.clients.Secret
}

func (a *StaticAuth) ParameterStore() *ParameterStoreClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.ParameterStore != nil {
		return a.clients.ParameterStore
	}

	clients := ParameterStoreClients{}
	a.clients.ParameterStore = &clients
	a.clients.ParameterStore.New(a.session)
	return a.clients.ParameterStore
}

func (a *StaticAuth) Kinesis() *KinesisClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.kinesis != nil {
		return a.clients.kinesis
	}

	clients := KinesisClients{}
	a.clients.kinesis = &clients
	a.clients.kinesis.New(a.session)
	return a.clients.kinesis
}

func (a *StaticAuth) Ses() *SesClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.ses != nil {
		return a.clients.ses
	}

	clients := SesClients{}
	a.clients.ses = &clients
	a.clients.ses.New(a.session)
	return a.clients.ses
}

func (a *StaticAuth) UpdatePostgres(ctx context.Context, poolConfig *pgxpool.Config) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Set max connection lifetime to 8 minutes in postgres connection pool configuration.
	// Note: this will refresh connections before the 15 min expiration on the IAM AWS auth token,
	// while leveraging the BeforeConnect hook to recreate the token in time dynamically.
	poolConfig.MaxConnLifetime = time.Minute * 8

	// Setup connection pool config needed for AWS IAM authentication
	poolConfig.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {
		// Manually reset auth token with aws and reset the config password using the new iam token
		pwd, err := a.getDatabaseToken(ctx, poolConfig)
		if err != nil {
			return fmt.Errorf("failed to get database token: %w", err)
		}
		pgConfig.Password = pwd
		poolConfig.ConnConfig.Password = pwd

		return nil
	}
}

// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html
func (a *StaticAuth) getDatabaseToken(ctx context.Context, poolConfig *pgxpool.Config) (string, error) {
	dbEndpoint := poolConfig.ConnConfig.Host + ":" + strconv.Itoa(int(poolConfig.ConnConfig.Port))

	// First, check if there are credentials set explicitly with accesskey and secretkey
	if a.accessKey != nil && a.secretKey != nil {
		awsCfg := v2creds.NewStaticCredentialsProvider(*a.accessKey, *a.secretKey, a.sessionToken)
		authenticationToken, err := auth.BuildAuthToken(
			ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}

		return authenticationToken, nil
	}

	// Second, check if we are assuming a role instead
	if a.assumeRoleARN != nil {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
		}
		stsClient := sts.NewFromConfig(awsCfg)

		assumeRoleCfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(*a.region),
			config.WithCredentialsProvider(
				awsv2.NewCredentialsCache(
					stscreds.NewAssumeRoleProvider(stsClient, *a.assumeRoleARN, func(aro *stscreds.AssumeRoleOptions) {
						if a.sessionName != "" {
							aro.RoleSessionName = a.sessionName
						}
					}),
				),
			),
		)
		if err != nil {
			return "", fmt.Errorf("failed to assume aws role %w", err)
		}

		authenticationToken, err := auth.BuildAuthToken(
			ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, assumeRoleCfg.Credentials)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}
		return authenticationToken, nil
	}

	// Lastly, and by default, just use the default aws configuration
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
	}

	authenticationToken, err := auth.BuildAuthToken(ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
	}

	return authenticationToken, nil
}

func (a *StaticAuth) createSession() (*session.Session, error) {
	var awsConfig *aws.Config
	if a.cfg == nil {
		awsConfig = aws.NewConfig()
	} else {
		awsConfig = a.cfg
	}

	if a.region != nil {
		awsConfig = awsConfig.WithRegion(*a.region)
	}

	if a.accessKey != nil && a.secretKey != nil {
		// session token is an option field
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(*a.accessKey, *a.secretKey, a.sessionToken))
	}

	if a.endpoint != nil {
		awsConfig = awsConfig.WithEndpoint(*a.endpoint)
	}

	// TODO support assume role for all aws components

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

func (a *StaticAuth) Close() error {
	return nil
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
