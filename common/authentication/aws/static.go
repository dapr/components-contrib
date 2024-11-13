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
	"sync"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/dapr/kit/logger"
)

type StaticAuth struct {
	mu     sync.RWMutex
	Logger logger.Logger

	Region       string
	Endpoint     *string
	AccessKey    *string
	SecretKey    *string
	SessionToken *string

	Clients *Clients
	Session *session.Session
	Cfg     *aws.Config
}

func newStaticIAM(_ context.Context, opts Options, cfg *aws.Config) (*StaticAuth, error) {
	auth := &StaticAuth{
		Logger:       opts.Logger,
		Region:       opts.Region,
		Endpoint:     &opts.Endpoint,
		AccessKey:    &opts.AccessKey,
		SecretKey:    &opts.SecretKey,
		SessionToken: &opts.SessionToken,
		Cfg: func() *aws.Config {
			// if nil is passed or it's just a default cfg,
			// then we use the options to build the aws cfg.
			if cfg != nil && cfg != aws.NewConfig() {
				return cfg
			}
			return GetConfig(opts)
		}(),
		Clients: newClients(),
	}

	initialSession, err := auth.getTokenClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get token client: %v", err)
	}

	auth.Session = initialSession

	return auth, nil
}

func (a *StaticAuth) S3() *S3Clients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.s3 != nil {
		return a.Clients.s3
	}

	s3Clients := S3Clients{}
	a.Clients.s3 = &s3Clients
	a.Clients.s3.New(a.Session)
	return a.Clients.s3
}

func (a *StaticAuth) DynamoDB() *DynamoDBClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.Dynamo != nil {
		return a.Clients.Dynamo
	}

	clients := DynamoDBClients{}
	a.Clients.Dynamo = &clients
	a.Clients.Dynamo.New(a.Session)

	return a.Clients.Dynamo
}

func (a *StaticAuth) Sqs() *SqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.sqs != nil {
		return a.Clients.sqs
	}

	clients := SqsClients{}
	a.Clients.sqs = &clients
	a.Clients.sqs.New(a.Session)

	return a.Clients.sqs
}

func (a *StaticAuth) Sns() *SnsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.sns != nil {
		return a.Clients.sns
	}

	clients := SnsClients{}
	a.Clients.sns = &clients
	a.Clients.sns.New(a.Session)
	return a.Clients.sns
}

func (a *StaticAuth) SnsSqs() *SnsSqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.snssqs != nil {
		return a.Clients.snssqs
	}

	clients := SnsSqsClients{}
	a.Clients.snssqs = &clients
	a.Clients.snssqs.New(a.Session)
	return a.Clients.snssqs
}

func (a *StaticAuth) SecretManager() *SecretManagerClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.Secret != nil {
		return a.Clients.Secret
	}

	clients := SecretManagerClients{}
	a.Clients.Secret = &clients
	a.Clients.Secret.New(a.Session)
	return a.Clients.Secret
}

func (a *StaticAuth) ParameterStore() *ParameterStoreClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.ParameterStore != nil {
		return a.Clients.ParameterStore
	}

	clients := ParameterStoreClients{}
	a.Clients.ParameterStore = &clients
	a.Clients.ParameterStore.New(a.Session)
	return a.Clients.ParameterStore
}

func (a *StaticAuth) Kinesis() *KinesisClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.kinesis != nil {
		return a.Clients.kinesis
	}

	clients := KinesisClients{}
	a.Clients.kinesis = &clients
	a.Clients.kinesis.New(a.Session)
	return a.Clients.kinesis
}

func (a *StaticAuth) Ses() *SesClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.ses != nil {
		return a.Clients.ses
	}

	clients := SesClients{}
	a.Clients.ses = &clients
	a.Clients.ses.New(a.Session)
	return a.Clients.ses
}

func (a *StaticAuth) getTokenClient() (*session.Session, error) {
	var awsConfig *aws.Config
	if a.Cfg == nil {
		awsConfig = aws.NewConfig()
	} else {
		awsConfig = a.Cfg
	}

	if a.Region != "" {
		awsConfig = awsConfig.WithRegion(a.Region)
	}

	if a.AccessKey != nil && a.SecretKey != nil {
		// session token is an option field
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(*a.AccessKey, *a.SecretKey, *a.SessionToken))
	}

	if a.Endpoint != nil {
		awsConfig = awsConfig.WithEndpoint(*a.Endpoint)
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
