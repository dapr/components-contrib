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
	logger logger.Logger

	region       string
	endpoint     *string
	accessKey    *string
	secretKey    *string
	sessionToken *string

	clients *Clients
	session *session.Session
	cfg     *aws.Config
}

func (a *StaticAuth) Initialize(_ context.Context, opts Options, cfg *aws.Config) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.logger = opts.Logger
	a.region = opts.Region
	a.endpoint = &opts.Endpoint
	a.accessKey = &opts.AccessKey
	a.secretKey = &opts.SecretKey
	a.sessionToken = &opts.SessionToken
	a.cfg = cfg
	a.clients = newClients()

	initialSession, err := a.getTokenClient()
	if err != nil {
		return fmt.Errorf("failed to get token client: %v", err)
	}

	a.session = initialSession

	return nil
}

func (a *StaticAuth) S3(ctx context.Context) *S3Clients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.s3 == nil {
		s3Clients := S3Clients{}
		a.clients.s3 = &s3Clients
		a.clients.s3.New(a.session)
	}

	return a.clients.s3
}

func (a *StaticAuth) DynamoDB(ctx context.Context) *DynamoDBClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.dynamo == nil {
		clients := DynamoDBClients{}
		a.clients.dynamo = &clients
		a.clients.dynamo.New(a.session)
	}

	return a.clients.dynamo
}

func (a *StaticAuth) DynamoDBI(ctx context.Context) *DynamoDBClientsI {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.dynamoI == nil {
		clients := DynamoDBClientsI{}
		a.clients.dynamoI = &clients
		a.clients.dynamoI.New(a.session)
	}

	return a.clients.dynamoI
}

func (a *StaticAuth) Sqs(ctx context.Context) *SqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.sqs == nil {
		clients := SqsClients{}
		a.clients.sqs = &clients
		a.clients.sqs.New(a.session)
	}

	return a.clients.sqs
}

func (a *StaticAuth) Sns(ctx context.Context) *SnsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.sns == nil {
		clients := SnsClients{}
		a.clients.sns = &clients
		a.clients.sns.New(a.session)
	}

	return a.clients.sns
}

func (a *StaticAuth) SnsSqs(ctx context.Context) *SnsSqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.snssqs == nil {
		clients := SnsSqsClients{}
		a.clients.snssqs = &clients
		a.clients.snssqs.New(a.session)
	}

	return a.clients.snssqs
}

func (a *StaticAuth) SecretManager(ctx context.Context) *SecretManagerClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.secret == nil {
		clients := SecretManagerClients{}
		a.clients.secret = &clients
		a.clients.secret.New(a.session)
	}

	return a.clients.secret
}

func (a *StaticAuth) ParameterStore(ctx context.Context) *ParameterStoreClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.parameterStore == nil {
		clients := ParameterStoreClients{}
		a.clients.parameterStore = &clients
		a.clients.parameterStore.New(a.session)
	}

	return a.clients.parameterStore
}

func (a *StaticAuth) Kinesis(ctx context.Context) *KinesisClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.kinesis == nil {
		clients := KinesisClients{}
		a.clients.kinesis = &clients
		a.clients.kinesis.New(a.session)
	}

	return a.clients.kinesis
}

func (a *StaticAuth) Ses(ctx context.Context) *SesClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.ses == nil {
		clients := SesClients{}
		a.clients.ses = &clients
		a.clients.ses.New(a.session)
	}

	return a.clients.ses
}

func (a *StaticAuth) getTokenClient() (*session.Session, error) {
	awsConfig := aws.NewConfig()

	if a.region != "" {
		awsConfig = awsConfig.WithRegion(a.region)
	}

	if a.accessKey != nil && a.secretKey != nil {
		// session token is an option field
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(*a.accessKey, *a.secretKey, *a.sessionToken))
	}

	if a.endpoint != nil {
		awsConfig = awsConfig.WithEndpoint(*a.endpoint)
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
