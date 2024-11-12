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

	Clients *Clients
	session *session.Session
	cfg     *aws.Config
}

func newStaticIAM(ctx context.Context, opts Options, cfg *aws.Config) (*StaticAuth, error) {
	auth := &StaticAuth{
		logger:       opts.Logger,
		region:       opts.Region,
		endpoint:     &opts.Endpoint,
		accessKey:    &opts.AccessKey,
		secretKey:    &opts.SecretKey,
		sessionToken: &opts.SessionToken,
		cfg:          cfg,
		Clients:      newClients(),
	}

	initialSession, err := auth.getTokenClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get token client: %v", err)
	}

	auth.session = initialSession

	return auth, nil
}

func (a *StaticAuth) S3(ctx context.Context) (*S3Clients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.s3 != nil {
		return a.Clients.s3, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		s3Clients := S3Clients{}
		a.Clients.s3 = &s3Clients
		a.logger.Debugf("Initializing S3 clients with session %v", a.session)
		a.Clients.s3.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.s3, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) DynamoDB(ctx context.Context) (*DynamoDBClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.Dynamo != nil {
		return a.Clients.Dynamo, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := DynamoDBClients{}
		a.Clients.Dynamo = &clients
		a.Clients.Dynamo.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.Dynamo, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) Sqs(ctx context.Context) (*SqsClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.sqs != nil {
		return a.Clients.sqs, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := SqsClients{}
		a.Clients.sqs = &clients
		a.Clients.sqs.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.sqs, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) Sns(ctx context.Context) (*SnsClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.sns != nil {
		return a.Clients.sns, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := SnsClients{}
		a.Clients.sns = &clients
		a.Clients.sns.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.sns, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) SnsSqs(ctx context.Context) (*SnsSqsClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.snssqs != nil {
		return a.Clients.snssqs, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := SnsSqsClients{}
		a.Clients.snssqs = &clients
		a.Clients.snssqs.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.snssqs, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) SecretManager(ctx context.Context) (*SecretManagerClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.Secret != nil {
		return a.Clients.Secret, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := SecretManagerClients{}
		a.Clients.Secret = &clients
		a.Clients.Secret.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.Secret, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) ParameterStore(ctx context.Context) (*ParameterStoreClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.ParameterStore != nil {
		return a.Clients.ParameterStore, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := ParameterStoreClients{}
		a.Clients.ParameterStore = &clients
		a.Clients.ParameterStore.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.ParameterStore, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) Kinesis(ctx context.Context) (*KinesisClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.kinesis != nil {
		return a.Clients.kinesis, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := KinesisClients{}
		a.Clients.kinesis = &clients
		a.Clients.kinesis.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.kinesis, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *StaticAuth) Ses(ctx context.Context) (*SesClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.ses != nil {
		return a.Clients.ses, nil
	}

	// respect context cancellation while initializing client
	done := make(chan struct{})
	go func() {
		defer close(done)
		clients := SesClients{}
		a.Clients.ses = &clients
		a.Clients.ses.New(a.session)
	}()

	// wait for new client or context to be canceled
	select {
	case <-done:
		return a.Clients.ses, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
