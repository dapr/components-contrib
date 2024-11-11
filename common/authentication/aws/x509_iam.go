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
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	awssh "github.com/aws/rolesanywhere-credential-helper/aws_signing_helper"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere"
	cryptopem "github.com/dapr/kit/crypto/pem"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

type x509TempAuth struct {
	mu      sync.RWMutex
	logger  logger.Logger
	clients *Clients
	session *session.Session
	cfg     *aws.Config

	chainPEM []byte
	keyPEM   []byte

	region          *string
	TrustProfileArn *string        `json:"trustProfileArn" mapstructure:"trustProfileArn"`
	TrustAnchorArn  *string        `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	AssumeRoleArn   *string        `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
	SessionDuration *time.Duration `json:"sessionDuration" mapstructure:"sessionDuration"`
}

func (a *x509TempAuth) Initialize(ctx context.Context, opts Options, cfg *aws.Config) error {
	var x509Auth x509TempAuth
	if err := kitmd.DecodeMetadata(opts.Properties, &x509Auth); err != nil {
		return err
	}

	switch {
	case x509Auth.TrustProfileArn == nil:
		return errors.New("trustProfileArn is required")
	case x509Auth.TrustAnchorArn == nil:
		return errors.New("trustAnchorArn is required")
	case x509Auth.AssumeRoleArn == nil:
		return errors.New("assumeRoleArn is required")
	case x509Auth.SessionDuration == nil:
		awsDefaultDuration := time.Duration(900) // default 15m
		x509Auth.SessionDuration = &awsDefaultDuration
	}

	a.logger = opts.Logger
	a.TrustProfileArn = x509Auth.TrustProfileArn
	a.TrustAnchorArn = x509Auth.TrustAnchorArn
	a.AssumeRoleArn = x509Auth.AssumeRoleArn
	a.SessionDuration = x509Auth.SessionDuration
	a.cfg = GetConfig(opts)
	a.clients = newClients()

	err := a.getCertPEM(ctx)
	if err != nil {
		return fmt.Errorf("failed to get x.509 credentials: %v", err)
	}

	// Parse trust anchor and profile ARNs
	if err := a.initializeTrustAnchors(); err != nil {
		return err
	}

	initialSession, err := a.createOrRefreshSession(ctx)
	if err != nil {
		return fmt.Errorf("failed to create the initial session: %v", err)
	}
	a.session = initialSession
	go a.startSessionRefresher(context.Background())

	return nil
}

func (a *x509TempAuth) Close() error {
	return nil
}

func (a *x509TempAuth) getCertPEM(ctx context.Context) error {
	// retrieve svid from spiffe context
	svid, ok := spiffecontext.From(ctx)
	if !ok {
		return fmt.Errorf("no SVID found in context")
	}
	// get x.509 svid
	svidx, err := svid.GetX509SVID()
	if err != nil {
		return err
	}

	// marshal x.509 svid to pem format
	chainPEM, keyPEM, err := svidx.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal SVID: %w", err)
	}

	a.chainPEM = chainPEM
	a.keyPEM = keyPEM
	return nil
}

func (a *x509TempAuth) S3(ctx context.Context) *S3Clients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.s3 == nil {
		s3Clients := S3Clients{}
		a.clients.s3 = &s3Clients
		a.clients.s3.New(a.session)
	}

	return a.clients.s3
}

func (a *x509TempAuth) DynamoDB(ctx context.Context) *DynamoDBClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.dynamo == nil {
		clients := DynamoDBClients{}
		a.clients.dynamo = &clients
		a.clients.dynamo.New(a.session)
	}

	return a.clients.dynamo
}

func (a *x509TempAuth) DynamoDBI(ctx context.Context) *DynamoDBClientsI {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.dynamoI == nil {
		clients := DynamoDBClientsI{}
		a.clients.dynamoI = &clients
		a.clients.dynamoI.New(a.session)
	}

	return a.clients.dynamoI
}

func (a *x509TempAuth) Sqs(ctx context.Context) *SqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.sqs == nil {
		clients := SqsClients{}
		a.clients.sqs = &clients
		a.clients.sqs.New(a.session)
	}

	return a.clients.sqs
}

func (a *x509TempAuth) Sns(ctx context.Context) *SnsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.sns == nil {
		clients := SnsClients{}
		a.clients.sns = &clients
		a.clients.sns.New(a.session)
	}

	return a.clients.sns
}

func (a *x509TempAuth) SnsSqs(ctx context.Context) *SnsSqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.snssqs == nil {
		clients := SnsSqsClients{}
		a.clients.snssqs = &clients
		a.clients.snssqs.New(a.session)
	}

	return a.clients.snssqs
}

func (a *x509TempAuth) SecretManager(ctx context.Context) *SecretManagerClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.secret == nil {
		clients := SecretManagerClients{}
		a.clients.secret = &clients
		a.clients.secret.New(a.session)
	}

	return a.clients.secret
}

func (a *x509TempAuth) ParameterStore(ctx context.Context) *ParameterStoreClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.parameterStore == nil {
		clients := ParameterStoreClients{}
		a.clients.parameterStore = &clients
		a.clients.parameterStore.New(a.session)
	}

	return a.clients.parameterStore
}

func (a *x509TempAuth) Kinesis(ctx context.Context) *KinesisClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.kinesis == nil {
		clients := KinesisClients{}
		a.clients.kinesis = &clients
		a.clients.kinesis.New(a.session)
	}

	return a.clients.kinesis
}

func (a *x509TempAuth) Ses(ctx context.Context) *SesClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.ses == nil {
		clients := SesClients{}
		a.clients.ses = &clients
		a.clients.ses.New(a.session)
	}

	return a.clients.ses
}

func (a *x509TempAuth) initializeTrustAnchors() error {
	var (
		trustAnchor arn.ARN
		profile     arn.ARN
		err         error
	)
	if a.TrustAnchorArn != nil {
		trustAnchor, err = arn.Parse(*a.TrustAnchorArn)
		if err != nil {
			return err
		}
		a.region = &trustAnchor.Region
	}

	if a.TrustProfileArn != nil {
		profile, err = arn.Parse(*a.TrustProfileArn)
		if err != nil {
			return err
		}

		if profile.Region != "" && trustAnchor.Region != profile.Region {
			return fmt.Errorf("trust anchor and profile must be in the same region: trustAnchor=%s, profile=%s",
				trustAnchor.Region, profile.Region)
		}
	}
	return nil
}

func (a *x509TempAuth) setSigningFunction(rolesAnywhereClient *rolesanywhere.RolesAnywhere) error {
	certs, err := cryptopem.DecodePEMCertificatesChain(a.chainPEM)
	if err != nil {
		return err
	}

	var ints []x509.Certificate
	for i := range certs[1:] {
		ints = append(ints, *certs[i+1])
	}

	key, err := cryptopem.DecodePEMPrivateKey(a.keyPEM)
	if err != nil {
		return err
	}

	keyECDSA := key.(*ecdsa.PrivateKey)
	signFunc := awssh.CreateSignFunction(*keyECDSA, *certs[0], ints)

	agentHandlerFunc := request.MakeAddToUserAgentHandler("dapr", logger.DaprVersion, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	rolesAnywhereClient.Handlers.Build.RemoveByName("core.SDKVersionUserAgentHandler")
	rolesAnywhereClient.Handlers.Build.PushBackNamed(request.NamedHandler{Name: "v4x509.CredHelperUserAgentHandler", Fn: agentHandlerFunc})
	rolesAnywhereClient.Handlers.Sign.Clear()
	rolesAnywhereClient.Handlers.Sign.PushBackNamed(request.NamedHandler{Name: "v4x509.SignRequestHandler", Fn: signFunc})

	return nil
}

func (a *x509TempAuth) createOrRefreshSession(ctx context.Context) (*session.Session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}}
	var mySession *session.Session
	var err error

	config := a.cfg.WithRegion(*a.region).WithHTTPClient(client).WithLogLevel(aws.LogOff)
	mySession = session.Must(session.NewSession(config))
	rolesAnywhereClient := rolesanywhere.New(mySession, config)

	// Set up signing function and handlers
	if err := a.setSigningFunction(rolesAnywhereClient); err != nil {
		return nil, err
	}

	var (
		duration             int64
		createSessionRequest rolesanywhere.CreateSessionInput
	)
	if *a.SessionDuration != 0 {
		duration = int64(a.SessionDuration.Seconds())

		createSessionRequest = rolesanywhere.CreateSessionInput{
			Cert:               ptr.Of(string(a.chainPEM)),
			ProfileArn:         a.TrustProfileArn,
			TrustAnchorArn:     a.TrustAnchorArn,
			RoleArn:            a.AssumeRoleArn,
			DurationSeconds:    aws.Int64(duration),
			InstanceProperties: nil,
			SessionName:        nil,
		}
	} else {
		duration = 900 // 15 minutes in seconds by default and be autorefreshed

		createSessionRequest = rolesanywhere.CreateSessionInput{
			Cert:               ptr.Of(string(a.chainPEM)),
			ProfileArn:         a.TrustProfileArn,
			TrustAnchorArn:     a.TrustAnchorArn,
			RoleArn:            a.AssumeRoleArn,
			DurationSeconds:    aws.Int64(duration),
			InstanceProperties: nil,
			SessionName:        nil,
		}
	}

	output, err := rolesAnywhereClient.CreateSessionWithContext(ctx, &createSessionRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to create session using dapr app identity: %w", err)
	}

	if output == nil || len(output.CredentialSet) != 1 {
		return nil, fmt.Errorf("expected 1 credential set from X.509 rolesanyway response, got %d", len(output.CredentialSet))
	}

	accessKey := output.CredentialSet[0].Credentials.AccessKeyId
	secretKey := output.CredentialSet[0].Credentials.SecretAccessKey
	sessionToken := output.CredentialSet[0].Credentials.SessionToken
	awsCreds := credentials.NewStaticCredentials(*accessKey, *secretKey, *sessionToken)
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials: awsCreds,
	}, config))
	if sess == nil {
		return nil, fmt.Errorf("sam session is nil somehow %v", sess)
	}

	return sess, nil
}

func (a *x509TempAuth) startSessionRefresher(ctx context.Context) error {
	// if there is a set session duration, then exit bc we will not auto refresh the session.
	if *a.SessionDuration != 0 {
		return nil
	}

	a.logger.Debugf("starting session refresher for x509 auth")
	errChan := make(chan error, 1)
	go func() {
		// renew at ~half the lifespan
		ticker := time.NewTicker(8 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				a.logger.Infof("Refreshing session as expiration is near")
				newSession, err := a.createOrRefreshSession(ctx)
				if err != nil {
					errChan <- fmt.Errorf("failed to refresh session: %w", err)
					return
				}

				a.clients.refresh(newSession)

				a.logger.Debugf("AWS IAM Roles Anywhere session credentials refreshed successfully")
			case <-ctx.Done():
				a.logger.Debugf("Session refresher stopped due to context cancellation")
				errChan <- nil
				return
			}
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
