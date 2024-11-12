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
	cryptoX509 "crypto/x509"
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
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere/rolesanywhereiface"

	cryptopem "github.com/dapr/kit/crypto/pem"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

type x509 struct {
	mu sync.RWMutex

	wg sync.WaitGroup
	// used for background session refresh logic that cannot use the context passed to the newx509 function
	internalContext       context.Context
	internalContextCancel func()

	logger              logger.Logger
	Clients             *Clients
	rolesAnywhereClient rolesanywhereiface.RolesAnywhereAPI // this is so we can mock it in tests
	session             *session.Session
	cfg                 *aws.Config

	chainPEM []byte
	keyPEM   []byte

	region          *string
	TrustProfileArn *string        `json:"trustProfileArn" mapstructure:"trustProfileArn"`
	TrustAnchorArn  *string        `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	AssumeRoleArn   *string        `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
	SessionDuration *time.Duration `json:"sessionDuration" mapstructure:"sessionDuration"`
}

func newX509(ctx context.Context, opts Options, cfg *aws.Config) (*x509, error) {
	var x509Auth x509
	if err := kitmd.DecodeMetadata(opts.Properties, &x509Auth); err != nil {
		return nil, err
	}

	switch {
	case x509Auth.TrustProfileArn == nil:
		return nil, errors.New("trustProfileArn is required")
	case x509Auth.TrustAnchorArn == nil:
		return nil, errors.New("trustAnchorArn is required")
	case x509Auth.AssumeRoleArn == nil:
		return nil, errors.New("assumeRoleArn is required")

		// https://aws.amazon.com/about-aws/whats-new/2024/03/iam-roles-anywhere-credentials-valid-12-hours/#:~:text=The%20duration%20can%20range%20from,and%20applications%2C%20to%20use%20X.
	case x509Auth.SessionDuration == nil:
		awsDefaultDuration := time.Hour // default 1 hour from AWS
		x509Auth.SessionDuration = &awsDefaultDuration
	case *x509Auth.SessionDuration != 0 && (*x509Auth.SessionDuration < time.Minute*15 || *x509Auth.SessionDuration > time.Hour*12):
		return nil, errors.New("sessionDuration must be greater than 15 minutes, and less than 12 hours")
	}

	auth := &x509{
		wg:              sync.WaitGroup{},
		logger:          opts.Logger,
		TrustProfileArn: x509Auth.TrustProfileArn,
		TrustAnchorArn:  x509Auth.TrustAnchorArn,
		AssumeRoleArn:   x509Auth.AssumeRoleArn,
		SessionDuration: x509Auth.SessionDuration,
		cfg:             GetConfig(opts),
		Clients:         newClients(),
	}

	var err error
	err = auth.getCertPEM(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get x.509 credentials: %v", err)
	}

	// Parse trust anchor and profile ARNs
	if err = auth.initializeTrustAnchors(); err != nil {
		return nil, err
	}

	initialSession, err := auth.createOrRefreshSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create the initial session: %v", err)
	}
	auth.session = initialSession

	// This is needed to keep the session refresher on the background context, but still cancellable.
	auth.internalContext, auth.internalContextCancel = context.WithCancel(context.Background())
	auth.startSessionRefresher()

	return auth, nil
}

func (a *x509) Close() error {
	if a.internalContextCancel != nil {
		a.internalContextCancel()
	}
	a.wg.Wait()
	return nil
}

func (a *x509) getCertPEM(ctx context.Context) error {
	// retrieve svid from spiffe context
	svid, ok := spiffecontext.From(ctx)
	if !ok {
		return errors.New("no SVID found in context")
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

func (a *x509) S3(ctx context.Context) (*S3Clients, error) {
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

func (a *x509) DynamoDB(ctx context.Context) (*DynamoDBClients, error) {
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

func (a *x509) Sqs(ctx context.Context) (*SqsClients, error) {
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

func (a *x509) Sns(ctx context.Context) (*SnsClients, error) {
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

func (a *x509) SnsSqs(ctx context.Context) (*SnsSqsClients, error) {
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

func (a *x509) SecretManager(ctx context.Context) (*SecretManagerClients, error) {
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

func (a *x509) ParameterStore(ctx context.Context) (*ParameterStoreClients, error) {
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

func (a *x509) Kinesis(ctx context.Context) (*KinesisClients, error) {
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

func (a *x509) Ses(ctx context.Context) (*SesClients, error) {
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

func (a *x509) initializeTrustAnchors() error {
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

func (a *x509) setSigningFunction(rolesAnywhereClient *rolesanywhere.RolesAnywhere) error {
	certs, err := cryptopem.DecodePEMCertificatesChain(a.chainPEM)
	if err != nil {
		return err
	}

	ints := make([]cryptoX509.Certificate, 0, len(certs)-1)
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

func (a *x509) createOrRefreshSession(ctx context.Context) (*session.Session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}}
	var mySession *session.Session

	var config *aws.Config
	if a.cfg != nil {
		config = a.cfg.WithRegion(*a.region).WithHTTPClient(client).WithLogLevel(aws.LogOff)
	}

	if a.rolesAnywhereClient == nil {
		mySession = session.Must(session.NewSession(config))
		rolesAnywhereClient := rolesanywhere.New(mySession, config)
		// Set up signing function and handlers
		if err := a.setSigningFunction(rolesAnywhereClient); err != nil {
			return nil, err
		}
		a.rolesAnywhereClient = rolesAnywhereClient
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
		a.logger.Infof("sam setting 15min default for session duration")
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
	a.logger.Infof("sam session time %v", *createSessionRequest.DurationSeconds)

	output, err := a.rolesAnywhereClient.CreateSessionWithContext(ctx, &createSessionRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to create session using dapr app identity: %w", err)
	}

	if output == nil || len(output.CredentialSet) != 1 {
		return nil, fmt.Errorf("expected 1 credential set from X.509 rolesanyway response, got %d", len(output.CredentialSet))
	}
	a.logger.Infof("sam successfully created new session with iam roles anywhere client!")

	accessKey := output.CredentialSet[0].Credentials.AccessKeyId
	secretKey := output.CredentialSet[0].Credentials.SecretAccessKey
	sessionToken := output.CredentialSet[0].Credentials.SessionToken

	a.logger.Infof("the ak %v sk %v st %v", accessKey, secretKey, sessionToken)
	a.logger.Infof("sam the len of credentials set %v", len(output.CredentialSet))
	awsCreds := credentials.NewStaticCredentials(*accessKey, *secretKey, *sessionToken)
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials: awsCreds,
	}, config))
	if sess == nil {
		return nil, fmt.Errorf("sam session is nil somehow %v", sess)
	}
	a.logger.Infof("sam just set session in refreshorcreate func %v", a.session)

	return sess, nil
}

func (a *x509) startSessionRefresher() {
	a.logger.Infof("starting session refresher for x509 auth")
	// if there is a set session duration, then exit bc we will not auto refresh the session.
	if *a.SessionDuration != 0 {
		a.logger.Debugf("session duration was set, so there is no authentication refreshing")
		return
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		// renew at ~half the lifespan
		expiration, err := a.session.Config.Credentials.ExpiresAt()
		if err != nil {
			a.logger.Errorf("failed to retrieve session expiration time: %w", err)
			return
		}

		timeUntilExpiration := time.Until(expiration)
		refreshInterval := timeUntilExpiration / 2
		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				a.logger.Infof("Refreshing session as expiration is near")
				newSession, err := a.createOrRefreshSession(a.internalContext)
				if err != nil {
					a.logger.Errorf("failed to refresh session: %w", err)
					return
				}
				a.logger.Infof("sam in ticker after created refreshed session %v", newSession)

				a.Clients.refresh(newSession)

				a.logger.Debugf("AWS IAM Roles Anywhere session credentials refreshed successfully")
			case <-a.internalContext.Done():
				a.logger.Debugf("Session refresher stopped due to context cancellation")
				return
			}
		}
	}()
}
