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

	wg      sync.WaitGroup
	closeCh chan struct{}

	logger              logger.Logger
	Clients             *Clients
	rolesAnywhereClient rolesanywhereiface.RolesAnywhereAPI // this is so we can mock it in tests
	session             *session.Session
	Cfg                 *aws.Config

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

	if x509Auth.SessionDuration == nil {
		defaultDuration := time.Hour
		x509Auth.SessionDuration = &defaultDuration
	}

	switch {
	case x509Auth.TrustProfileArn == nil:
		return nil, errors.New("trustProfileArn is required")
	case x509Auth.TrustAnchorArn == nil:
		return nil, errors.New("trustAnchorArn is required")
	case x509Auth.AssumeRoleArn == nil:
		return nil, errors.New("assumeRoleArn is required")
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
	auth.startSessionRefresher()

	return auth, nil
}

func (a *x509) Close() error {
	close(a.closeCh)
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

func (a *x509) S3() *S3Clients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.s3 != nil {
		return a.Clients.s3
	}

	s3Clients := S3Clients{}
	a.Clients.s3 = &s3Clients
	a.Clients.s3.New(a.session)
	return a.Clients.s3
}

func (a *x509) DynamoDB() *DynamoDBClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.Dynamo != nil {
		return a.Clients.Dynamo
	}

	clients := DynamoDBClients{}
	a.Clients.Dynamo = &clients
	a.Clients.Dynamo.New(a.session)

	return a.Clients.Dynamo
}

func (a *x509) Sqs() *SqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.sqs != nil {
		return a.Clients.sqs
	}

	clients := SqsClients{}
	a.Clients.sqs = &clients
	a.Clients.sqs.New(a.session)

	return a.Clients.sqs
}

func (a *x509) Sns() *SnsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.sns != nil {
		return a.Clients.sns
	}

	clients := SnsClients{}
	a.Clients.sns = &clients
	a.Clients.sns.New(a.session)
	return a.Clients.sns
}

func (a *x509) SnsSqs() *SnsSqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.snssqs != nil {
		return a.Clients.snssqs
	}

	clients := SnsSqsClients{}
	a.Clients.snssqs = &clients
	a.Clients.snssqs.New(a.session)
	return a.Clients.snssqs
}

func (a *x509) SecretManager() *SecretManagerClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.Secret != nil {
		return a.Clients.Secret
	}

	clients := SecretManagerClients{}
	a.Clients.Secret = &clients
	a.Clients.Secret.New(a.session)
	return a.Clients.Secret
}

func (a *x509) ParameterStore() *ParameterStoreClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.ParameterStore != nil {
		return a.Clients.ParameterStore
	}

	clients := ParameterStoreClients{}
	a.Clients.ParameterStore = &clients
	a.Clients.ParameterStore.New(a.session)
	return a.Clients.ParameterStore
}

func (a *x509) Kinesis() *KinesisClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.kinesis != nil {
		return a.Clients.kinesis
	}

	clients := KinesisClients{}
	a.Clients.kinesis = &clients
	a.Clients.kinesis.New(a.session)
	return a.Clients.kinesis
}

func (a *x509) Ses() *SesClients {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Clients.ses != nil {
		return a.Clients.ses
	}

	clients := SesClients{}
	a.Clients.ses = &clients
	a.Clients.ses.New(a.session)
	return a.Clients.ses
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

	var awsConfig *aws.Config
	if a.Cfg == nil {
		awsConfig = aws.NewConfig().WithHTTPClient(client).WithLogLevel(aws.LogOff)
	} else {
		awsConfig = a.Cfg.WithHTTPClient(client).WithLogLevel(aws.LogOff)
	}
	if a.region != nil {
		awsConfig.WithRegion(*a.region)
	}
	// this is needed for testing purposes to mock the client,
	// so code never sets the client, but tests do.
	var rolesClient *rolesanywhere.RolesAnywhere
	if a.rolesAnywhereClient == nil {
		mySession = session.Must(session.NewSession(awsConfig))
		rolesAnywhereClient := rolesanywhere.New(mySession, awsConfig)
		// Set up signing function and handlers
		if err := a.setSigningFunction(rolesAnywhereClient); err != nil {
			return nil, err
		}
		rolesClient = rolesAnywhereClient
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
		duration = int64(time.Hour.Seconds())

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
	var output *rolesanywhere.CreateSessionOutput
	if a.rolesAnywhereClient != nil {
		var err error
		output, err = a.rolesAnywhereClient.CreateSessionWithContext(ctx, &createSessionRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to create session using dapr app identity: %w", err)
		}
	} else {
		var err error
		output, err = rolesClient.CreateSessionWithContext(ctx, &createSessionRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to create session using dapr app identity: %w", err)
		}
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
	}, awsConfig))
	if sess == nil {
		return nil, errors.New("session is nil")
	}

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
		for {
			// renew at ~half the lifespan
			expiration, err := a.session.Config.Credentials.ExpiresAt()
			if err != nil {
				a.logger.Errorf("Failed to retrieve session expiration time, using 30 minute interval: %w", err)
				expiration = time.Now().Add(time.Hour)
			}
			timeUntilExpiration := time.Until(expiration)
			refreshInterval := timeUntilExpiration / 2
			select {
			case <-time.After(refreshInterval):
				a.refreshClient()
			case <-a.closeCh:
				a.logger.Debugf("Session refresher is stopped")
				return
			}
		}
	}()
}

func (a *x509) refreshClient() {
	for {
		newSession, err := a.createOrRefreshSession(context.Background())
		if err == nil {
			a.Clients.refresh(newSession)
			a.logger.Debugf("AWS IAM Roles Anywhere session credentials refreshed successfully")
			return
		}
		a.logger.Errorf("Failed to refresh session: %w", err)
		select {
		case <-time.After(time.Second * 5):
		case <-a.closeCh:
			return
		}
	}
}
