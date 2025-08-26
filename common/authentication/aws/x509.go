/*
Copyright 2024 The Dapr Authors
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
	"strconv"
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	awssh "github.com/aws/rolesanywhere-credential-helper/aws_signing_helper"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere/rolesanywhereiface"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	cryptopem "github.com/dapr/kit/crypto/pem"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

func isX509Auth(m map[string]string) bool {
	tp := m["trustProfileArn"]
	ta := m["trustAnchorArn"]
	ar := m["assumeRoleArn"]
	return tp != "" && ta != "" && ar != ""
}

type x509Options struct {
	TrustProfileArn *string `json:"trustProfileArn" mapstructure:"trustProfileArn"`
	TrustAnchorArn  *string `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	AssumeRoleArn   *string `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
}

type x509 struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	closeCh chan struct{}

	logger              logger.Logger
	clients             *Clients
	rolesAnywhereClient rolesanywhereiface.RolesAnywhereAPI // this is so we can mock it in tests
	session             *session.Session
	cfg                 *aws.Config

	chainPEM []byte
	keyPEM   []byte

	region          *string
	trustProfileArn *string
	trustAnchorArn  *string
	assumeRoleArn   *string
	sessionName     string
}

func newX509(ctx context.Context, opts Options, cfg *aws.Config) (*x509, error) {
	var x509Auth x509Options
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
	}

	auth := &x509{
		logger:          opts.Logger,
		trustProfileArn: x509Auth.TrustProfileArn,
		trustAnchorArn:  x509Auth.TrustAnchorArn,
		assumeRoleArn:   x509Auth.AssumeRoleArn,
		cfg: func() *aws.Config {
			// if nil is passed or it's just a default cfg,
			// then we use the options to build the aws cfg.
			if cfg != nil && cfg != aws.NewConfig() {
				return cfg
			}
			return GetConfig(opts)
		}(),
		clients: newClients(),
		closeCh: make(chan struct{}),
	}

	if err := auth.getCertPEM(ctx); err != nil {
		return nil, fmt.Errorf("failed to get x.509 credentials: %v", err)
	}

	// Parse trust anchor and profile ARNs
	if err := auth.initializeTrustAnchors(); err != nil {
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
	return nil
}

func (a *x509) getCertPEM(ctx context.Context) error {
	// retrieve svid from spiffe context
	svid, ok := spiffecontext.X509From(ctx)
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

	if a.clients.s3 != nil {
		return a.clients.s3
	}

	s3Clients := S3Clients{}
	a.clients.s3 = &s3Clients
	a.clients.s3.New(a.session)
	return a.clients.s3
}

func (a *x509) DynamoDB() *DynamoDBClients {
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

func (a *x509) Sqs() *SqsClients {
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

func (a *x509) Sns() *SnsClients {
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

func (a *x509) SnsSqs() *SnsSqsClients {
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

func (a *x509) SecretManager() *SecretManagerClients {
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

func (a *x509) ParameterStore() *ParameterStoreClients {
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

func (a *x509) Kinesis() *KinesisClients {
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

func (a *x509) Ses() *SesClients {
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

// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html
func (a *x509) getDatabaseToken(ctx context.Context, poolConfig *pgxpool.Config) (string, error) {
	dbEndpoint := poolConfig.ConnConfig.Host + ":" + strconv.Itoa(int(poolConfig.ConnConfig.Port))

	// First, check session credentials.
	// This should always be what we use to generate the x509 auth credentials for postgres.
	// However, we can leave the Second and Lastly checks as backup for now.
	var creds credentials.Value
	if a.session != nil {
		var err error
		creds, err = a.session.Config.Credentials.Get()
		if err != nil {
			a.logger.Infof("failed to get access key and secret key, will fallback to reading the default AWS credentials file: %w", err)
		}
	}

	if creds.AccessKeyID != "" && creds.SecretAccessKey != "" {
		creds, err := a.session.Config.Credentials.Get()
		if err != nil {
			return "", fmt.Errorf("failed to retrieve session credentials: %w", err)
		}
		awsCfg := v2creds.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)
		authenticationToken, err := auth.BuildAuthToken(
			ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}

		return authenticationToken, nil
	}

	// Second, check if we are assuming a role instead
	if a.assumeRoleArn != nil {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
		}
		stsClient := sts.NewFromConfig(awsCfg)

		assumeRoleCfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(*a.region),
			config.WithCredentialsProvider(
				awsv2.NewCredentialsCache(
					stscreds.NewAssumeRoleProvider(stsClient, *a.assumeRoleArn, func(aro *stscreds.AssumeRoleOptions) {
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

	authenticationToken, err := auth.BuildAuthToken(
		ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
	}

	return authenticationToken, nil
}

func (a *x509) UpdatePostgres(ctx context.Context, poolConfig *pgxpool.Config) {
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

func (a *x509) initializeTrustAnchors() error {
	var (
		trustAnchor arn.ARN
		profile     arn.ARN
		err         error
	)
	if a.trustAnchorArn != nil {
		trustAnchor, err = arn.Parse(*a.trustAnchorArn)
		if err != nil {
			return err
		}
		a.region = &trustAnchor.Region
	}

	if a.trustProfileArn != nil {
		profile, err = arn.Parse(*a.trustProfileArn)
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
	if a.cfg == nil {
		awsConfig = aws.NewConfig().WithHTTPClient(client).WithLogLevel(aws.LogOff)
	} else {
		awsConfig = a.cfg.WithHTTPClient(client).WithLogLevel(aws.LogOff)
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

	createSessionRequest := rolesanywhere.CreateSessionInput{
		Cert:           ptr.Of(string(a.chainPEM)),
		ProfileArn:     a.trustProfileArn,
		TrustAnchorArn: a.trustAnchorArn,
		RoleArn:        a.assumeRoleArn,
		// https://aws.amazon.com/about-aws/whats-new/2024/03/iam-roles-anywhere-credentials-valid-12-hours/#:~:text=The%20duration%20can%20range%20from,and%20applications%2C%20to%20use%20X.
		DurationSeconds:    aws.Int64(int64(time.Hour.Seconds())), // AWS default is 1hr timeout
		InstanceProperties: nil,
		SessionName:        nil,
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
			err = a.clients.refresh(newSession)
			if err != nil {
				a.logger.Errorf("Failed to refresh client, retrying in 5 seconds: %w", err)
			}
			a.logger.Debugf("AWS IAM Roles Anywhere session credentials refreshed successfully")
			return
		}
		a.logger.Errorf("Failed to refresh session, retrying in 5 seconds: %w", err)
		select {
		case <-time.After(time.Second * 5):
		case <-a.closeCh:
			return
		}
	}
}
