package auth

import (
	"context"
	"crypto"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/dapr/components-contrib/common/aws/options"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	awssh "github.com/mikeee/aws_rolesanywhere-credential-helper/aws_signing_helper"
	"sync"
)

const (
	aws4_x509_ecdsa_sha256 = "AWS4-X509-ECDSA-SHA256"
)

func authTypeIsx509(opts options.Options) bool {
	if opts.Properties != nil {
		keys := []string{"trustProfileArn", "trustAnchorArn", "assumeRoleArn"}
		for _, key := range keys {
			if _, ok := opts.Properties[key]; !ok {
				return false
			}
		}
		return true
	}
	return false
}

type AuthX509Options struct {
	TrustProfileArn *string `json:"trustProfileArn" mapstructure:"trustProfileArn"`
	TrustAnchorArn  *string `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	AssumeRoleArn   *string `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
}

type AuthX509 struct {
	logger logger.Logger
	mutex  sync.RWMutex

	signer        crypto.Signer
	chainPEM      []byte
	privateKeyPEM []byte
	awsSigner     awsSigner

	assumeRoleArn   *string
	trustProfileArn *string
	trustAnchorArn  *string
	region          *string
}

func newX509Auth(ctx context.Context, opts options.Options) (AuthProvider, error) {
	var x509Opts AuthX509Options
	if err := kitmd.DecodeMetadata(opts.Properties, &x509Opts); err != nil {
		return nil, err
	}
	switch {
	case x509Opts.TrustProfileArn == nil:
		return nil, errors.New("trustProfileArn is required")
	case x509Opts.TrustAnchorArn == nil:
		return nil, errors.New("trustAnchorArn is required")
	case x509Opts.AssumeRoleArn == nil:
		return nil, errors.New("assumeRoleArn is required")
	}

	authX509 := &AuthX509{
		logger: opts.Logger,

		assumeRoleArn:   x509Opts.AssumeRoleArn,
		trustAnchorArn:  x509Opts.TrustAnchorArn,
		trustProfileArn: x509Opts.TrustProfileArn,
	}

	if err := authX509.updateCertsFromContext(ctx); err != nil {
		return nil, err
	}

	return authX509, nil
}

func (x *AuthX509) updateCertsFromContext(ctx context.Context) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	svidSrc, ok := spiffecontext.From(ctx)
	if !ok {
		return errors.New("failed to get SVID from context")
	}

	svid, err := svidSrc.GetX509SVID()
	if err != nil {
		return fmt.Errorf("failed to get SVID: %w", err)
	}

	chainPEM, privateKeyPEM, err := svid.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal SVID: %w", err)
	}

	x.chainPEM = chainPEM
	x.privateKeyPEM = privateKeyPEM

	signer, err := newX509Signer(privateKeyPEM)
	if err != nil {
		return fmt.Errorf("failed to create signer: %w", err)
	}
	x.signer = signer

	x.awsSigner = awsSigner{
		Signer: x.signer,
		chain:  x.chainPEM,
	}

	return nil
}

func parseX509Certificate(chainPEM []byte) (*x509.Certificate, error) {
	return x509.ParseCertificate(chainPEM)
}

func newX509Signer(privateKeyPEM []byte) (crypto.Signer, error) {
	privateKey, err := x509.ParseECPrivateKey(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}
	return privateKey, nil
}

type awsSigner struct {
	crypto.Signer
	chain []byte
}

func (a *awsSigner) Certificate() (*x509.Certificate, error) {
	cert, err := x509.ParseCertificate(a.chain)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}
	return cert, nil
}

func (a *awsSigner) CertificateChain() ([]*x509.Certificate, error) {
	cert, err := a.Certificate()
	if err != nil {
		return nil, fmt.Errorf("failed to get certificate: %w", err)
	}
	return []*x509.Certificate{cert}, nil
}

func (a *awsSigner) Close() {
	panic("implement me")
}

func (x *AuthX509) getCredentials() (*awssh.CredentialProcessOutput, error) {
	credentialsOpts := &awssh.CredentialsOpts{
		RoleArn:           *x.assumeRoleArn,
		ProfileArnStr:     *x.trustProfileArn,
		TrustAnchorArnStr: *x.trustAnchorArn,
		SessionDuration:   SESSION_DURATION,
		Region:            *x.region,
		Debug:             true, // TODO: @mikeee remove debug
	}

	cpo, err := awssh.GenerateCredentials(credentialsOpts, &x.awsSigner, aws4_x509_ecdsa_sha256)
	if err != nil {
		return nil, fmt.Errorf("failed to generate credentials: %w", err)
	}

	return &cpo, nil
}

func (x *AuthX509) GetCredentials() (aws.Credentials, error) {
	panic("implement me")
}

func (x *AuthX509) getOrRefreshCredentials(ctx context.Context) (aws.Credentials, error) {
	panic("implement me")
}

func (x *AuthX509) AuthTest() bool {
	return true
}

func (x *AuthX509) GetAWSCredentialsProvider() aws.CredentialsProvider {
	awsConfig, err := config.LoadDefaultConfig(
		context.TODO(),
	)
	if err != nil {
		x.logger.Errorf("failed to load default config: %v", err)
		return nil
	}

	// create a new sts client
	stsClient := sts.NewFromConfig(awsConfig)

	assumeRoleProvider := stscreds.NewAssumeRoleProvider(stsClient, *x.assumeRoleArn)

	return aws.NewCredentialsCache(assumeRoleProvider)
}

func (x *AuthX509) Close() error {
	panic("implement me")
}
