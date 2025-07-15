package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/mikeee/aws_credential_helper"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"

	cryptopem "github.com/dapr/kit/crypto/pem"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

func isX509Auth(m map[string]string) bool {
	tp := m["trustProfileArn"]
	ta := m["trustAnchorArn"]
	ar := m["assumeRoleArn"]
	return tp != "" && ta != "" && ar != ""
}

type X509 struct {
	logger logger.Logger

	x509Cert *x509.Certificate

	region          *string
	assumeRoleArn   *string
	trustAnchorArn  *string
	trustProfileArn *string

	wrappedCredentialProvider *aws_credential_helper.CredentialProvider
}

func (x *X509) Type() ProviderType {
	return X509ProviderType
}

func (x *X509) RefreshX509(ctx context.Context) error {
	cert, _, signer, err := getCertAndSigner(ctx)
	if err != nil {
		return errors.New("failed to refresh X509 cert: " + err.Error())
	}
	x.x509Cert = cert
	if x.wrappedCredentialProvider != nil {
		if x.region == nil || x.assumeRoleArn == nil || x.trustAnchorArn == nil || x.trustProfileArn == nil {
			return errors.New("missing required fields: Region, AssumeRoleArn, TrustAnchorArn, TrustProfileArn")
		}

		credentialProviderInput := aws_credential_helper.CredentialProviderInput{
			Region:          *x.region,
			TrustProfileArn: *x.trustProfileArn,
			TrustAnchorArn:  *x.trustAnchorArn,
			AssumeRoleArn:   *x.assumeRoleArn,
			Signer:          *signer,
		}

		credentialProvider, err := aws_credential_helper.NewCredentialProvider(ctx, credentialProviderInput)
		if err != nil {
			return errors.New("failed to create new credential provider: " + err.Error())
		}

		x.wrappedCredentialProvider = credentialProvider
	} else {
		// change signer
		x.wrappedCredentialProvider.ChangeSigner(*signer)
	}

	return nil
}

func (x *X509) Retrieve(ctx context.Context) (aws.Credentials, error) {
	// Check cert/svid expiry, if expired then refresh
	if isCertExpired(x.x509Cert) {
		if err := x.RefreshX509(ctx); err != nil {
			return aws.Credentials{}, errors.New("failed to refresh X509 cert: " + err.Error())
		}
	}
	return x.wrappedCredentialProvider.Retrieve(ctx)
}

func getSpiffeX509Svid(ctx context.Context) (*x509svid.SVID, error) {
	svid, ok := spiffecontext.X509From(ctx)
	if !ok {
		return nil, errors.New("invalid SVID: no certs found")
	}

	return svid.GetX509SVID()
}

func marshalSvid(svid *x509svid.SVID) ([]byte, []byte, error) {
	if svid == nil {
		return nil, nil, errors.New("nil SVID")
	}

	return svid.Marshal()
}

func getCertAndSigner(ctx context.Context) (*x509.Certificate, *ecdsa.PrivateKey, *aws_credential_helper.Signer,
	error,
) {
	// obtain certs from spiffe via context
	x509Svid, err := getSpiffeX509Svid(ctx)
	if err != nil {
		panic("failed to get SPIFFE certs: " + err.Error())
	}

	chain, key, err := marshalSvid(x509Svid)
	if err != nil {
		return nil, nil, nil, err
	}

	certs, err := cryptopem.DecodePEMCertificatesChain(chain)
	if err != nil {
		return nil, nil, nil, err
	}

	pkey, err := cryptopem.DecodePEMPrivateKey(key)
	if err != nil {
		return nil, nil, nil, err
	}

	signer := aws_credential_helper.NewSigner(certs[0], pkey.(*ecdsa.PrivateKey))

	return certs[0], pkey.(*ecdsa.PrivateKey), &signer, nil
}

func isCertExpired(cert *x509.Certificate) bool {
	if cert == nil {
		return true
	}
	now := time.Now()
	return now.After(cert.NotAfter) || now.Before(cert.NotBefore)
}

type awsRAOpts struct {
	AssumeRoleArn   *string `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
	TrustAnchorArn  *string `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	TrustProfileArn *string `json:"trustProfileArn" mapstructure:"trustProfileArn"`
}

func newAuthX509(ctx context.Context, opts Options) (*X509, error) {
	var authOpts awsRAOpts
	if err := kitmd.DecodeMetadata(opts.Properties, &authOpts); err != nil {
		return nil, errors.New("failed to decode metadata: " + err.Error())
	}

	switch {
	case authOpts.AssumeRoleArn == nil:
		return nil, errors.New("missing required field: AssumeRoleArn")
	case authOpts.TrustAnchorArn == nil:
		return nil, errors.New("missing required field: TrustAnchorArn")
	case authOpts.TrustProfileArn == nil:
		return nil, errors.New("missing required field: TrustProfileArn")
	}

	cert, _, signer, err := getCertAndSigner(ctx)
	if err != nil {
		return nil, errors.New("failed to get cert and signer: " + err.Error())
	}

	// create credential provider

	authInput := aws_credential_helper.CredentialProviderInput{
		Region: opts.Region,

		AssumeRoleArn:   *authOpts.AssumeRoleArn,
		TrustAnchorArn:  *authOpts.TrustAnchorArn,
		TrustProfileArn: *authOpts.TrustProfileArn,

		Signer: *signer,
	}

	credentialProvider, err := aws_credential_helper.NewCredentialProvider(ctx, authInput)
	if err != nil {
		return nil, errors.New("failed to create new credential provider: " + err.Error())
	}

	// test credentialprovider
	cred, err := credentialProvider.Retrieve(ctx)
	if err != nil {
		return nil, errors.New("failed to retrieve credentials: " + err.Error())
	}

	if cred.Expires.Before(time.Now()) {
		return nil, errors.New("credentials are not valid")
	}

	return &X509{
		logger:                    nil,
		x509Cert:                  cert,
		wrappedCredentialProvider: credentialProvider,
	}, nil
}
