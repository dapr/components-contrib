package aws

import (
	"context"
	cryptoX509 "crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere/rolesanywhereiface"
	"github.com/dapr/kit/crypto/spiffe"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/crypto/test"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockRequestSVIDFn() ([]*cryptoX509.Certificate, error) {
	spiffeID, err := url.Parse("spiffe://example.org/test") // create tester SPIFFE ID
	if err != nil {
		return nil, err
	}

	// encode the URI as a SAN extension
	uriSAN, err := asn1.Marshal(spiffeID.String())
	if err != nil {
		return nil, err
	}

	// create dummy certificate with the required URI SAN
	cert := &cryptoX509.Certificate{
		Subject:      pkix.Name{CommonName: "test-cert"},
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		ExtraExtensions: []pkix.Extension{
			{
				Id:       asn1.ObjectIdentifier{2, 5, 29, 17}, // OID for subject alternative name
				Critical: false,
				Value:    uriSAN,
			},
		},
	}

	return []*cryptoX509.Certificate{cert}, nil
}

type mockRolesAnywhereClient struct {
	rolesanywhereiface.RolesAnywhereAPI

	CreateSessionOutput *rolesanywhere.CreateSessionOutput
	CreateSessionError  error
}

func (m *mockRolesAnywhereClient) CreateSessionWithContext(ctx context.Context, input *rolesanywhere.CreateSessionInput, opts ...request.Option) (*rolesanywhere.CreateSessionOutput, error) {
	return m.CreateSessionOutput, m.CreateSessionError
}

func TestGetX509Client(t *testing.T) {
	tests := []struct {
		name       string
		mockOutput *rolesanywhere.CreateSessionOutput
		mockError  error
	}{
		{
			name: "valid x509 client",
			mockOutput: &rolesanywhere.CreateSessionOutput{
				CredentialSet: []*rolesanywhere.CredentialResponse{
					{
						Credentials: &rolesanywhere.Credentials{
							AccessKeyId:     aws.String("mockAccessKeyId"),
							SecretAccessKey: aws.String("mockSecretAccessKey"),
							SessionToken:    aws.String("mockSessionToken"),
							Expiration:      aws.String(time.Now().Add(15 * time.Minute).Format(time.RFC3339)),
						},
					},
				},
			},
			mockError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration := time.Duration(800)
			mockSvc := &mockRolesAnywhereClient{
				CreateSessionOutput: tt.mockOutput,
				CreateSessionError:  tt.mockError,
			}
			mockAWS := &x509{
				logger:              logger.NewLogger("testLogger"),
				AssumeRoleArn:       ptr.Of("arn:aws:iam:012345678910:role/exampleIAMRoleName"),
				TrustAnchorArn:      ptr.Of("arn:aws:rolesanywhere:us-west-1:012345678910:trust-anchor/01234568-0123-0123-0123-012345678901"),
				TrustProfileArn:     ptr.Of("arn:aws:rolesanywhere:us-west-1:012345678910:profile/01234568-0123-0123-0123-012345678901"),
				SessionDuration:     &duration,
				rolesAnywhereClient: mockSvc,
			}
			pki := test.GenPKI(t, test.PKIOptions{
				LeafID: spiffeid.RequireFromString("spiffe://example.com/foo/bar"),
			})

			respCert := []*cryptoX509.Certificate{pki.LeafCert}
			var respErr error

			var fetches atomic.Int32
			s := spiffe.New(spiffe.Options{
				Log: logger.NewLogger("test"),
				RequestSVIDFn: func(context.Context, []byte) ([]*cryptoX509.Certificate, error) {
					fetches.Add(1)
					return respCert, respErr
				},
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errCh := make(chan error)
			go func() {
				errCh <- s.Run(ctx)
			}()

			select {
			case err := <-errCh:
				require.NoError(t, err)
			default:
			}

			err := s.Ready(ctx)
			assert.NoError(t, err)

			// inject the SVID source into the context
			ctx = spiffecontext.With(ctx, s)
			session, err := mockAWS.createOrRefreshSession(ctx)

			assert.NoError(t, err)
			assert.NotNil(t, session)
		})
	}
}
