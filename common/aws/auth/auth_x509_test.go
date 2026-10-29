/*
Copyright 2026 The Dapr Authors
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

package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
)

func TestGetCertAndSignerKeyAlgorithm(t *testing.T) {
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	_, ed25519Key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	t.Run("an Ed25519 workload key, the daprd default, is rejected", func(t *testing.T) {
		_, _, err := getCertAndSigner(spiffecontext.WithX509(t.Context(), newSVID(t, ed25519Key)))
		require.ErrorContains(t, err, "workload private key is ed25519.PrivateKey, but IAM Roles Anywhere requires ECDSA")
	})

	t.Run("an ECDSA workload key is accepted", func(t *testing.T) {
		cert, signer, err := getCertAndSigner(spiffecontext.WithX509(t.Context(), newSVID(t, ecdsaKey)))
		require.NoError(t, err)
		assert.NotNil(t, cert)
		assert.NotNil(t, signer)
	})
}

// newSVID returns an X.509 SVID for key, issued by a throwaway CA.
func newSVID(t *testing.T, key crypto.Signer) *x509svid.SVID {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caKey.Public(), caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	id := spiffeid.RequireFromString("spiffe://public/ns/default/app")
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{id.URL()},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, key.Public(), caKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(leafDER)
	require.NoError(t, err)

	return &x509svid.SVID{ID: id, Certificates: []*x509.Certificate{leaf, ca}, PrivateKey: key}
}
