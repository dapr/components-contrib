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

package redis

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// genTestCertAndKey produces a self-signed PEM-encoded cert and the
// corresponding PEM-encoded RSA private key for OIDC private_key_jwt tests.
func genTestCertAndKey(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Dapr Test"}},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

func TestOIDCTokenSource_HappyPath(t *testing.T) {
	certPEM, keyPEM := genTestCertAndKey(t)

	var (
		mu       sync.Mutex
		received url.Values
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		mu.Lock()
		received = r.Form
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "the-bearer-jwt",
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	ts := &OAuthTokenSourcePrivateKeyJWT{
		TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
		ClientID:            "test-client",
		ClientAssertionCert: string(certPEM),
		ClientAssertionKey:  string(keyPEM),
		Resource:            "JPMC:URI:TEST",
		Kid:                 "deadbeefcafebabe",
		Scopes:              []string{"openid"},
	}

	token, expiry, err := ts.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "the-bearer-jwt", token)
	assert.WithinDuration(t, time.Now().Add(1*time.Hour), expiry, 5*time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify the form parameters that JPMC's ADFS specifies.
	assert.Equal(t, "client_credentials", received.Get("grant_type"))
	assert.Equal(t, "test-client", received.Get("client_id"))
	assert.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
		received.Get("client_assertion_type"))
	assert.Equal(t, "JPMC:URI:TEST", received.Get("resource"))
	assert.Equal(t, "openid", received.Get("scope"))

	assertion := received.Get("client_assertion")
	require.NotEmpty(t, assertion)

	// Verify the assertion's aud claim is a STRING, not a single-element
	// array. JPMC's ADFS rejects array-valued aud claims.
	parts := strings.Split(assertion, ".")
	require.Len(t, parts, 3, "JWT should have 3 segments")
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims map[string]interface{}
	require.NoError(t, json.Unmarshal(payloadBytes, &claims))
	assert.IsType(t, "", claims["aud"], "aud must be a JSON string, not an array")
	assert.Equal(t, "test-client", claims["iss"])
	assert.Equal(t, "test-client", claims["sub"])

	// Verify the kid header is set on the JWT.
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, err)
	var header map[string]interface{}
	require.NoError(t, json.Unmarshal(headerBytes, &header))
	assert.Equal(t, "deadbeefcafebabe", header["kid"])
	assert.Equal(t, "RS256", header["alg"])
}

func TestOIDCTokenSource_CachesUntilExpiry(t *testing.T) {
	certPEM, keyPEM := genTestCertAndKey(t)

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "tok",
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	ts := &OAuthTokenSourcePrivateKeyJWT{
		TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
		ClientID:            "test-client",
		ClientAssertionCert: string(certPEM),
		ClientAssertionKey:  string(keyPEM),
	}

	_, _, err := ts.Token(context.Background())
	require.NoError(t, err)
	_, _, err = ts.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "second call should be served from the cached token")
}

func TestOIDCTokenSource_TokenEndpointError(t *testing.T) {
	certPEM, keyPEM := genTestCertAndKey(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer server.Close()

	ts := &OAuthTokenSourcePrivateKeyJWT{
		TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
		ClientID:            "test-client",
		ClientAssertionCert: string(certPEM),
		ClientAssertionKey:  string(keyPEM),
	}

	_, _, err := ts.Token(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestOIDCTokenSource_MissingConfiguration(t *testing.T) {
	ts := &OAuthTokenSourcePrivateKeyJWT{
		TokenEndpoint: oauth2.Endpoint{TokenURL: "https://idp.example/token"},
		// ClientID intentionally empty
	}
	_, _, err := ts.Token(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not fully configured")
}

func TestSettings_GetOIDCTokenAndSetAsPassword_RequiresFields(t *testing.T) {
	cases := []struct {
		name    string
		s       Settings
		wantErr string
	}{
		{
			name:    "missing token endpoint",
			s:       Settings{OidcClientID: "c", OidcClientAssertionCert: "x", OidcClientAssertionKey: "y"},
			wantErr: "oidcTokenEndpoint",
		},
		{
			name:    "missing client id",
			s:       Settings{OidcTokenEndpoint: "https://idp/token", OidcClientAssertionCert: "x", OidcClientAssertionKey: "y"},
			wantErr: "oidcClientID",
		},
		{
			name:    "missing cert/key",
			s:       Settings{OidcTokenEndpoint: "https://idp/token", OidcClientID: "c"},
			wantErr: "oidcClientAssertionCert",
		},
		{
			name:    "static password set alongside OIDC",
			s:       Settings{Password: "static", OidcTokenEndpoint: "https://idp/token", OidcClientID: "c", OidcClientAssertionCert: "x", OidcClientAssertionKey: "y"},
			wantErr: "redisPassword",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tc.s.GetOIDCTokenAndSetAsPassword(context.Background())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
