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
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"

	kitlogger "github.com/dapr/kit/logger"
)

// createTestCertAndKey returns a self-signed certificate and its RSA private
// key, both PEM-encoded.
// This helper with modifications comes from https://github.com/madflojo/testcerts/blob/main/testcerts.go
// Copyright 2019 Benjamin Cane, MIT License
func createTestCertAndKey(t *testing.T) ([]byte, []byte) {
	t.Helper()

	ca := &x509.Certificate{
		Subject: pkix.Name{
			Organization: []string{"Dapr Development Only Organization"},
		},
		SerialNumber:          big.NewInt(123),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	keypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	cert, err := x509.CreateCertificate(rand.Reader, ca, ca, &keypair.PublicKey, keypair)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(keypair)})
	return certPEM, keyPEM
}

func newTokenEndpointStub(t *testing.T, hits *atomic.Int32, lastForm *sync.Map) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		require.NoError(t, r.ParseForm())
		for k, v := range r.Form {
			lastForm.Store(k, v[0])
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "test-token",
			"expires_in":   3600,
		})
	}))
}

func formValue(t *testing.T, form *sync.Map, key string) string {
	t.Helper()
	v, ok := form.Load(key)
	if !ok {
		return ""
	}
	return v.(string)
}

func decodeJWTPart(t *testing.T, jwtPart string) map[string]interface{} {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(jwtPart)
	require.NoError(t, err)
	var out map[string]interface{}
	require.NoError(t, json.Unmarshal(decoded, &out))
	return out
}

func TestOAuthTokenSourcePrivateKeyJWT(t *testing.T) {
	certPEM, keyPEM := createTestCertAndKey(t)
	// A certificate from an unrelated key pair, used to exercise the cert/key
	// mismatch validation.
	otherCertPEM, _ := createTestCertAndKey(t)

	t.Run("happy path sends a signed client assertion and returns the token", func(t *testing.T) {
		var hits atomic.Int32
		var form sync.Map
		server := newTokenEndpointStub(t, &hits, &form)
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
			Scopes:              []string{"openid", "redis-prod"},
			Resource:            "URI:RS-12345-API",
			Kid:                 "test-kid",
		}

		token, err := ts.Token(context.Background())
		require.NoError(t, err)
		require.Equal(t, "test-token", token.AccessToken)
		require.True(t, token.Expiry.After(time.Now()))

		require.Equal(t, "client_credentials", formValue(t, &form, "grant_type"))
		require.Equal(t, "test-client", formValue(t, &form, "client_id"))
		require.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", formValue(t, &form, "client_assertion_type"))
		require.Equal(t, "URI:RS-12345-API", formValue(t, &form, "resource"))
		require.Equal(t, "openid redis-prod", formValue(t, &form, "scope"))
		require.Empty(t, formValue(t, &form, "audience"))

		assertion := formValue(t, &form, "client_assertion")
		require.NotEmpty(t, assertion)
		parts := strings.Split(assertion, ".")
		require.Len(t, parts, 3, "JWT should have 3 parts")

		header := decodeJWTPart(t, parts[0])
		require.Equal(t, "test-kid", header["kid"])
		require.Equal(t, "RS256", header["alg"])

		claims := decodeJWTPart(t, parts[1])
		require.Equal(t, "test-client", claims["iss"])
		require.Equal(t, "test-client", claims["sub"])
		// Some IdPs require the audience to be a single JSON string, not an array
		require.IsType(t, "", claims["aud"])
		require.Equal(t, server.URL, claims["aud"])
		require.NotEmpty(t, claims["jti"])
	})

	t.Run("audience overrides the aud claim and is sent as form parameter", func(t *testing.T) {
		var hits atomic.Int32
		var form sync.Map
		server := newTokenEndpointStub(t, &hits, &form)
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
			Audience:            "https://idp.example.com/realms/local",
		}

		_, err := ts.Token(context.Background())
		require.NoError(t, err)

		require.Equal(t, "https://idp.example.com/realms/local", formValue(t, &form, "audience"))
		parts := strings.Split(formValue(t, &form, "client_assertion"), ".")
		claims := decodeJWTPart(t, parts[1])
		require.Equal(t, "https://idp.example.com/realms/local", claims["aud"])
	})

	t.Run("valid cached token short-circuits the endpoint", func(t *testing.T) {
		var hits atomic.Int32
		var form sync.Map
		server := newTokenEndpointStub(t, &hits, &form)
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}
		ts.cachedToken = oauth2.Token{AccessToken: "cached-token", Expiry: time.Now().Add(1 * time.Hour)}

		token, err := ts.Token(context.Background())
		require.NoError(t, err)
		require.Equal(t, "cached-token", token.AccessToken)
		require.Equal(t, int32(0), hits.Load())
	})

	t.Run("ForceToken bypasses a valid cached token", func(t *testing.T) {
		var hits atomic.Int32
		var form sync.Map
		server := newTokenEndpointStub(t, &hits, &form)
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}
		ts.cachedToken = oauth2.Token{AccessToken: "cached-token", Expiry: time.Now().Add(1 * time.Hour)}

		token, err := ts.ForceToken(context.Background())
		require.NoError(t, err)
		require.Equal(t, "test-token", token.AccessToken)
		require.Equal(t, int32(1), hits.Load())
	})

	t.Run("concurrent calls fetch only once", func(t *testing.T) {
		var hits atomic.Int32
		var form sync.Map
		server := newTokenEndpointStub(t, &hits, &form)
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}

		var wg sync.WaitGroup
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := ts.Token(context.Background())
				require.NoError(t, err)
			}()
		}
		wg.Wait()
		require.Equal(t, int32(1), hits.Load())
	})

	t.Run("missing configuration errors", func(t *testing.T) {
		tests := []struct {
			name string
			ts   *OAuthTokenSourcePrivateKeyJWT
			err  string
		}{
			{
				name: "missing token endpoint",
				ts:   &OAuthTokenSourcePrivateKeyJWT{ClientID: "test-client"},
				err:  "not fully configured",
			},
			{
				name: "missing client ID",
				ts:   &OAuthTokenSourcePrivateKeyJWT{TokenEndpoint: oauth2.Endpoint{TokenURL: "https://idp.example.com/token"}},
				err:  "not fully configured",
			},
			{
				name: "missing client assertion cert and key",
				ts: &OAuthTokenSourcePrivateKeyJWT{
					TokenEndpoint: oauth2.Endpoint{TokenURL: "https://idp.example.com/token"},
					ClientID:      "test-client",
				},
				err: "requires client assertion cert and key",
			},
			{
				name: "invalid private key",
				ts: &OAuthTokenSourcePrivateKeyJWT{
					TokenEndpoint:       oauth2.Endpoint{TokenURL: "https://idp.example.com/token"},
					ClientID:            "test-client",
					ClientAssertionCert: string(certPEM),
					ClientAssertionKey:  "not a key",
				},
				err: "unable to parse private key",
			},
			{
				name: "certificate does not match private key",
				ts: &OAuthTokenSourcePrivateKeyJWT{
					TokenEndpoint:       oauth2.Endpoint{TokenURL: "https://idp.example.com/token"},
					ClientID:            "test-client",
					ClientAssertionCert: string(otherCertPEM),
					ClientAssertionKey:  string(keyPEM),
				},
				err: "does not match the private key",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tt.ts.Token(context.Background())
				require.ErrorContains(t, err, tt.err)
			})
		}
	})

	t.Run("non-2xx response errors", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}

		_, err := ts.Token(context.Background())
		require.ErrorContains(t, err, "token endpoint returned 401")
	})

	t.Run("empty access_token in response errors", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"expires_in": 3600}`)
		}))
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}

		_, err := ts.Token(context.Background())
		require.ErrorContains(t, err, "no access_token in response")
	})

	t.Run("custom CA verifies the token endpoint TLS connection", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token": "tls-token", "expires_in": 3600}`)
		}))
		defer server.Close()

		serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}
		require.NoError(t, ts.addCa(string(serverCertPEM)))

		token, err := ts.Token(context.Background())
		require.NoError(t, err)
		require.Equal(t, "tls-token", token.AccessToken)
	})

	t.Run("without the custom CA the TLS connection fails", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token": "tls-token", "expires_in": 3600}`)
		}))
		defer server.Close()

		ts := &OAuthTokenSourcePrivateKeyJWT{
			TokenEndpoint:       oauth2.Endpoint{TokenURL: server.URL},
			ClientID:            "test-client",
			ClientAssertionCert: string(certPEM),
			ClientAssertionKey:  string(keyPEM),
		}

		_, err := ts.Token(context.Background())
		require.Error(t, err)
	})
}

func TestGetOIDCTokenSourceAndSetInitialTokenAsPassword(t *testing.T) {
	certPEM, keyPEM := createTestCertAndKey(t)
	logger := kitlogger.NewLogger("test")

	validSettings := func(tokenURL string) *Settings {
		return &Settings{
			UseOIDC:                 true,
			OidcTokenEndpoint:       tokenURL,
			OidcClientID:            "test-client",
			OidcClientAssertionCert: string(certPEM),
			OidcClientAssertionKey:  string(keyPEM),
		}
	}

	t.Run("validation errors", func(t *testing.T) {
		tests := []struct {
			name   string
			modify func(s *Settings)
			err    string
		}{
			{
				name:   "useEntraID and useOIDC are mutually exclusive",
				modify: func(s *Settings) { s.UseEntraID = true },
				err:    "useEntraID and useOIDC must not be enabled at the same time",
			},
			{
				name:   "password must not be set",
				modify: func(s *Settings) { s.Password = "secret" },
				err:    "password must not be specified when using OIDC authentication",
			},
			{
				name:   "missing token endpoint",
				modify: func(s *Settings) { s.OidcTokenEndpoint = "" },
				err:    "missing oidcTokenEndpoint",
			},
			{
				name:   "missing client ID",
				modify: func(s *Settings) { s.OidcClientID = "" },
				err:    "missing oidcClientID",
			},
			{
				name:   "missing client assertion cert",
				modify: func(s *Settings) { s.OidcClientAssertionCert = "" },
				err:    "missing oidcClientAssertionCert",
			},
			{
				name:   "missing client assertion key",
				modify: func(s *Settings) { s.OidcClientAssertionKey = "" },
				err:    "missing oidcClientAssertionKey",
			},
			{
				name:   "invalid CA cert",
				modify: func(s *Settings) { s.OidcCACert = "not a cert" },
				err:    "error parsing PEM certificate",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				s := validSettings("https://idp.example.com/token")
				tt.modify(s)
				_, _, err := s.GetOIDCTokenSourceAndSetInitialTokenAsPassword(context.Background(), &logger)
				require.ErrorContains(t, err, tt.err)
			})
		}
	})

	t.Run("happy path sets the token as password and leaves the username empty", func(t *testing.T) {
		var hits atomic.Int32
		var form sync.Map
		server := newTokenEndpointStub(t, &hits, &form)
		defer server.Close()

		s := validSettings(server.URL)
		tokenSource, expiry, err := s.GetOIDCTokenSourceAndSetInitialTokenAsPassword(context.Background(), &logger)
		require.NoError(t, err)
		require.NotNil(t, tokenSource)
		require.True(t, expiry.After(time.Now()))
		require.Equal(t, "test-token", s.Password)
		// No explicit username: kept empty so the client uses the 1-argument
		// AUTH <token> form for maximum RESP compatibility.
		require.Empty(t, s.Username)
		// no scopes specified: defaults to openid only
		require.Equal(t, []string{"openid"}, s.internalOidcScopes)
		require.Equal(t, "openid", formValue(t, &form, "scope"))
	})

	t.Run("explicit username and scopes are preserved", func(t *testing.T) {
		var hits atomic.Int32
		var form sync.Map
		server := newTokenEndpointStub(t, &hits, &form)
		defer server.Close()

		s := validSettings(server.URL)
		s.Username = "my-user"
		s.OidcScopes = "openid,redis-prod"
		_, _, err := s.GetOIDCTokenSourceAndSetInitialTokenAsPassword(context.Background(), &logger)
		require.NoError(t, err)
		require.Equal(t, "my-user", s.Username)
		require.Equal(t, []string{"openid", "redis-prod"}, s.internalOidcScopes)
		require.Equal(t, "openid redis-prod", formValue(t, &form, "scope"))
	})

	t.Run("token endpoint failure surfaces as configuration error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		s := validSettings(server.URL)
		_, _, err := s.GetOIDCTokenSourceAndSetInitialTokenAsPassword(context.Background(), &logger)
		require.ErrorContains(t, err, "redis client configuration error")
	})
}
