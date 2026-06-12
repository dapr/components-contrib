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
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"

	"github.com/dapr/kit/crypto/pem"
)

// OAuthTokenSourcePrivateKeyJWT obtains OAuth2 access tokens using the
// client_credentials grant with the private_key_jwt client authentication
// method (RFC 7523), where the client authenticates to the token endpoint
// with a signed JWT assertion instead of a client secret.
// It is a port of the Kafka component's implementation in
// common/component/kafka/sasl_oauthbearer_private_key_jwt.go, without the
// Sarama-specific wrapping.
type OAuthTokenSourcePrivateKeyJWT struct {
	TokenEndpoint       oauth2.Endpoint
	ClientID            string
	Scopes              []string
	ClientAssertionCert string
	ClientAssertionKey  string
	Resource            string
	Audience            string
	Kid                 string

	lock         sync.Mutex
	cachedToken  oauth2.Token
	httpClient   *http.Client
	trustedCas   []*x509.Certificate
	skipCaVerify bool
}

type oidcTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

func (ts *OAuthTokenSourcePrivateKeyJWT) addCa(caPem string) error {
	pemBytes := []byte(caPem)

	caCerts, err := pem.DecodePEMCertificates(pemBytes)
	if err != nil {
		return fmt.Errorf("error parsing PEM certificate: %w", err)
	}
	if len(caCerts) > 1 {
		return fmt.Errorf("expected 1 certificate, got %d", len(caCerts))
	}

	if ts.trustedCas == nil {
		ts.trustedCas = make([]*x509.Certificate, 0)
	}
	ts.trustedCas = append(ts.trustedCas, caCerts[0])

	return nil
}

func (ts *OAuthTokenSourcePrivateKeyJWT) configureClient() {
	if ts.httpClient != nil {
		return
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: ts.skipCaVerify, //nolint:gosec
	}

	if ts.trustedCas != nil {
		caPool, err := x509.SystemCertPool()
		if err != nil {
			caPool = x509.NewCertPool()
		}

		for _, c := range ts.trustedCas {
			caPool.AddCert(c)
		}
		tlsConfig.RootCAs = caPool
	}

	ts.httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}

// Token returns the cached access token if it is still valid, otherwise it
// fetches a new one from the token endpoint.
func (ts *OAuthTokenSourcePrivateKeyJWT) Token(ctx context.Context) (*oauth2.Token, error) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	return ts.token(ctx)
}

// ForceToken discards any cached token and fetches a fresh one. It is used by
// the refresh routine, which runs before the cached token expires and would
// otherwise be handed the still-valid cached token back.
func (ts *OAuthTokenSourcePrivateKeyJWT) ForceToken(ctx context.Context) (*oauth2.Token, error) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	ts.cachedToken = oauth2.Token{}
	return ts.token(ctx)
}

// token must be called with ts.lock held.
// At the time of writing this, the oauth2 package does not support the client assertion authentication method.
// Ref: https://github.com/golang/oauth2/issues/744
func (ts *OAuthTokenSourcePrivateKeyJWT) token(ctx context.Context) (*oauth2.Token, error) {
	if ts.cachedToken.Valid() {
		tok := ts.cachedToken
		return &tok, nil
	}

	if ts.TokenEndpoint.TokenURL == "" || ts.ClientID == "" {
		return nil, errors.New("cannot generate token, OAuthTokenSourcePrivateKeyJWT not fully configured")
	}

	if ts.ClientAssertionCert == "" || ts.ClientAssertionKey == "" {
		return nil, errors.New("private_key_jwt requires client assertion cert and key")
	}

	pk, err := pem.DecodePEMPrivateKey([]byte(ts.ClientAssertionKey))
	if err != nil {
		return nil, fmt.Errorf("unable to parse private key: %w", err)
	}
	rsaKey, ok := pk.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private_key_jwt requires RSA private key")
	}

	now := time.Now()

	audClaim := ts.TokenEndpoint.TokenURL
	if ts.Audience != "" {
		audClaim = ts.Audience
	}

	assertionToken, err := jwt.NewBuilder().
		Issuer(ts.ClientID).
		Subject(ts.ClientID).
		Audience([]string{audClaim}).
		IssuedAt(now).
		Expiration(now.Add(1 * time.Minute)).
		JwtID(uuid.New().String()).
		NotBefore(now).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build token: %w", err)
	}

	// Some IdPs require the audience to be set as a single string
	assertionToken.Options().Enable(jwt.FlattenAudience)

	var signOptions []jwt.Option
	if ts.Kid != "" {
		headers := jws.NewHeaders()
		if err = headers.Set("kid", ts.Kid); err != nil {
			return nil, fmt.Errorf("error setting JWT kid header: %w", err)
		}
		signOptions = append(signOptions, jws.WithProtectedHeaders(headers))
	}
	assertion, err := jwt.Sign(assertionToken, jwt.WithKey(jwa.RS256, rsaKey, signOptions...))
	if err != nil {
		return nil, fmt.Errorf("error signing client assertion: %w", err)
	}

	urlValues := &url.Values{}
	urlValues.Set("grant_type", "client_credentials")
	urlValues.Set("client_id", ts.ClientID)
	urlValues.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
	urlValues.Set("client_assertion", string(assertion))
	if ts.Audience != "" {
		urlValues.Set("audience", ts.Audience)
	}
	if ts.Resource != "" {
		urlValues.Set("resource", ts.Resource)
	}
	if len(ts.Scopes) > 0 {
		urlValues.Set("scope", strings.Join(ts.Scopes, " "))
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ts.configureClient()
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, ts.TokenEndpoint.TokenURL, strings.NewReader(urlValues.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// The token endpoint URL comes from trusted component metadata configured by the operator, not from untrusted input.
	resp, err := ts.httpClient.Do(req) //nolint:gosec,nolintlint
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("token endpoint returned %d", resp.StatusCode)
	}
	var tr oidcTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, err
	}
	if tr.AccessToken == "" {
		return nil, errors.New("no access_token in response")
	}
	ts.cachedToken = oauth2.Token{AccessToken: tr.AccessToken, Expiry: time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)}
	tok := ts.cachedToken
	return &tok, nil
}
