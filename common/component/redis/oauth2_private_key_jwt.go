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

// This file implements the OAuth2 private_key_jwt (RFC 7523) client
// authentication method for the Redis component. The shape mirrors
// common/component/kafka/sasl_oauthbearer_private_key_jwt.go — the JWT
// builder, token request, and response parser are equivalent. The key
// difference is that this token source is consumed by the Redis component:
// the returned access token is set as the AUTH password on the underlying
// Redis connection, and refreshed via go-redis's AuthACL on the live pool
// (see redis.go StartOIDCTokenRefreshBackgroundRoutine).

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
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"

	"github.com/dapr/kit/crypto/pem"
)

// OAuthTokenSourcePrivateKeyJWT is an OAuth2 token source that uses the
// private_key_jwt client authentication method (RFC 7523) to obtain an
// access token via the client_credentials grant.
//
// The shape mirrors the Kafka equivalent in
// common/component/kafka/sasl_oauthbearer_private_key_jwt.go.
type OAuthTokenSourcePrivateKeyJWT struct {
	CachedToken         oauth2.Token
	TokenEndpoint       oauth2.Endpoint
	ClientID            string
	Scopes              []string
	ClientAssertionCert string
	ClientAssertionKey  string
	Resource            string
	Audience            string
	Kid                 string

	httpClient *http.Client
	trustedCAs []*x509.Certificate
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

// getOIDCTokenSource constructs an OAuthTokenSourcePrivateKeyJWT from the
// Redis Settings. Scopes default to {"openid"} when not configured.
func (s *Settings) getOIDCTokenSource() (*OAuthTokenSourcePrivateKeyJWT, error) {
	scopes := []string{"openid"}
	if s.OidcScopes != "" {
		scopes = strings.Split(s.OidcScopes, ",")
		for i := range scopes {
			scopes[i] = strings.TrimSpace(scopes[i])
		}
	}

	ts := &OAuthTokenSourcePrivateKeyJWT{
		TokenEndpoint:       oauth2.Endpoint{TokenURL: s.OidcTokenEndpoint},
		ClientID:            s.OidcClientID,
		Scopes:              scopes,
		ClientAssertionCert: s.OidcClientAssertionCert,
		ClientAssertionKey:  s.OidcClientAssertionKey,
		Resource:            s.OidcResource,
		Audience:            s.OidcAudience,
		Kid:                 s.OidcKid,
	}

	if s.OidcCACert != "" {
		if err := ts.addCA(s.OidcCACert); err != nil {
			return nil, err
		}
	}

	return ts, nil
}

func (ts *OAuthTokenSourcePrivateKeyJWT) addCA(caPEM string) error {
	caCerts, err := pem.DecodePEMCertificates([]byte(caPEM))
	if err != nil {
		return fmt.Errorf("error parsing OIDC CA PEM certificate: %w", err)
	}
	if len(caCerts) == 0 {
		return errors.New("OIDC CA PEM contained no certificates")
	}
	if ts.trustedCAs == nil {
		ts.trustedCAs = make([]*x509.Certificate, 0, len(caCerts))
	}
	ts.trustedCAs = append(ts.trustedCAs, caCerts...)
	return nil
}

func (ts *OAuthTokenSourcePrivateKeyJWT) configureClient() {
	if ts.httpClient != nil {
		return
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if ts.trustedCAs != nil {
		caPool, err := x509.SystemCertPool()
		if err != nil || caPool == nil {
			caPool = x509.NewCertPool()
		}
		for _, c := range ts.trustedCAs {
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

// Token returns a valid access token, fetching a new one from the IDP if the
// cached token has expired or is unset. The returned string is the raw JWT
// suitable for use as a Redis AUTH password; the time is its expiry.
//
// At the time of writing, golang.org/x/oauth2 does not natively support the
// client_assertion authentication method; we build the form request by hand.
// Ref: https://github.com/golang/oauth2/issues/744
func (ts *OAuthTokenSourcePrivateKeyJWT) Token(ctx context.Context) (string, time.Time, error) {
	if ts.CachedToken.Valid() {
		return ts.CachedToken.AccessToken, ts.CachedToken.Expiry, nil
	}

	if ts.TokenEndpoint.TokenURL == "" || ts.ClientID == "" {
		return "", time.Time{}, errors.New("OIDC token source not fully configured (missing token endpoint or client ID)")
	}
	if ts.ClientAssertionCert == "" || ts.ClientAssertionKey == "" {
		return "", time.Time{}, errors.New("OIDC private_key_jwt requires client assertion cert and key")
	}

	pk, err := pem.DecodePEMPrivateKey([]byte(ts.ClientAssertionKey))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("unable to parse OIDC client assertion private key: %w", err)
	}
	rsaKey, ok := pk.(*rsa.PrivateKey)
	if !ok {
		return "", time.Time{}, errors.New("OIDC private_key_jwt requires an RSA private key")
	}

	now := time.Now()

	// Per RFC 7523 the audience claim should identify the token endpoint.
	// Some IDPs (e.g. JPMC ADFS) require an explicit audience value; the
	// OidcAudience setting overrides the default when configured.
	audClaim := ts.TokenEndpoint.TokenURL
	if ts.Audience != "" {
		audClaim = ts.Audience
	}

	token, err := jwt.NewBuilder().
		Issuer(ts.ClientID).
		Subject(ts.ClientID).
		Audience([]string{audClaim}).
		IssuedAt(now).
		Expiration(now.Add(1 * time.Minute)).
		JwtID(uuid.New().String()).
		NotBefore(now).
		Build()
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to build OIDC client assertion: %w", err)
	}

	// Emit the aud claim as a single JSON string rather than a one-element
	// array. Required by ADFS and other strict IDPs.
	token.Options().Enable(jwt.FlattenAudience)

	var signOptions []jwt.Option
	if ts.Kid != "" {
		headers := jws.NewHeaders()
		if err = headers.Set("kid", ts.Kid); err != nil {
			return "", time.Time{}, fmt.Errorf("error setting JWT kid header: %w", err)
		}
		signOptions = append(signOptions, jws.WithProtectedHeaders(headers))
	}
	assertion, err := jwt.Sign(token, jwt.WithKey(jwa.RS256, rsaKey, signOptions...))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("error signing OIDC client assertion: %w", err)
	}

	form := &url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", ts.ClientID)
	form.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
	form.Set("client_assertion", string(assertion))
	if ts.Audience != "" {
		form.Set("audience", ts.Audience)
	}
	if ts.Resource != "" {
		form.Set("resource", ts.Resource)
	}
	if len(ts.Scopes) > 0 {
		form.Set("scope", strings.Join(ts.Scopes, " "))
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ts.configureClient()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, ts.TokenEndpoint.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ts.httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("OIDC token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", time.Time{}, fmt.Errorf("OIDC token endpoint returned HTTP %d", resp.StatusCode)
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to decode OIDC token response: %w", err)
	}
	if tr.AccessToken == "" {
		return "", time.Time{}, errors.New("OIDC token response contained no access_token")
	}

	expiry := time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	ts.CachedToken = oauth2.Token{AccessToken: tr.AccessToken, Expiry: expiry}
	return tr.AccessToken, expiry, nil
}
