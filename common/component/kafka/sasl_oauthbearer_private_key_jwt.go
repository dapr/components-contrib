/*
Copyright 2025 The Dapr Authors
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

package kafka

import (
	ctx "context"
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

	"github.com/dapr/kit/crypto/pem"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"
)

type OAuthTokenSourcePrivateKeyJWT struct {
	CachedToken         oauth2.Token
	Extensions          map[string]string
	TokenEndpoint       oauth2.Endpoint
	ClientID            string
	ClientSecret        string
	Scopes              []string
	httpClient          *http.Client
	trustedCas          []*x509.Certificate
	skipCaVerify        bool
	ClientAuthMethod    string
	ClientAssertionCert string
	ClientAssertionKey  string
	Resource            string
	Audience            string
	Kid                 string
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

func (m KafkaMetadata) getOAuthTokenSourcePrivateKeyJWT() *OAuthTokenSourcePrivateKeyJWT {
	return &OAuthTokenSourcePrivateKeyJWT{
		TokenEndpoint:       oauth2.Endpoint{TokenURL: m.OidcTokenEndpoint},
		ClientID:            m.OidcClientID,
		ClientSecret:        m.OidcClientSecret,
		Scopes:              m.internalOidcScopes,
		Extensions:          m.internalOidcExtensions,
		skipCaVerify:        m.TLSSkipVerify,
		ClientAuthMethod:    m.AuthType,
		ClientAssertionCert: m.OidcClientAssertionCert,
		ClientAssertionKey:  m.OidcClientAssertionKey,
		Resource:            m.OidcResource,
		Audience:            m.OidcAudience,
		Kid:                 m.OidcKid,
	}
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

// At the time of writing this, the oauth2 package does not support the client assertion authentication method.
// Ref: https://github.com/golang/oauth2/issues/744
func (ts *OAuthTokenSourcePrivateKeyJWT) Token() (*sarama.AccessToken, error) {
	if ts.CachedToken.Valid() {
		return ts.asSaramaToken(), nil
	}

	if ts.TokenEndpoint.TokenURL == "" || ts.ClientID == "" {
		return nil, errors.New("cannot generate token, OAuthTokenSourcePrivateKeyJWT not fully configured")
	}

	if ts.ClientAssertionCert == "" || ts.ClientAssertionKey == "" {
		return nil, errors.New("client_jwt requires client assertion cert and key")
	}

	pk, err := pem.DecodePEMPrivateKey([]byte(ts.ClientAssertionKey))
	if err != nil {
		return nil, fmt.Errorf("unable to parse private key: %w", err)
	}
	rsaKey, ok := pk.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("client_jwt requires RSA private key")
	}

	now := time.Now()
	aud := ts.TokenEndpoint.TokenURL

	audClaim := aud
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
		return nil, fmt.Errorf("failed to build token: %w", err)
	}

	// Some IdPs require the audience to be set as a single string
	token.Options().Enable(jwt.FlattenAudience)

	var signOptions []jwt.Option
	if ts.Kid != "" {
		headers := jws.NewHeaders()
		if err = headers.Set("kid", ts.Kid); err != nil {
			return nil, fmt.Errorf("error setting JWT kid header: %w", err)
		}
		signOptions = append(signOptions, jws.WithProtectedHeaders(headers))
	}
	assertion, err := jwt.Sign(token, jwt.WithKey(jwa.RS256, rsaKey, signOptions...))
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

	timeoutCtx, cancel := ctx.WithTimeout(ctx.TODO(), 30*time.Second)
	defer cancel()
	ts.configureClient()
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, ts.TokenEndpoint.TokenURL, strings.NewReader(urlValues.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := ts.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("token endpoint returned %d", resp.StatusCode)
	}
	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, err
	}
	if tr.AccessToken == "" {
		return nil, errors.New("no access_token in response")
	}
	ts.CachedToken = oauth2.Token{AccessToken: tr.AccessToken, Expiry: time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)}
	return ts.asSaramaToken(), nil
}

func (ts *OAuthTokenSourcePrivateKeyJWT) asSaramaToken() *sarama.AccessToken {
	return &(sarama.AccessToken{Token: ts.CachedToken.AccessToken, Extensions: ts.Extensions})
}
