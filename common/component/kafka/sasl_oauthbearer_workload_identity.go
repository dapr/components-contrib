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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dapr/kit/crypto/pem"

	"github.com/IBM/sarama"
	"golang.org/x/oauth2"
)

const (
	// Default path where Azure Workload Identity projects the SA token.
	defaultAzureFederatedTokenFile = "/var/run/secrets/azure/tokens/azure-identity-token"

	// Environment variable names injected by the Azure Workload Identity webhook.
	envAzureClientID            = "AZURE_CLIENT_ID"
	envAzureTenantID            = "AZURE_TENANT_ID"
	envAzureFederatedTokenFile  = "AZURE_FEDERATED_TOKEN_FILE"
	envAzureAuthorityHost       = "AZURE_AUTHORITY_HOST"
)

// OAuthTokenSourceWorkloadIdentity implements sarama.AccessTokenProvider using
// Azure Workload Identity (or similar projected SA token) for SASL/OAUTHBEARER.
//
// It reads a short-lived service account token from the filesystem,
// then exchanges it with the IdP (e.g., Entra ID) for an access token
// using the client_credentials grant with a client_assertion.
type OAuthTokenSourceWorkloadIdentity struct {
	CachedToken   oauth2.Token
	Extensions    map[string]string
	TokenEndpoint string
	ClientID      string
	TokenFilePath string
	Scopes        []string
	httpClient    *http.Client
	trustedCas    []*x509.Certificate
	skipCaVerify  bool
}

func (m KafkaMetadata) getOAuthTokenSourceWorkloadIdentity() *OAuthTokenSourceWorkloadIdentity {
	tokenEndpoint := m.OidcTokenEndpoint
	clientID := m.OidcClientID
	tokenFilePath := m.OidcTokenFilePath
	scopes := m.internalOidcScopes

	// Fall back to Azure Workload Identity environment variables.
	if clientID == "" {
		clientID = os.Getenv(envAzureClientID)
	}
	if tokenFilePath == "" {
		tokenFilePath = os.Getenv(envAzureFederatedTokenFile)
	}
	if tokenFilePath == "" {
		tokenFilePath = defaultAzureFederatedTokenFile
	}
	if tokenEndpoint == "" {
		authorityHost := os.Getenv(envAzureAuthorityHost)
		tenantID := os.Getenv(envAzureTenantID)
		if authorityHost != "" && tenantID != "" {
			tokenEndpoint = strings.TrimRight(authorityHost, "/") + "/" + tenantID + "/oauth2/v2.0/token"
		}
	}

	// Default scope: api://<clientID>/.default (Azure convention).
	if len(scopes) == 0 && clientID != "" {
		scopes = []string{"api://" + clientID + "/.default"}
	}

	return &OAuthTokenSourceWorkloadIdentity{
		TokenEndpoint: tokenEndpoint,
		ClientID:      clientID,
		TokenFilePath: tokenFilePath,
		Scopes:        scopes,
		Extensions:    m.internalOidcExtensions,
		skipCaVerify:  m.TLSSkipVerify,
	}
}

func (ts *OAuthTokenSourceWorkloadIdentity) addCa(caPem string) error {
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

func (ts *OAuthTokenSourceWorkloadIdentity) configureClient() {
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

// Token implements sarama.AccessTokenProvider. It reads the projected SA token
// from the file system and exchanges it with the IdP for an access token.
func (ts *OAuthTokenSourceWorkloadIdentity) Token() (*sarama.AccessToken, error) {
	if ts.CachedToken.Valid() {
		return ts.asSaramaToken(), nil
	}

	if ts.TokenEndpoint == "" || ts.ClientID == "" {
		return nil, errors.New("kafka: oidc_workload_identity not fully configured: tokenEndpoint and clientID are required")
	}

	if ts.TokenFilePath == "" {
		return nil, errors.New("kafka: oidc_workload_identity: token file path is empty")
	}

	// Read the projected service account token (re-read each time since it rotates).
	assertionBytes, err := os.ReadFile(ts.TokenFilePath)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to read federated token file %q: %w", ts.TokenFilePath, err)
	}
	assertion := strings.TrimSpace(string(assertionBytes))
	if assertion == "" {
		return nil, fmt.Errorf("kafka: federated token file %q is empty", ts.TokenFilePath)
	}

	// Exchange the SA token for an access token.
	urlValues := &url.Values{}
	urlValues.Set("grant_type", "client_credentials")
	urlValues.Set("client_id", ts.ClientID)
	urlValues.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
	urlValues.Set("client_assertion", assertion)
	if len(ts.Scopes) > 0 {
		urlValues.Set("scope", strings.Join(ts.Scopes, " "))
	}

	timeoutCtx, cancel := ctx.WithTimeout(ctx.TODO(), 30*time.Second)
	defer cancel()

	ts.configureClient()
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, ts.TokenEndpoint, strings.NewReader(urlValues.Encode()))
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ts.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kafka: token exchange request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("kafka: token endpoint returned HTTP %d", resp.StatusCode)
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, fmt.Errorf("kafka: failed to decode token response: %w", err)
	}
	if tr.AccessToken == "" {
		return nil, errors.New("kafka: no access_token in token response")
	}

	ts.CachedToken = oauth2.Token{
		AccessToken: tr.AccessToken,
		Expiry:      time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second),
	}

	return ts.asSaramaToken(), nil
}

func (ts *OAuthTokenSourceWorkloadIdentity) asSaramaToken() *sarama.AccessToken {
	return &sarama.AccessToken{Token: ts.CachedToken.AccessToken, Extensions: ts.Extensions}
}
