/*
Copyright 2021 The Dapr Authors
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

package oauth2

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	ccreds "golang.org/x/oauth2/clientcredentials"

	"github.com/dapr/kit/logger"
)

// ClientCredentialsMetadata is the metadata fields which can be used by a
// component to configure an OIDC client_credentials token source.
type ClientCredentialsMetadata struct {
	TokenCAPEM          string   `mapstructure:"oauth2TokenCAPEM"`
	TokenURL            string   `mapstructure:"oauth2TokenURL"`
	ClientID            string   `mapstructure:"oauth2ClientID"`
	ClientSecret        string   `mapstructure:"oauth2ClientSecret"`
	ClientSecretPath    string   `mapstructure:"oauth2ClientSecretPath"`
	CredentialsFilePath string   `mapstructure:"oauth2CredentialsFile"`
	Audiences           []string `mapstructure:"oauth2Audiences"`
	Scopes              []string `mapstructure:"oauth2Scopes"`
}

// ResolveCredentials loads client_id and client_secret from files if configured.
func (m *ClientCredentialsMetadata) ResolveCredentials() error {
	if m.CredentialsFilePath != "" && m.ClientSecretPath != "" {
		return errors.New("'oauth2CredentialsFile' and 'oauth2ClientSecretPath' fields are mutually exclusive")
	}

	if m.CredentialsFilePath != "" {
		fileClientID, fileClientSecret, fileIssuerURL, err := LoadCredentialsFromJSONFile(m.CredentialsFilePath)
		if err != nil {
			return fmt.Errorf("failed to load credentials from JSON file: %w", err)
		}

		// Metadata overrides file values
		if m.ClientID == "" {
			m.ClientID = fileClientID
		}
		if m.ClientSecret == "" {
			m.ClientSecret = fileClientSecret
		}
		if m.TokenURL == "" {
			m.TokenURL = fileIssuerURL
		}
		return nil
	}

	if m.ClientSecretPath != "" {
		// Metadata overrides file value
		if m.ClientSecret == "" {
			secretBytes, err := os.ReadFile(m.ClientSecretPath)
			if err != nil {
				return fmt.Errorf("could not read oauth2 client secret from file %q: %w", m.ClientSecretPath, err)
			}
			m.ClientSecret = strings.TrimSpace(string(secretBytes))
		}
		return nil
	}

	return nil
}

// ToOptions converts ClientCredentialsMetadata to ClientCredentialsOptions.
func (m *ClientCredentialsMetadata) ToOptions(logger logger.Logger) ClientCredentialsOptions {
	return ClientCredentialsOptions{
		Logger:       logger,
		TokenURL:     m.TokenURL,
		CAPEM:        []byte(m.TokenCAPEM),
		ClientID:     m.ClientID,
		ClientSecret: m.ClientSecret,
		Scopes:       m.Scopes,
		Audiences:    m.Audiences,
	}
}

// CredentialsFile represents a JSON credentials file.
type CredentialsFile struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	IssuerURL    string `json:"issuer_url"`
}

// LoadCredentialsFromJSONFile reads client_id, client_secret, and issuer_url from a JSON file.
func LoadCredentialsFromJSONFile(filePath string) (clientID, clientSecret, issuerURL string, err error) {
	secretBytes, err := os.ReadFile(filePath)
	if err != nil {
		return "", "", "", fmt.Errorf("could not read oauth2 credentials from file %q: %w", filePath, err)
	}

	var creds CredentialsFile
	if err := json.Unmarshal(secretBytes, &creds); err != nil {
		return "", "", "", fmt.Errorf("failed to parse JSON credentials file: %w", err)
	}

	if creds.ClientID == "" || creds.ClientSecret == "" || creds.IssuerURL == "" {
		return "", "", "", errors.New("credentials file must contain client_id, client_secret, and issuer_url")
	}

	return creds.ClientID, creds.ClientSecret, creds.IssuerURL, nil
}

type ClientCredentialsOptions struct {
	Logger       logger.Logger
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string
	Audiences    []string
	CAPEM        []byte
}

// ClientCredentials is an OAuth2 Token Source that uses the client_credentials
// grant type to fetch a token.
type ClientCredentials struct {
	log          logger.Logger
	currentToken *oauth2.Token
	httpClient   *http.Client
	fetchTokenFn func(context.Context) (*oauth2.Token, error)

	lock sync.Mutex
}

func NewClientCredentials(ctx context.Context, opts ClientCredentialsOptions) (*ClientCredentials, error) {
	conf, httpClient, err := opts.toConfig()
	if err != nil {
		return nil, err
	}

	token, err := conf.Token(context.WithValue(ctx, oauth2.HTTPClient, httpClient))
	if err != nil {
		return nil, fmt.Errorf("error fetching initial oauth2 client_credentials token: %w", err)
	}

	opts.Logger.Info("Fetched initial oauth2 client_credentials token")

	return &ClientCredentials{
		log:          opts.Logger,
		currentToken: token,
		httpClient:   httpClient,
		fetchTokenFn: conf.Token,
	}, nil
}

func (c *ClientCredentialsOptions) toConfig() (*ccreds.Config, *http.Client, error) {
	if len(c.Scopes) == 0 {
		return nil, nil, errors.New("oauth2 client_credentials token source requires at least one scope")
	}

	if len(c.Audiences) == 0 {
		return nil, nil, errors.New("oauth2 client_credentials token source requires at least one audience")
	}

	_, err := url.Parse(c.TokenURL)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing token URL: %w", err)
	}

	conf := &ccreds.Config{
		ClientID:       c.ClientID,
		ClientSecret:   c.ClientSecret,
		TokenURL:       c.TokenURL,
		Scopes:         c.Scopes,
		EndpointParams: url.Values{"audience": c.Audiences},
	}

	// If caPool is nil, then the Go TLS library will use the system's root CA.
	var caPool *x509.CertPool
	if len(c.CAPEM) > 0 {
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(c.CAPEM) {
			return nil, nil, errors.New("failed to parse CA PEM")
		}
	}

	return conf, &http.Client{
		Timeout: time.Second * 30,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    caPool,
			},
		},
	}, nil
}

func (c *ClientCredentials) Token() (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.currentToken.Valid() {
		return c.currentToken.AccessToken, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := c.renewToken(ctx); err != nil {
		return "", err
	}
	return c.currentToken.AccessToken, nil
}

func (c *ClientCredentials) renewToken(ctx context.Context) error {
	c.log.Debug("renewing token: fetching new token from OAuth server...")
	token, err := c.fetchTokenFn(context.WithValue(ctx, oauth2.HTTPClient, c.httpClient))
	if err != nil {
		return err
	}

	if !token.Valid() {
		return errors.New("oauth2 client_credentials token source returned an invalid token")
	}

	c.currentToken = token
	c.log.Debugf("OAuth token renewed successfully, new expiry: %s", token.Expiry)
	return nil
}
