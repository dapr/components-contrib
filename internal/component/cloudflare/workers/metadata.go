/*
Copyright 2022 The Dapr Authors
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

package workers

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// Base metadata struct, common to all components
// The components can be initialized in two ways:
// - Instantiate the component with a "workerURL": assumes a worker that has been pre-deployed and it's ready to be used; we will not need API tokens
// - Instantiate the component with a "cfAPIToken" and "cfAccountID": Dapr will take care of creating the worker if it doesn't exist (or upgrade it if needed)
type BaseMetadata struct {
	WorkerURL   string `mapstructure:"workerUrl"`
	CfAPIToken  string `mapstructure:"cfAPIToken"`
	CfAccountID string `mapstructure:"cfAccountID"`
	Key         string `mapstructure:"key"`
	WorkerName  string `mapstructure:"workerName"`

	privKey ed25519.PrivateKey
}

// Validate the metadata object.
func (m *BaseMetadata) Validate() error {
	// Option 1: check if we have a workerURL
	if m.WorkerURL != "" {
		u, err := url.Parse(m.WorkerURL)
		if err != nil {
			return fmt.Errorf("invalid property 'workerUrl': %w", err)
		}
		if u.Scheme != "https" && u.Scheme != "http" {
			return errors.New("invalid property 'workerUrl': unsupported scheme")
		}
		// Re-set the URL to make sure it's sanitized
		m.WorkerURL = u.String()
		if !strings.HasSuffix(m.WorkerURL, "/") {
			m.WorkerURL += "/"
		}
	} else if m.CfAPIToken == "" || m.CfAccountID == "" {
		// Option 2: we need cfAPIToken and cfAccountID
		return errors.New("invalid component metadata: either 'workerUrl' or the combination of 'cfAPIToken'/'cfAccountID' is required")
	}

	// WorkerName
	if m.WorkerName == "" {
		return errors.New("property 'workerName' is required")
	}

	// Key
	if m.Key == "" {
		return errors.New("property 'key' is required")
	}
	block, _ := pem.Decode([]byte(m.Key))
	if block == nil || len(block.Bytes) == 0 {
		return errors.New("invalid property 'key': not a PEM-encoded key")
	}
	if block.Type != "PRIVATE KEY" {
		return errors.New("invalid property 'key': not a private key in PKCS#8 format")
	}
	pkAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("invalid property 'key': failed to import private key: %w", err)
	}
	pk, ok := pkAny.(ed25519.PrivateKey)
	if !ok {
		return errors.New("invalid property 'key': not an Ed25519 private key")
	}
	m.privKey = pk

	return nil
}

// CreateToken creates a JWT token for authorizing requests
func (m BaseMetadata) CreateToken() (string, error) {
	now := time.Now()
	token, err := jwt.NewBuilder().
		Audience([]string{m.WorkerName}).
		Issuer(tokenIssuer).
		IssuedAt(now).
		Expiration(now.Add(tokenExpiration)).
		Build()
	if err != nil {
		return "", fmt.Errorf("failed to build token: %w", err)
	}

	signed, err := jwt.Sign(token, jwt.WithKey(jwa.EdDSA, m.privKey))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return string(signed), nil
}
