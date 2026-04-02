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

package zeebe

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

var ErrMissingGatewayAddr = errors.New("gatewayAddr is a required attribute")

var ErrInvalidOAuthMetadata = errors.New("invalid OAuth metadata")

// ClientFactory enables injection for testing.
type ClientFactory interface {
	Get(metadata bindings.Metadata) (zbc.Client, error)
}

type ClientFactoryImpl struct {
	logger logger.Logger
}

// https://docs.zeebe.io/operations/authentication.html
type ClientMetadata struct {
	GatewayAddr            string        `json:"gatewayAddr" mapstructure:"gatewayAddr"`
	GatewayKeepAlive       time.Duration `json:"gatewayKeepAlive" mapstructure:"gatewayKeepAlive"`
	CaCertificatePath      string        `json:"caCertificatePath" mapstructure:"caCertificatePath"`
	UsePlaintextConnection bool          `json:"usePlainTextConnection,string" mapstructure:"usePlainTextConnection"`
	ClientID               string        `json:"clientId" mapstructure:"clientId"`
	ClientSecret           string        `json:"clientSecret" mapstructure:"clientSecret"`
	AuthorizationServerURL string        `json:"authorizationServerUrl" mapstructure:"authorizationServerUrl"`
	TokenAudience          string        `json:"tokenAudience" mapstructure:"tokenAudience"`
	TokenScope             string        `json:"tokenScope" mapstructure:"tokenScope"`
	ClientConfigPath       string        `json:"clientConfigPath" mapstructure:"clientConfigPath"`
}

// NewClientFactoryImpl returns a new ClientFactory instance.
func NewClientFactoryImpl(logger logger.Logger) *ClientFactoryImpl {
	return &ClientFactoryImpl{logger: logger}
}

func (c *ClientFactoryImpl) Get(metadata bindings.Metadata) (zbc.Client, error) {
	meta, err := c.parseMetadata(metadata)
	if err != nil {
		return nil, err
	}

	credentialsProvider, err := meta.newCredentialsProvider()
	if err != nil {
		return nil, err
	}

	client, err := zbc.NewClient(&zbc.ClientConfig{
		GatewayAddress:         meta.GatewayAddr,
		UsePlaintextConnection: meta.UsePlaintextConnection,
		CaCertificatePath:      meta.CaCertificatePath,
		KeepAlive:              meta.GatewayKeepAlive,
		CredentialsProvider:    credentialsProvider,
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *ClientFactoryImpl) parseMetadata(meta bindings.Metadata) (*ClientMetadata, error) {
	var m ClientMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.GatewayAddr == "" {
		return nil, ErrMissingGatewayAddr
	}

	return &m, nil
}

func (m *ClientMetadata) oauthConfigured() bool {
	return m.ClientID != "" ||
		m.ClientSecret != "" ||
		m.AuthorizationServerURL != "" ||
		m.TokenAudience != "" ||
		m.TokenScope != "" ||
		m.ClientConfigPath != ""
}

func (m *ClientMetadata) validateOAuthMetadata() error {
	if !m.oauthConfigured() {
		return nil
	}

	missing := make([]string, 0, 4)
	if m.ClientID == "" {
		missing = append(missing, "clientId")
	}
	if m.ClientSecret == "" {
		missing = append(missing, "clientSecret")
	}
	if m.AuthorizationServerURL == "" {
		missing = append(missing, "authorizationServerUrl")
	}
	if m.TokenAudience == "" {
		missing = append(missing, "tokenAudience")
	}

	if len(missing) > 0 {
		return fmt.Errorf("%w: when OAuth is configured, clientId, clientSecret, authorizationServerUrl, and tokenAudience must all be provided; missing: %s", ErrInvalidOAuthMetadata, strings.Join(missing, ", "))
	}

	return nil
}

func (m *ClientMetadata) newCredentialsProvider() (zbc.CredentialsProvider, error) {
	if err := m.validateOAuthMetadata(); err != nil {
		return nil, err
	}

	if !m.oauthConfigured() {
		return nil, nil
	}

	providerConfig := &zbc.OAuthProviderConfig{
		ClientID:               m.ClientID,
		ClientSecret:           m.ClientSecret,
		Audience:               m.TokenAudience,
		Scope:                  m.TokenScope,
		AuthorizationServerURL: m.AuthorizationServerURL,
	}

	if m.ClientConfigPath != "" {
		cache, err := zbc.NewOAuthYamlCredentialsCache(m.ClientConfigPath)
		if err != nil {
			return nil, err
		}

		providerConfig.Cache = cache
	}

	return zbc.NewOAuthCredentialsProvider(providerConfig)
}
