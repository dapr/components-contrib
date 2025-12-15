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
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	ccreds "golang.org/x/oauth2/clientcredentials"
)

func Test_toConfig(t *testing.T) {
	tests := map[string]struct {
		opts      ClientCredentialsOptions
		expConfig *ccreds.Config
		expErr    bool
	}{
		"no scopes should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "https://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
			},
			expErr: true,
		},
		"bad URL endpoint should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "&&htp:/f url",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
				Scopes:       []string{"foo"},
			},
			expErr: true,
		},
		"bad CA certificate should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "http://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
				Scopes:       []string{"foo"},
				CAPEM:        []byte("ca-pem"),
			},
			expErr: true,
		},
		"no audiences should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "http://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Scopes:       []string{"foo"},
			},
			expErr: true,
		},
		"should default scope": {
			opts: ClientCredentialsOptions{
				TokenURL:     "http://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
				Scopes:       []string{"foo", "bar"},
			},
			expConfig: &ccreds.Config{
				ClientID:       "client-id",
				ClientSecret:   "client-secret",
				TokenURL:       "http://localhost:8080",
				Scopes:         []string{"foo", "bar"},
				EndpointParams: url.Values{"audience": []string{"audience"}},
			},
			expErr: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			config, _, err := test.opts.toConfig()
			assert.Equalf(t, test.expErr, err != nil, "%v", err)
			assert.Equal(t, test.expConfig, config)
		})
	}
}

func Test_TokenRenewal(t *testing.T) {
	expired := &oauth2.Token{AccessToken: "old-token", Expiry: time.Now().Add(-1 * time.Minute)}
	renewed := &oauth2.Token{AccessToken: "new-token", Expiry: time.Now().Add(1 * time.Hour)}

	c := &ClientCredentials{
		currentToken: expired,
		fetchTokenFn: func(ctx context.Context) (*oauth2.Token, error) {
			return renewed, nil
		},
	}

	tok, err := c.Token()
	require.NoError(t, err)
	assert.Equal(t, "new-token", tok)
}
