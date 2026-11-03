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
package keyvault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

type secretTransport func(*http.Request) (*http.Response, error)

func (s secretTransport) Do(req *http.Request) (*http.Response, error) {
	return s(req)
}

func secretResponse(t *testing.T, req *http.Request, status int, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(string(data))),
		Request:    req,
	}
}

type secretCredential struct{}

func (secretCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "test-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func newTestSecretStore(t *testing.T, transport secretTransport) *keyvaultSecretStore {
	t.Helper()
	client, err := azsecrets.NewClient("https://test-vault.vault.azure.net", secretCredential{}, &azsecrets.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: transport,
			Retry:     policy.RetryOptions{MaxRetries: -1},
		},
	})
	require.NoError(t, err)
	return &keyvaultSecretStore{
		vaultClient:    client,
		vaultName:      "test-vault",
		vaultDNSSuffix: "vault.azure.net",
	}
}

func TestGetSecretValue(t *testing.T) {
	values := []struct {
		name  string
		value string
	}{
		{"plain", "actual-secret-value"},
		{"empty", ""},
		{"whitespace", " \t\r\n "},
		{"JSON object", `{"value":"inner","metadata":{"enabled":true}}`},
		{"JSON array", `["first",{"value":"second"}]`},
		{"JSON string", `"quoted-secret"`},
		{"JSON null", "null"},
		{"reference-shaped JSON", `{"uri":"https://another-vault.vault.azure.net/secrets/other"}`},
		{"multiline", "line1\nline2\r\nline3\n"},
		{"escaped and control characters", "quotes=\" backslash=\\ tab=\t nul=\x00"},
		{"Unicode", "caf\u00e9 \u79d8\u5bc6 \U0001f512"},
		{"long", strings.Repeat("secret-", 3000)},
	}
	versions := []struct {
		name     string
		metadata map[string]string
		path     string
	}{
		{"latest with nil metadata", nil, "/secrets/password/"},
		{"latest with empty metadata", map[string]string{}, "/secrets/password/"},
		{"latest with empty version", map[string]string{VersionID: ""}, "/secrets/password/"},
		{"pinned version", map[string]string{VersionID: "version1"}, "/secrets/password/version1"},
	}
	for _, value := range values {
		t.Run(value.name, func(t *testing.T) {
			for _, version := range versions {
				t.Run(version.name, func(t *testing.T) {
					calls := 0
					s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
						calls++
						assert.Equal(t, http.MethodGet, req.Method)
						assert.Equal(t, "test-vault.vault.azure.net", req.URL.Host)
						assert.Equal(t, version.path, req.URL.Path)
						assert.NotEmpty(t, req.URL.Query().Get("api-version"))
						return secretResponse(t, req, http.StatusOK, map[string]any{
							"value":       value.value,
							"id":          "https://test-vault.vault.azure.net/secrets/password/version1",
							"contentType": "application/json",
							"attributes":  map[string]bool{"enabled": true},
						}), nil
					})
					res, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "password", Metadata: version.metadata})
					require.NoError(t, err)
					assert.Equal(t, map[string]string{"password": value.value}, res.Data)
					assert.Equal(t, 1, calls)
				})
			}
		})
	}

	for _, body := range []map[string]any{
		{"value": nil},
		{},
	} {
		t.Run("missing or null SDK value", func(t *testing.T) {
			s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
				return secretResponse(t, req, http.StatusOK, body), nil
			})
			res, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "other-secret"})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"other-secret": ""}, res.Data)
		})
	}
}

func TestGetSecretErrors(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
				return secretResponse(t, req, status, map[string]any{
					"error": map[string]string{"code": http.StatusText(status), "message": "secret cannot be retrieved"},
				}), nil
			})
			res, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "password"})
			var responseErr *azcore.ResponseError
			require.ErrorAs(t, err, &responseErr)
			assert.Equal(t, status, responseErr.StatusCode)
			assert.Nil(t, res.Data)
		})
	}
	t.Run("transport failure", func(t *testing.T) {
		transportErr := errors.New("transport unavailable")
		s := newTestSecretStore(t, func(*http.Request) (*http.Response, error) {
			return nil, transportErr
		})
		res, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "password"})
		require.ErrorIs(t, err, transportErr)
		assert.Nil(t, res.Data)
	})
	t.Run("invalid value type", func(t *testing.T) {
		s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
			return secretResponse(t, req, http.StatusOK, map[string]any{"value": map[string]string{"value": "not a string"}}), nil
		})
		res, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "password"})
		require.Error(t, err)
		assert.Nil(t, res.Data)
	})
	for _, canceled := range []bool{true, false} {
		t.Run(map[bool]string{true: "canceled", false: "deadline exceeded"}[canceled], func(t *testing.T) {
			s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
				require.Error(t, req.Context().Err())
				return nil, req.Context().Err()
			})
			ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			wantErr := context.DeadlineExceeded
			if canceled {
				cancel()
				ctx, cancel = context.WithCancel(t.Context())
				cancel()
				wantErr = context.Canceled
			}
			defer cancel()
			res, err := s.GetSecret(ctx, secretstores.GetSecretRequest{Name: "password"})
			require.ErrorIs(t, err, wantErr)
			assert.Nil(t, res.Data)
		})
	}
}

func TestBulkGetSecretValues(t *testing.T) {
	values := map[string]any{
		"plain":      "actual-secret-value",
		"structured": `{"value":"inner","uri":"https://example.invalid"}`,
		"multiline":  "line1\nline2\r\n",
		"unicode":    "caf\u00e9 \u79d8\u5bc6 \U0001f512",
		"empty":      "",
		"null":       nil,
	}
	property := func(name string, attributes any) map[string]any {
		return map[string]any{
			"id":         "https://test-vault.vault.azure.net/secrets/" + name,
			"attributes": attributes,
		}
	}
	requests := []string{}
	s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.URL.Path+"?page="+req.URL.Query().Get("page"))
		assert.Equal(t, http.MethodGet, req.Method)
		if req.URL.Path == "/secrets" {
			var body map[string]any
			switch req.URL.Query().Get("page") {
			case "":
				body = map[string]any{
					"value": []any{
						property("plain", map[string]bool{"enabled": true}),
						property("disabled", map[string]bool{"enabled": false}),
						property("no-attributes", nil),
						property("no-enabled", map[string]any{}),
					},
					"nextLink": "https://test-vault.vault.azure.net/secrets?page=2",
				}
			case "2":
				body = map[string]any{
					"value":    []any{},
					"nextLink": "https://test-vault.vault.azure.net/secrets?page=3",
				}
			case "3":
				properties := []any{}
				for _, name := range []string{"structured", "multiline", "unicode", "empty", "null"} {
					properties = append(properties, property(name, map[string]bool{"enabled": true}))
				}
				body = map[string]any{"value": properties}
			default:
				t.Fatalf("unexpected page: %s", req.URL)
			}
			return secretResponse(t, req, http.StatusOK, body), nil
		}
		name := strings.TrimSuffix(strings.TrimPrefix(req.URL.Path, "/secrets/"), "/")
		value, ok := values[name]
		require.True(t, ok, "unexpected secret fetch: %s", req.URL)
		assert.Equal(t, "/secrets/"+name+"/", req.URL.Path)
		return secretResponse(t, req, http.StatusOK, map[string]any{
			"value": value,
			"id":    "https://test-vault.vault.azure.net/secrets/" + name + "/latest-version",
		}), nil
	})
	res, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.NoError(t, err)
	assert.Equal(t, map[string]map[string]string{
		"plain":      {"plain": "actual-secret-value"},
		"structured": {"structured": `{"value":"inner","uri":"https://example.invalid"}`},
		"multiline":  {"multiline": "line1\nline2\r\n"},
		"unicode":    {"unicode": "caf\u00e9 \u79d8\u5bc6 \U0001f512"},
		"empty":      {"empty": ""},
		"null":       {"null": ""},
	}, res.Data)
	assert.Equal(t, []string{
		"/secrets?page=", "/secrets/plain/?page=", "/secrets?page=2", "/secrets?page=3",
		"/secrets/structured/?page=", "/secrets/multiline/?page=", "/secrets/unicode/?page=",
		"/secrets/empty/?page=", "/secrets/null/?page=",
	}, requests)
}

func TestBulkGetSecretErrors(t *testing.T) {
	for _, stage := range []string{"first page", "next page", "secret fetch"} {
		t.Run(stage, func(t *testing.T) {
			s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
				fail := stage == "first page" || (stage == "next page" && req.URL.Query().Get("page") == "2") ||
					(stage == "secret fetch" && req.URL.Path == "/secrets/password/")
				if fail {
					return secretResponse(t, req, http.StatusForbidden, map[string]any{
						"error": map[string]string{"code": "Forbidden", "message": "access denied"},
					}), nil
				}
				if req.URL.Path == "/secrets" {
					return secretResponse(t, req, http.StatusOK, map[string]any{
						"value": []any{
							map[string]any{"id": "https://test-vault.vault.azure.net/secrets/password", "attributes": map[string]bool{"enabled": true}},
						},
						"nextLink": "https://test-vault.vault.azure.net/secrets?page=2",
					}), nil
				}
				return secretResponse(t, req, http.StatusOK, map[string]string{"value": "must not escape as a partial result"}), nil
			})
			res, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
			var responseErr *azcore.ResponseError
			require.ErrorAs(t, err, &responseErr)
			assert.Equal(t, http.StatusForbidden, responseErr.StatusCode)
			assert.Nil(t, res.Data)
		})
	}
	t.Run("invalid maxresults", func(t *testing.T) {
		s := newTestSecretStore(t, func(*http.Request) (*http.Response, error) {
			t.Error("invalid metadata must not reach the transport")
			return nil, errors.New("unexpected request")
		})
		res, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{Metadata: map[string]string{"maxresults": "invalid"}})
		require.Error(t, err)
		assert.Nil(t, res.Data)
	})
}

func TestBulkGetSecretMaxResults(t *testing.T) {
	calls := 0
	s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Path == "/secrets" {
			assert.Empty(t, req.URL.Query().Get("page"))
			return secretResponse(t, req, http.StatusOK, map[string]any{
				"value": []any{
					map[string]any{"id": "https://test-vault.vault.azure.net/secrets/password", "attributes": map[string]bool{"enabled": true}},
				},
				"nextLink": "https://test-vault.vault.azure.net/secrets?page=2",
			}), nil
		}
		assert.Equal(t, "/secrets/password/", req.URL.Path)
		return secretResponse(t, req, http.StatusOK, map[string]string{"value": "only value"}), nil
	})
	res, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{Metadata: map[string]string{"maxresults": "1"}})
	require.NoError(t, err)
	assert.Equal(t, map[string]map[string]string{"password": {"password": "only value"}}, res.Data)
	assert.Equal(t, 2, calls)
}

func TestBulkGetSecretEmpty(t *testing.T) {
	for _, metadata := range []map[string]string{nil, {"maxresults": ""}} {
		t.Run("empty list", func(t *testing.T) {
			calls := 0
			s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
				calls++
				assert.Equal(t, "/secrets", req.URL.Path)
				return secretResponse(t, req, http.StatusOK, map[string]any{"value": []any{}}), nil
			})
			res, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{Metadata: metadata})
			require.NoError(t, err)
			assert.Equal(t, map[string]map[string]string{}, res.Data)
			assert.Equal(t, 1, calls)
		})
	}
}

func TestBulkGetSecretTransportAndCancellation(t *testing.T) {
	for _, secretFetch := range []bool{false, true} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("secretFetch=%v/canceled=%v", secretFetch, canceled), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				wantErr := errors.New("transport unavailable")
				if canceled {
					wantErr = context.Canceled
				}
				s := newTestSecretStore(t, func(req *http.Request) (*http.Response, error) {
					if secretFetch && req.URL.Path == "/secrets" {
						return secretResponse(t, req, http.StatusOK, map[string]any{
							"value": []any{
								map[string]any{"id": "https://test-vault.vault.azure.net/secrets/password", "attributes": map[string]bool{"enabled": true}},
							},
						}), nil
					}
					if canceled {
						cancel()
						return nil, req.Context().Err()
					}
					return nil, wantErr
				})
				res, err := s.BulkGetSecret(ctx, secretstores.BulkGetSecretRequest{})
				require.ErrorIs(t, err, wantErr)
				assert.Nil(t, res.Data)
			})
		}
	}
}

func TestInit(t *testing.T) {
	t.Run("Init with valid metadata", func(t *testing.T) {
		m := secretstores.Metadata{}
		s := NewAzureKeyvaultSecretStore(logger.NewLogger("test"))
		m.Properties = map[string]string{
			"vaultName":         "foo",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
		}
		err := s.Init(t.Context(), m)
		require.NoError(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		require.True(t, ok)
		assert.Equal(t, "foo", kv.vaultName)
		assert.Equal(t, "vault.azure.net", kv.vaultDNSSuffix)
		assert.NotNil(t, kv.vaultClient)
	})
	t.Run("Init with valid metadata and Azure environment", func(t *testing.T) {
		m := secretstores.Metadata{}
		s := NewAzureKeyvaultSecretStore(logger.NewLogger("test"))
		m.Properties = map[string]string{
			"vaultName":         "foo",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
			"azureEnvironment":  "AZURECHINACLOUD",
		}
		err := s.Init(t.Context(), m)
		require.NoError(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		require.True(t, ok)
		assert.Equal(t, "foo", kv.vaultName)
		assert.Equal(t, "vault.azure.cn", kv.vaultDNSSuffix)
		assert.NotNil(t, kv.vaultClient)
	})
	t.Run("Init with Azure environment as part of vaultName FQDN (1) - legacy", func(t *testing.T) {
		m := secretstores.Metadata{}
		s := NewAzureKeyvaultSecretStore(logger.NewLogger("test"))
		m.Properties = map[string]string{
			"vaultName":         "foo.vault.azure.cn",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
		}
		err := s.Init(t.Context(), m)
		require.NoError(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		require.True(t, ok)
		assert.Equal(t, "foo", kv.vaultName)
		assert.Equal(t, "vault.azure.cn", kv.vaultDNSSuffix)
		assert.NotNil(t, kv.vaultClient)
	})
	t.Run("Init with Azure environment as part of vaultName FQDN (2) - legacy", func(t *testing.T) {
		m := secretstores.Metadata{}
		s := NewAzureKeyvaultSecretStore(logger.NewLogger("test"))
		m.Properties = map[string]string{
			"vaultName":         "https://foo.vault.usgovcloudapi.net",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
		}
		err := s.Init(t.Context(), m)
		require.NoError(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		require.True(t, ok)
		assert.Equal(t, "foo", kv.vaultName)
		assert.Equal(t, "vault.usgovcloudapi.net", kv.vaultDNSSuffix)
		assert.NotNil(t, kv.vaultClient)
	})
}

func TestGetFeatures(t *testing.T) {
	s := NewAzureKeyvaultSecretStore(logger.NewLogger("test"))
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
