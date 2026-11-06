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

package appconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/configuration"
	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type MockConfigurationStore struct{}

type keyVaultReferenceConfigurationStore struct {
	MockConfigurationStore
}

type referenceSecretStore struct {
	secretstores.SecretStore
	getSecret func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error)
}

func (s referenceSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	return s.getSecret(ctx, req)
}

func (m *keyVaultReferenceConfigurationStore) GetSetting(ctx context.Context, key string, _ *azappconfig.GetSettingOptions) (azappconfig.GetSettingResponse, error) {
	if err := ctx.Err(); err != nil {
		return azappconfig.GetSettingResponse{}, err
	}
	return azappconfig.GetSettingResponse{Setting: azappconfig.Setting{
		Key:         ptr.Of(key),
		Value:       ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password/version1"}`),
		Label:       ptr.Of("production"),
		ContentType: ptr.Of("application/vnd.microsoft.appconfig.keyvaultref+json;charset=utf-8"),
	}}, nil
}

func (m *keyVaultReferenceConfigurationStore) NewListSettingsPager(_ azappconfig.SettingSelector, _ *azappconfig.ListSettingsOptions) *runtime.Pager[azappconfig.ListSettingsPageResponse] {
	return runtime.NewPager(runtime.PagingHandler[azappconfig.ListSettingsPageResponse]{
		More: func(azappconfig.ListSettingsPageResponse) bool { return false },
		Fetcher: func(ctx context.Context, cur *azappconfig.ListSettingsPageResponse) (azappconfig.ListSettingsPageResponse, error) {
			ref, err := m.GetSetting(ctx, "database-password", nil)
			return azappconfig.ListSettingsPageResponse{Settings: []azappconfig.Setting{
				ref.Setting,
				{Key: ptr.Of("plain"), Value: ptr.Of("plain-value")},
			}}, err
		},
	})
}

func newTestKeyVaultConfigurationStore(t *testing.T, getSecret func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error)) *ConfigurationStore {
	t.Helper()
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)
	s.client = &keyVaultReferenceConfigurationStore{}
	s.metadata.RequestTimeout = time.Second
	s.metadata.SubscribePollInterval = time.Hour
	s.metadata.ResolveKeyVaultReferences = true
	s.keyVaultDNSSuffix = "vault.azure.net"
	s.keyVaultStores.Store("test-vault", referenceSecretStore{getSecret: getSecret})
	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})
	return s
}

func keyVaultConfigurationItem(value string) *configuration.Item {
	return &configuration.Item{
		Value: value,
		Metadata: map[string]string{
			"label":       "production",
			"contentType": keyVaultReferenceContentType + ";charset=utf-8",
		},
	}
}

func keyVaultValueTests() []struct {
	name  string
	value string
} {
	return []struct {
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
}

func Test_getConfigurationKeyVaultReferenceWithProvidedKeys(t *testing.T) {
	for _, tt := range keyVaultValueTests() {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			s := newTestKeyVaultConfigurationStore(t, func(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				calls++
				assert.Equal(t, "password", req.Name)
				assert.Equal(t, "version1", req.Metadata["version_id"])
				_, ok := ctx.Deadline()
				assert.True(t, ok)
				return secretstores.GetSecretResponse{Data: map[string]string{"password": tt.value}}, nil
			})
			res, err := s.Get(t.Context(), &configuration.GetRequest{Keys: []string{"database-password"}})
			require.NoError(t, err)
			assert.Equal(t, map[string]*configuration.Item{
				"database-password": keyVaultConfigurationItem(tt.value),
			}, res.Items)
			assert.Equal(t, 1, calls)
		})
	}
}

func Test_getConfigurationKeyVaultReferenceWithNoProvidedKeys(t *testing.T) {
	for _, tt := range keyVaultValueTests() {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			s := newTestKeyVaultConfigurationStore(t, func(_ context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				calls++
				assert.Equal(t, "password", req.Name)
				assert.Equal(t, "version1", req.Metadata["version_id"])
				return secretstores.GetSecretResponse{Data: map[string]string{"password": tt.value}}, nil
			})
			res, err := s.Get(t.Context(), &configuration.GetRequest{})
			require.NoError(t, err)
			assert.Equal(t, map[string]*configuration.Item{
				"database-password": keyVaultConfigurationItem(tt.value),
				"plain":             {Value: "plain-value", Metadata: map[string]string{}},
			}, res.Items)
			assert.Equal(t, 1, calls)
		})
	}
}

func Test_keyVaultConfigurationValueSerialization(t *testing.T) {
	for _, tt := range keyVaultValueTests() {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestKeyVaultConfigurationStore(t, func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				return secretstores.GetSecretResponse{Data: map[string]string{"password": tt.value}}, nil
			})
			res, err := s.Get(t.Context(), &configuration.GetRequest{Keys: []string{"database-password"}})
			require.NoError(t, err)
			data, err := json.Marshal(res)
			require.NoError(t, err)
			var decoded configuration.GetResponse
			require.NoError(t, json.Unmarshal(data, &decoded))
			assert.Equal(t, res, &decoded)
			assert.Equal(t, tt.value, decoded.Items["database-password"].Value)
		})
	}
}

func Test_subscribeConfigurationKeyVaultReference(t *testing.T) {
	requests := []struct {
		name string
		keys []string
	}{
		{name: "provided keys", keys: []string{"database-password"}},
		{name: "all keys"},
	}
	for _, tt := range keyVaultValueTests() {
		for _, request := range requests {
			t.Run(tt.name+"/"+request.name, func(t *testing.T) {
				calls := 0
				s := newTestKeyVaultConfigurationStore(t, func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
					calls++
					return secretstores.GetSecretResponse{Data: map[string]string{"password": tt.value}}, nil
				})
				updates := make(chan *configuration.UpdateEvent, 1)
				subID, err := s.Subscribe(t.Context(), &configuration.SubscribeRequest{
					Keys:     request.keys,
					Metadata: map[string]string{"sentinelkey": "sentinel"},
				}, func(ctx context.Context, event *configuration.UpdateEvent) error {
					select {
					case updates <- event:
					case <-ctx.Done():
					}
					return nil
				})
				require.NoError(t, err)
				require.NotEmpty(t, subID)
				select {
				case event := <-updates:
					assert.Equal(t, subID, event.ID)
					expected := map[string]*configuration.Item{
						"database-password": keyVaultConfigurationItem(tt.value),
					}
					if len(request.keys) == 0 {
						expected["plain"] = &configuration.Item{Value: "plain-value", Metadata: map[string]string{}}
					}
					assert.Equal(t, expected, event.Items)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for configuration update")
				}
				require.NoError(t, s.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: subID}))
				require.NoError(t, s.Close())
				assert.Equal(t, 1, calls)
				_, ok := s.cancelMap.Load(subID)
				assert.False(t, ok)
				assert.Empty(t, updates)
			})
		}
	}
}

func Test_getConfigurationKeyVaultReferenceValueRefresh(t *testing.T) {
	tests := []struct {
		name string
		keys []string
	}{
		{name: "provided keys", keys: []string{"database-password"}},
		{name: "all keys"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := "first-value"
			calls := 0
			s := newTestKeyVaultConfigurationStore(t, func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				calls++
				return secretstores.GetSecretResponse{Data: map[string]string{"password": value}}, nil
			})
			first, err := s.Get(t.Context(), &configuration.GetRequest{Keys: tt.keys})
			require.NoError(t, err)
			value = `{"value":"rotated"}`
			second, err := s.Get(t.Context(), &configuration.GetRequest{Keys: tt.keys})
			require.NoError(t, err)
			assert.Equal(t, "first-value", first.Items["database-password"].Value)
			assert.Equal(t, value, second.Items["database-password"].Value)
			value = ""
			third, err := s.Get(t.Context(), &configuration.GetRequest{Keys: tt.keys})
			require.NoError(t, err)
			assert.Equal(t, "", third.Items["database-password"].Value)
			assert.Equal(t, 3, calls)
		})
	}
}

func Test_subscribeConfigurationKeyVaultReferenceValueRefresh(t *testing.T) {
	tests := []struct {
		name string
		keys []string
	}{
		{name: "provided keys", keys: []string{"database-password"}},
		{name: "all keys"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := []string{`{"value":"rotated"}`, ""}
			calls := 0
			s := newTestKeyVaultConfigurationStore(t, func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				value := values[1]
				if calls == 0 {
					value = values[0]
				}
				calls++
				return secretstores.GetSecretResponse{Data: map[string]string{"password": value}}, nil
			})
			s.metadata.SubscribePollInterval = time.Millisecond
			updates := make(chan string)
			subID, err := s.Subscribe(t.Context(), &configuration.SubscribeRequest{
				Keys:     tt.keys,
				Metadata: map[string]string{"sentinelkey": "sentinel"},
			}, func(ctx context.Context, event *configuration.UpdateEvent) error {
				select {
				case updates <- event.Items["database-password"].Value:
				case <-ctx.Done():
				}
				return nil
			})
			require.NoError(t, err)
			for _, value := range values {
				select {
				case update := <-updates:
					assert.Equal(t, value, update)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for refreshed configuration value")
				}
			}
			require.NoError(t, s.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: subID}))
			require.NoError(t, s.Close())
		})
	}
}

func TestResolveKeyVaultReferencesMetadata(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		set     bool
		want    bool
		wantErr bool
	}{
		{name: "omitted"},
		{name: "explicit false", set: true, value: "false"},
		{name: "explicit true", set: true, value: "true", want: true},
		{name: "uppercase true", set: true, value: "TRUE", want: true},
		{name: "empty value", set: true, value: ""},
		{name: "non-truthy value follows metadata decoder", set: true, value: "not-a-boolean"},
		{name: "case-insensitive key", key: "RESOLVEKEYVAULTREFERENCES", set: true, value: "true", want: true},
		{name: "duplicate keys", key: "RESOLVEKEYVAULTREFERENCES", set: true, value: "true", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			properties := map[string]string{
				"connectionString": "Endpoint=https://test.azconfig.io;Id=test;Secret=dGVzdA==",
			}
			if tt.set {
				key := tt.key
				if key == "" {
					key = "resolveKeyVaultReferences"
				}
				properties[key] = tt.value
				if tt.wantErr {
					properties["resolveKeyVaultReferences"] = "false"
				}
			}
			s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)
			err := s.Init(t.Context(), configuration.Metadata{Base: mdata.Base{Properties: properties}})
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, s.client)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, s.metadata.ResolveKeyVaultReferences)
			if tt.want {
				assert.Equal(t, "vault.azure.net", s.keyVaultDNSSuffix)
				return
			}
			assert.Empty(t, s.keyVaultProperties)
			assert.Empty(t, s.keyVaultDNSSuffix)
		})
	}
}

func Test_getConfigurationKeyVaultReferencesDisabled(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
	}{
		{name: "omitted"},
		{name: "explicit false", properties: map[string]string{"resolveKeyVaultReferences": "false"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			properties := map[string]string{
				"connectionString": "Endpoint=https://test.azconfig.io;Id=test;Secret=dGVzdA==",
			}
			for key, value := range tt.properties {
				properties[key] = value
			}
			s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)
			require.NoError(t, s.Init(t.Context(), configuration.Metadata{Base: mdata.Base{Properties: properties}}))
			t.Cleanup(func() {
				require.NoError(t, s.Close())
			})
			s.client = &keyVaultReferenceConfigurationStore{}
			s.keyVaultStores.Store("test-vault", referenceSecretStore{
				getSecret: func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
					t.Error("disabled resolution must not fetch a secret")
					return secretstores.GetSecretResponse{}, errors.New("unexpected secret request")
				},
			})
			for _, keys := range [][]string{{"database-password"}, nil} {
				res, err := s.Get(t.Context(), &configuration.GetRequest{Keys: keys})
				require.NoError(t, err)
				assert.Equal(t, `{"uri":"https://test-vault.vault.azure.net/secrets/password/version1"}`, res.Items["database-password"].Value)
			}
		})
	}
}

func TestResolveKeyVaultReference(t *testing.T) {
	tests := []struct {
		name        string
		value       *string
		contentType *string
		disabled    bool
		want        string
		wantVersion string
		wantErr     string
	}{
		{name: "latest", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password"}`), want: "resolved"},
		{name: "pinned version", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password/v2"}`), want: "resolved", wantVersion: "v2"},
		{name: "empty secret", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password"}`), want: ""},
		{name: "disabled", disabled: true, value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password"}`), want: `{"uri":"https://test-vault.vault.azure.net/secrets/password"}`},
		{name: "ordinary JSON", contentType: ptr.Of("application/json"), value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password"}`), want: `{"uri":"https://test-vault.vault.azure.net/secrets/password"}`},
		{name: "plain value", contentType: ptr.Of("text/plain"), value: ptr.Of("plain"), want: "plain"},
		{name: "invalid JSON", value: ptr.Of(`{`), wantErr: "invalid Key Vault reference"},
		{name: "JSON array reference", value: ptr.Of(`[]`), wantErr: "invalid Key Vault reference"},
		{name: "JSON string reference", value: ptr.Of(`"https://test-vault.vault.azure.net/secrets/password"`), wantErr: "invalid Key Vault reference"},
		{name: "numeric URI", value: ptr.Of(`{"uri":123}`), wantErr: "invalid Key Vault reference"},
		{name: "null URI", value: ptr.Of(`{"uri":null}`), wantErr: "HTTPS secret URI"},
		{name: "missing value", wantErr: "invalid Key Vault reference"},
		{name: "missing URI", value: ptr.Of(`{}`), wantErr: "HTTPS secret URI"},
		{name: "HTTP", value: ptr.Of(`{"uri":"http://test-vault.vault.azure.net/secrets/password"}`), wantErr: "HTTPS secret URI"},
		{name: "wrong cloud", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.cn/secrets/password"}`), wantErr: "HTTPS secret URI"},
		{name: "untrusted host", value: ptr.Of(`{"uri":"https://vault.azure.net.evil.example/secrets/password"}`), wantErr: "HTTPS secret URI"},
		{name: "port", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net:443/secrets/password"}`), wantErr: "HTTPS secret URI"},
		{name: "userinfo", value: ptr.Of(`{"uri":"https://user@test-vault.vault.azure.net/secrets/password"}`), wantErr: "HTTPS secret URI"},
		{name: "query", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password?value=x"}`), wantErr: "HTTPS secret URI"},
		{name: "fragment", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password#x"}`), wantErr: "HTTPS secret URI"},
		{name: "key not secret", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/keys/password"}`), wantErr: "HTTPS secret URI"},
		{name: "encoded path", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/%70assword"}`), wantErr: "HTTPS secret URI"},
		{name: "extra path", value: ptr.Of(`{"uri":"https://test-vault.vault.azure.net/secrets/password/v2/extra"}`), wantErr: "HTTPS secret URI"},
		{name: "invalid URI", value: ptr.Of(`{"uri":"https://%/secrets/password"}`), wantErr: "invalid Key Vault reference URI"},
		{name: "invalid reference content type", value: ptr.Of("{}"), contentType: ptr.Of(keyVaultReferenceContentType + ";invalid"), wantErr: "invalid Key Vault reference content type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			s := newTestKeyVaultConfigurationStore(t, func(_ context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				called = true
				assert.Equal(t, "password", req.Name)
				assert.Equal(t, tt.wantVersion, req.Metadata["version_id"])
				return secretstores.GetSecretResponse{Data: map[string]string{"password": tt.want}}, nil
			})
			s.metadata.ResolveKeyVaultReferences = !tt.disabled
			ct := tt.contentType
			if ct == nil {
				ct = ptr.Of(keyVaultReferenceContentType + ";charset=utf-8")
			}
			item, err := s.settingToConfigurationItem(t.Context(), azappconfig.Setting{Value: tt.value, ContentType: ct})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Nil(t, item)
				assert.False(t, called)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, item.Value)
				assert.Equal(t, !tt.disabled && tt.contentType == nil, called)
			}
		})
	}
}

func Test_getConfigurationKeyVaultReferenceErrors(t *testing.T) {
	fetchErr := errors.New("access denied")
	tests := []struct {
		name string
		keys []string
	}{
		{name: "provided keys", keys: []string{"database-password"}},
		{name: "all keys"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestKeyVaultConfigurationStore(t, func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				return secretstores.GetSecretResponse{}, fetchErr
			})
			res, err := s.Get(t.Context(), &configuration.GetRequest{Keys: tt.keys})
			require.ErrorIs(t, err, fetchErr)
			assert.Empty(t, res.Items)
		})
	}
}

func Test_settingToConfigurationItemMissingSecretData(t *testing.T) {
	s := newTestKeyVaultConfigurationStore(t, func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
		return secretstores.GetSecretResponse{}, nil
	})
	setting, err := s.client.GetSetting(t.Context(), "key", nil)
	require.NoError(t, err)
	_, err = s.settingToConfigurationItem(t.Context(), setting.Setting)
	require.ErrorContains(t, err, "does not contain the referenced secret")
}

func Test_getConfigurationKeyVaultReferenceContextErrors(t *testing.T) {
	tests := []struct {
		name     string
		keys     []string
		canceled bool
		wantErr  error
	}{
		{name: "provided keys deadline", keys: []string{"database-password"}, wantErr: context.DeadlineExceeded},
		{name: "all keys deadline", wantErr: context.DeadlineExceeded},
		{name: "provided keys cancellation", keys: []string{"database-password"}, canceled: true, wantErr: context.Canceled},
		{name: "all keys cancellation", canceled: true, wantErr: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			s := newTestKeyVaultConfigurationStore(t, func(ctx context.Context, _ secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				calls++
				if tt.canceled {
					cancel()
				}
				<-ctx.Done()
				return secretstores.GetSecretResponse{}, ctx.Err()
			})
			s.metadata.RequestTimeout = 10 * time.Millisecond
			res, err := s.Get(ctx, &configuration.GetRequest{Keys: tt.keys})
			require.ErrorIs(t, err, tt.wantErr)
			assert.Empty(t, res.Items)
			assert.Equal(t, 1, calls)
		})
	}
}

func Test_subscribeConfigurationKeyVaultReferenceFailureDoesNotNotify(t *testing.T) {
	tests := []struct {
		name string
		keys []string
	}{
		{name: "provided keys", keys: []string{"database-password"}},
		{name: "all keys"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			attempted := make(chan struct{})
			s := newTestKeyVaultConfigurationStore(t, func(context.Context, secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
				calls++
				cancel()
				close(attempted)
				return secretstores.GetSecretResponse{}, errors.New("access denied")
			})
			updates := make(chan *configuration.UpdateEvent, 1)
			subID, err := s.Subscribe(ctx, &configuration.SubscribeRequest{
				Keys:     tt.keys,
				Metadata: map[string]string{"sentinelkey": "sentinel"},
			}, func(ctx context.Context, event *configuration.UpdateEvent) error {
				select {
				case updates <- event:
				case <-ctx.Done():
				}
				return nil
			})
			require.NoError(t, err)
			select {
			case <-attempted:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for secret resolution")
			}
			require.NoError(t, s.Close())
			assert.Empty(t, updates)
			assert.Equal(t, 1, calls)
			_, ok := s.cancelMap.Load(subID)
			assert.False(t, ok)
		})
	}
}

func TestInitKeyVaultResolution(t *testing.T) {
	for _, auth := range []string{"host", "connectionString"} {
		t.Run(auth, func(t *testing.T) {
			properties := map[string]string{
				"resolveKeyVaultReferences": "true",
				"azureEnvironment":          "AzureChinaCloud",
				"azureTenantId":             "00000000-0000-0000-0000-000000000000",
				"azureClientId":             "00000000-0000-0000-0000-000000000000",
				"azureClientSecret":         "test-secret",
			}
			if auth == "host" {
				properties[auth] = "https://test.azconfig.azure.cn"
			} else {
				properties[auth] = "Endpoint=https://test.azconfig.azure.cn;Id=test;Secret=dGVzdA=="
			}
			s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)
			require.NoError(t, s.Init(t.Context(), configuration.Metadata{Base: mdata.Base{Properties: properties}}))
			assert.True(t, s.metadata.ResolveKeyVaultReferences)
			assert.Equal(t, "vault.azure.cn", s.keyVaultDNSSuffix)
			assert.Equal(t, properties, s.keyVaultProperties)
			properties["azureClientSecret"] = "changed"
			assert.Equal(t, "test-secret", s.keyVaultProperties["azureClientSecret"])

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err := s.settingToConfigurationItem(ctx, azappconfig.Setting{
				Value:       ptr.Of(`{"uri":"https://test-vault.vault.azure.cn/secrets/password"}`),
				ContentType: ptr.Of(keyVaultReferenceContentType),
			})
			require.ErrorIs(t, err, context.Canceled)
			_, ok := s.keyVaultStores.Load("test-vault")
			assert.True(t, ok)
			assert.NotContains(t, s.keyVaultProperties, "vaultName")
		})
	}
}

const (
	testMaxRetryDelay                      = "120s"
	testSubscribePollIntervalAndReqTimeout = "30s"
)

func (m *MockConfigurationStore) GetSetting(ctx context.Context, key string, options *azappconfig.GetSettingOptions) (azappconfig.GetSettingResponse, error) {
	if key == "testKey" || key == "test_sentinel_key" {
		settings := azappconfig.Setting{}

		settings.Key = ptr.Of(key)
		settings.Value = ptr.Of("testValue")
		settings.Label = ptr.Of("testLabel")
		settings.ContentType = ptr.Of("application/json")

		resp := azappconfig.GetSettingResponse{}
		resp.Setting = settings
		return resp, nil
	}
	resp := azappconfig.GetSettingResponse{}
	return resp, nil
}

func (m *MockConfigurationStore) NewListSettingsPager(selector azappconfig.SettingSelector, options *azappconfig.ListSettingsOptions) *runtime.Pager[azappconfig.ListSettingsPageResponse] {
	settings := make([]azappconfig.Setting, 2)

	setting1 := azappconfig.Setting{}
	setting1.Key = ptr.Of("testKey-1")
	setting1.Value = ptr.Of("testValue-1")
	setting1.Label = ptr.Of("testLabel-1")
	setting1.ContentType = ptr.Of("application/json")

	setting2 := azappconfig.Setting{}
	setting2.Key = ptr.Of("testKey-2")
	setting2.Value = ptr.Of("testValue-2")
	setting2.Label = ptr.Of("testLabel-2")
	setting2.ContentType = ptr.Of("text/plain")
	settings[0] = setting1
	settings[1] = setting2

	return runtime.NewPager(runtime.PagingHandler[azappconfig.ListSettingsPageResponse]{
		More: func(azappconfig.ListSettingsPageResponse) bool {
			return false
		},
		Fetcher: func(ctx context.Context, cur *azappconfig.ListSettingsPageResponse) (azappconfig.ListSettingsPageResponse, error) {
			listSettingPage := azappconfig.ListSettingsPageResponse{}
			listSettingPage.Settings = settings
			return listSettingPage, nil
		},
	})
}

func TestNewAzureAppConfigurationStore(t *testing.T) {
	type args struct {
		logger logger.Logger
	}
	tests := []struct {
		name string
		args args
		want configuration.Store
	}{
		{
			args: args{
				logger: logger.NewLogger("test"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewAzureAppConfigurationStore(tt.args.logger)
			assert.NotNil(t, got)
		})
	}
}

func Test_getConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}

	t.Run("call getConfiguration for provided keys", func(t *testing.T) {
		req := configuration.GetRequest{
			Keys:     []string{"testKey"},
			Metadata: map[string]string{},
		}
		res, err := s.Get(t.Context(), &req)
		require.NoError(t, err)
		assert.Len(t, res.Items, 1)
		assert.Equal(t, "application/json", res.Items["testKey"].Metadata["contentType"])
		assert.Equal(t, "testLabel", res.Items["testKey"].Metadata["label"])
	})
}

func Test_subscribeConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}

	metadata := make(map[string]string)
	metadata["sentinelkey"] = "test_sentinel_key"

	t.Run("call subscribe with sentinel key", func(t *testing.T) {
		req := configuration.SubscribeRequest{
			Keys:     []string{"testKey"},
			Metadata: metadata,
		}
		subID, err := s.Subscribe(t.Context(), &req, updateEventHandler)
		assert.NotEmpty(t, subID)
		require.NoError(t, err)
		unReq := &configuration.UnsubscribeRequest{
			ID: subID,
		}
		_ = s.Unsubscribe(t.Context(), unReq)
	})

	t.Run("call subscribe w/o sentinel key", func(t *testing.T) {
		req := configuration.SubscribeRequest{
			Keys:     []string{"testKey"},
			Metadata: make(map[string]string),
		}
		_, err := s.Subscribe(t.Context(), &req, updateEventHandler)
		require.Error(t, err)
	})
}

func Test_unsubscribeConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}
	cancelContext, cancel := context.WithCancel(t.Context())
	s.cancelMap.Store("id1", cancel)

	t.Run("call unsubscribe with incorrect subId", func(t *testing.T) {
		req := configuration.UnsubscribeRequest{
			ID: "id_not_exist",
		}
		err := s.Unsubscribe(cancelContext, &req)
		require.Error(t, err)
		_, ok := s.cancelMap.Load("id1")
		assert.True(t, ok)
	})

	t.Run("call unsubscribe with correct subId", func(t *testing.T) {
		req := configuration.UnsubscribeRequest{
			ID: "id1",
		}
		err := s.Unsubscribe(cancelContext, &req)
		require.NoError(t, err)
		_, ok := s.cancelMap.Load("id1")
		assert.False(t, ok)
	})
}

func Test_getConfigurationWithNoProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}

	t.Run("call getConfiguration for provided keys", func(t *testing.T) {
		req := configuration.GetRequest{
			Keys:     []string{},
			Metadata: map[string]string{},
		}
		res, err := s.Get(t.Context(), &req)
		require.NoError(t, err)
		assert.Len(t, res.Items, 2)
		assert.Equal(t, "application/json", res.Items["testKey-1"].Metadata["contentType"])
		assert.Equal(t, "testLabel-1", res.Items["testKey-1"].Metadata["label"])
		assert.Equal(t, "text/plain", res.Items["testKey-2"].Metadata["contentType"])
		assert.Equal(t, "testLabel-2", res.Items["testKey-2"].Metadata["label"])
	})
}

func TestInit(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test"))
	t.Run("Init with valid appConfigHost metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		m := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		err := s.Init(t.Context(), m)
		require.NoError(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[host], cs.metadata.Host)
		assert.Equal(t, 3, cs.metadata.MaxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.RetryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.MaxRetryDelay)
		assert.Equal(t, time.Second*30, cs.metadata.SubscribePollInterval)
		assert.Equal(t, time.Second*30, cs.metadata.RequestTimeout)
	})

	t.Run("Init with valid appConfigConnectionString metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "Endpoint=https://foo.azconfig.io;Id=osOX-l9-s0:sig;Secret=00000000000000000000000000000000000000000000"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		m := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		err := s.Init(t.Context(), m)
		require.NoError(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[connectionString], cs.metadata.ConnectionString)
		assert.Equal(t, 3, cs.metadata.MaxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.RetryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.MaxRetryDelay)
		assert.Equal(t, time.Second*30, cs.metadata.SubscribePollInterval)
		assert.Equal(t, time.Second*30, cs.metadata.RequestTimeout)
	})
}

func TestNewRetryOptions(t *testing.T) {
	m := metadata{
		MaxRetries:    3,
		RetryDelay:    time.Second * 4,
		MaxRetryDelay: time.Second * 120,
	}

	opts := newRetryOptions(m)

	assert.EqualValues(t, m.MaxRetries, opts.MaxRetries)
	assert.Equal(t, m.RetryDelay, opts.RetryDelay)
	assert.Equal(t, m.MaxRetryDelay, opts.MaxRetryDelay)
	assert.NotEqual(t, opts.MaxRetryDelay, opts.RetryDelay)
}

func TestParseMetadata(t *testing.T) {
	t.Run("parse metadata with "+host, func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		want := metadata{
			Host:                  "testHost",
			MaxRetries:            3,
			RetryDelay:            time.Second * 4,
			MaxRetryDelay:         time.Second * 120,
			SubscribePollInterval: time.Second * 30,
			RequestTimeout:        time.Second * 30,
		}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.NoError(t, err)
		assert.Equal(t, want.Host, m.Host)
		assert.Equal(t, want.MaxRetries, m.MaxRetries)
		assert.Equal(t, want.RetryDelay, m.RetryDelay)
		assert.Equal(t, want.MaxRetryDelay, m.MaxRetryDelay)
		assert.Equal(t, want.SubscribePollInterval, m.SubscribePollInterval)
		assert.Equal(t, want.RequestTimeout, m.RequestTimeout)
	})

	t.Run("parse metadata with "+connectionString, func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		want := metadata{
			ConnectionString:      "testConnectionString",
			MaxRetries:            3,
			RetryDelay:            time.Second * 4,
			MaxRetryDelay:         time.Second * 120,
			SubscribePollInterval: time.Second * 30,
			RequestTimeout:        time.Second * 30,
		}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.NoError(t, err)
		assert.Equal(t, want.ConnectionString, m.ConnectionString)
		assert.Equal(t, want.MaxRetries, m.MaxRetries)
		assert.Equal(t, want.RetryDelay, m.RetryDelay)
		assert.Equal(t, want.MaxRetryDelay, m.MaxRetryDelay)
		assert.Equal(t, want.SubscribePollInterval, m.SubscribePollInterval)
		assert.Equal(t, want.RequestTimeout, m.RequestTimeout)
	})

	t.Run(fmt.Sprintf("both %s and %s fields set in metadata", host, connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.Error(t, err)
	})

	t.Run(fmt.Sprintf("both %s and %s fields not set in metadata", host, connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = ""
		testProperties[connectionString] = ""
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.Error(t, err)
	})
}

func updateEventHandler(ctx context.Context, e *configuration.UpdateEvent) error {
	return nil
}
