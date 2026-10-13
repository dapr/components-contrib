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

package objectstorage

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPrivateKey(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}))
}

func TestNewConfigurationProvider(t *testing.T) {
	t.Run("raw identity credentials", func(t *testing.T) {
		meta := &objectStoreMetadata{
			TenancyOCID: "ocid1.tenancy.oc1..tenancy",
			UserOCID:    "ocid1.user.oc1..user",
			Region:      "us-ashburn-1",
			FingerPrint: "00:11:22:33",
			PrivateKey:  testPrivateKey(t),
		}

		provider, err := newConfigurationProvider(meta)
		require.NoError(t, err)

		region, err := provider.Region()
		require.NoError(t, err)
		assert.Equal(t, "us-ashburn-1", region)

		tenancy, err := provider.TenancyOCID()
		require.NoError(t, err)
		assert.Equal(t, meta.TenancyOCID, tenancy)
	})

	t.Run("configuration file", func(t *testing.T) {
		meta := &objectStoreMetadata{
			ConfigFileAuthentication: true,
			ConfigFilePath:           "/does/not/exist",
			ConfigFileProfile:        "CUSTOM",
		}

		provider, err := newConfigurationProvider(meta)
		require.NoError(t, err)
		require.NotNil(t, provider)

		// The profile is only read lazily, so a missing file surfaces here
		// rather than at construction time.
		_, err = provider.TenancyOCID()
		require.Error(t, err)
	})
}

func TestNewObjectClientOwnsItsTransport(t *testing.T) {
	meta := &objectStoreMetadata{
		TenancyOCID: "ocid1.tenancy.oc1..tenancy",
		UserOCID:    "ocid1.user.oc1..user",
		Region:      "us-ashburn-1",
		FingerPrint: "00:11:22:33",
		PrivateKey:  testPrivateKey(t),
	}

	provider, err := newConfigurationProvider(meta)
	require.NoError(t, err)

	objectClient, httpClient, err := newObjectClient(provider)
	require.NoError(t, err)
	require.NotNil(t, objectClient)
	require.NotNil(t, httpClient)

	// The SDK must dispatch through the client we can later close.
	assert.Same(t, httpClient, objectClient.HTTPClient)
	assert.IsType(t, &http.Transport{}, httpClient.Transport)
}

func TestClientCloseReleasesConnections(t *testing.T) {
	t.Run("closes the owned transport", func(t *testing.T) {
		c := &ociObjectStoreClient{
			httpClient: &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()},
		}

		require.NoError(t, c.close())
		// Close is idempotent, so a second call must also be safe.
		require.NoError(t, c.close())
	})

	t.Run("no transport", func(t *testing.T) {
		c := &ociObjectStoreClient{}
		require.NoError(t, c.close())
	})
}
