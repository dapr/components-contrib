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

package blobstorage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/kit/logger"
)

func TestClientInitFailures(t *testing.T) {
	log := logger.NewLogger("test")

	type scenario struct {
		metadata                 map[string]string
		expectedFailureSubString string
	}

	scenarios := map[string]scenario{
		"missing accountName": {
			metadata:                 createTestMetadata(false, true, true),
			expectedFailureSubString: "missing or empty accountName field from metadata",
		},
		"missing container": {
			metadata:                 createTestMetadata(true, true, false),
			expectedFailureSubString: "missing or empty containerName field from metadata",
		},
	}

	for name, s := range scenarios {
		t.Run(name, func(t *testing.T) {
			_, _, err := CreateContainerStorageClient(context.Background(), log, s.metadata)
			assert.Contains(t, err.Error(), s.expectedFailureSubString)
		})
	}
}

func TestSetCustomEndpoint(t *testing.T) {
	logDest := &bytes.Buffer{}
	log := logger.NewLogger("test")
	log.SetOutput(logDest)

	t.Run("no custom endpoint", func(t *testing.T) {
		meta := createTestMetadata(true, true, true)
		m, err := parseMetadata(meta)
		require.NoError(t, err)

		azEnvSettings, err := azauth.NewEnvironmentSettings(meta)
		require.NoError(t, err)

		err = m.setCustomEndpoint(log, meta, azEnvSettings)
		require.NoError(t, err)

		assert.Equal(t, "", m.customEndpoint)

		u, err := m.GetContainerURL(azEnvSettings)
		require.NoError(t, err)
		assert.Equal(t, "https://account.blob.core.windows.net/test", u.String())
	})

	t.Run("custom endpoint set", func(t *testing.T) {
		meta := createTestMetadata(true, true, true)
		meta[azauth.MetadataKeys["StorageEndpoint"][0]] = "https://localhost:8080"

		m, err := parseMetadata(meta)
		require.NoError(t, err)

		azEnvSettings, err := azauth.NewEnvironmentSettings(meta)
		require.NoError(t, err)

		err = m.setCustomEndpoint(log, meta, azEnvSettings)
		require.NoError(t, err)

		assert.Equal(t, "https://localhost:8080", m.customEndpoint)

		u, err := m.GetContainerURL(azEnvSettings)
		require.NoError(t, err)
		assert.Equal(t, "https://localhost:8080/account/test", u.String())
	})

	t.Run("custom endpoint set with trailing slash removed", func(t *testing.T) {
		meta := createTestMetadata(true, true, true)
		meta[azauth.MetadataKeys["StorageEndpoint"][0]] = "https://localhost:8080/"

		m, err := parseMetadata(meta)
		require.NoError(t, err)

		azEnvSettings, err := azauth.NewEnvironmentSettings(meta)
		require.NoError(t, err)

		err = m.setCustomEndpoint(log, meta, azEnvSettings)
		require.NoError(t, err)

		assert.Equal(t, "https://localhost:8080", m.customEndpoint)

		u, err := m.GetContainerURL(azEnvSettings)
		require.NoError(t, err)
		assert.Equal(t, "https://localhost:8080/account/test", u.String())
	})

	t.Run("custom endpoint set to Azure Blob Storage endpoint", func(t *testing.T) {
		logDest.Reset()

		meta := createTestMetadata(true, true, true)
		meta[azauth.MetadataKeys["StorageEndpoint"][0]] = "https://account.blob.core.windows.net/test"

		m, err := parseMetadata(meta)
		require.NoError(t, err)

		azEnvSettings, err := azauth.NewEnvironmentSettings(meta)
		require.NoError(t, err)

		err = m.setCustomEndpoint(log, meta, azEnvSettings)
		require.NoError(t, err)

		assert.Equal(t, "", m.customEndpoint)

		u, err := m.GetContainerURL(azEnvSettings)
		require.NoError(t, err)
		assert.Equal(t, "https://account.blob.core.windows.net/test", u.String())

		assert.Contains(t, logDest.String(), "Metadata property endpoint is set to an Azure Blob Storage endpoint and will be ignored")
	})
}

func createTestMetadata(accountName bool, accountKey bool, container bool) map[string]string {
	m := map[string]string{}
	if accountName {
		m[azauth.MetadataKeys["StorageAccountName"][0]] = "account"
	}
	if accountKey {
		m[azauth.MetadataKeys["StorageAccountKey"][0]] = "key"
	}
	if container {
		m[azauth.MetadataKeys["StorageContainerName"][0]] = "test"
	}
	return m
}
