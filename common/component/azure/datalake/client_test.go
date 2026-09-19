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

package datalake

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	azauth "github.com/dapr/components-contrib/common/authentication/azure"
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
		"missing fileSystemName": {
			metadata:                 createTestMetadata(true, true, false),
			expectedFailureSubString: "missing or empty fileSystemName field from metadata",
		},
	}

	for name, s := range scenarios {
		t.Run(name, func(t *testing.T) {
			_, _, err := CreateFileSystemStorageClient(t.Context(), log, s.metadata)
			require.Error(t, err)
			assert.Contains(t, err.Error(), s.expectedFailureSubString)
		})
	}
}

func TestCreateFileSystemStorageClientNoEntityManagement(t *testing.T) {
	// With entity management disabled, no network calls are made, so client
	// construction can be validated end-to-end offline.
	log := logger.NewLogger("test")

	t.Run("shared key credentials", func(t *testing.T) {
		meta := createTestMetadata(true, true, true)
		meta["disableEntityManagement"] = "true"

		client, m, err := CreateFileSystemStorageClient(t.Context(), log, meta)
		require.NoError(t, err)
		require.NotNil(t, client)
		require.NotNil(t, m)
		assert.Equal(t, "account", m.AccountName)
		assert.Equal(t, "test", m.FileSystemName)
		assert.True(t, m.DisableEntityManagement)
		assert.Contains(t, client.DFSURL(), "account.dfs.core.windows.net/test")
	})

	t.Run("connection string", func(t *testing.T) {
		meta := map[string]string{
			"connectionString":        "DefaultEndpointsProtocol=https;AccountName=account;AccountKey=" + testAccountKey() + ";EndpointSuffix=core.windows.net",
			"fileSystemName":          "test",
			"disableEntityManagement": "true",
		}

		client, m, err := CreateFileSystemStorageClient(t.Context(), log, meta)
		require.NoError(t, err)
		require.NotNil(t, client)
		require.NotNil(t, m)
		assert.Equal(t, "test", m.FileSystemName)
	})
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

		assert.Empty(t, m.customEndpoint)

		u, err := m.GetFileSystemURL(azEnvSettings)
		require.NoError(t, err)
		assert.Equal(t, "https://account.dfs.core.windows.net/test", u.String())
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

		u, err := m.GetFileSystemURL(azEnvSettings)
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

		u, err := m.GetFileSystemURL(azEnvSettings)
		require.NoError(t, err)
		assert.Equal(t, "https://localhost:8080/account/test", u.String())
	})

	t.Run("custom endpoint set to Azure Data Lake Storage endpoint", func(t *testing.T) {
		logDest.Reset()

		meta := createTestMetadata(true, true, true)
		meta[azauth.MetadataKeys["StorageEndpoint"][0]] = "https://account.dfs.core.windows.net/test"

		m, err := parseMetadata(meta)
		require.NoError(t, err)

		azEnvSettings, err := azauth.NewEnvironmentSettings(meta)
		require.NoError(t, err)

		err = m.setCustomEndpoint(log, meta, azEnvSettings)
		require.NoError(t, err)

		assert.Empty(t, m.customEndpoint)

		u, err := m.GetFileSystemURL(azEnvSettings)
		require.NoError(t, err)
		assert.Equal(t, "https://account.dfs.core.windows.net/test", u.String())

		assert.Contains(t, logDest.String(), "Metadata property endpoint is set to an Azure Data Lake Storage endpoint and will be ignored")
	})
}

func testAccountKey() string {
	return base64.StdEncoding.EncodeToString([]byte("fake-test-key"))
}

func createTestMetadata(accountName bool, accountKey bool, fileSystem bool) map[string]string {
	m := map[string]string{}
	if accountName {
		m[azauth.MetadataKeys["StorageAccountName"][0]] = "account"
	}
	if accountKey {
		m[azauth.MetadataKeys["StorageAccountKey"][0]] = testAccountKey()
	}
	if fileSystem {
		m["fileSystemName"] = "test"
	}
	return m
}
