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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMetadata(t *testing.T) {
	t.Run("parse all metadata", func(t *testing.T) {
		m := map[string]string{
			"accountName":             "account",
			"accountKey":              "key",
			"fileSystemName":          "test",
			"retryCount":              "5",
			"prefix":                  "myprefix",
			"disableEntityManagement": "true",
		}
		meta, err := parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "account", meta.AccountName)
		assert.Equal(t, "key", meta.AccountKey)
		assert.Equal(t, "test", meta.FileSystemName)
		assert.Equal(t, int32(5), meta.RetryCount)
		assert.Equal(t, "myprefix", meta.Prefix)
		assert.True(t, meta.DisableEntityManagement)
	})

	t.Run("default retry count", func(t *testing.T) {
		meta, err := parseMetadata(map[string]string{
			"accountName":    "account",
			"fileSystemName": "test",
		})
		require.NoError(t, err)
		assert.Equal(t, int32(defaultDataLakeRetryCount), meta.RetryCount)
	})

	t.Run("fileSystemName aliases", func(t *testing.T) {
		for _, alias := range []string{"fileSystemName", "fileSystem", "filesystemName"} {
			meta, err := parseMetadata(map[string]string{
				"accountName": "account",
				alias:         "test",
			})
			require.NoError(t, err, "alias %q", alias)
			assert.Equal(t, "test", meta.FileSystemName, "alias %q", alias)
		}
	})

	t.Run("storageAccount alias for account name", func(t *testing.T) {
		meta, err := parseMetadata(map[string]string{
			"storageAccount": "account",
			"fileSystemName": "test",
		})
		require.NoError(t, err)
		assert.Equal(t, "account", meta.AccountName)
	})

	t.Run("missing fileSystemName", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{
			"accountName": "account",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fileSystemName")
	})

	t.Run("missing accountName without connection string", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{
			"fileSystemName": "test",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "accountName")
	})

	t.Run("connection string does not require account name", func(t *testing.T) {
		meta, err := parseMetadata(map[string]string{
			"connectionString": "DefaultEndpointsProtocol=https;AccountName=account;AccountKey=key;EndpointSuffix=core.windows.net",
			"fileSystemName":   "test",
		})
		require.NoError(t, err)
		assert.Equal(t, "test", meta.FileSystemName)
	})

	t.Run("invalid retry count", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{
			"accountName":    "account",
			"fileSystemName": "test",
			"retryCount":     "not-a-number",
		})
		require.Error(t, err)
	})
}
