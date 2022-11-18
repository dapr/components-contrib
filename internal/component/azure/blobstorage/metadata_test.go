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
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	logger := logger.NewLogger("test")
	var m map[string]string

	t.Run("parse all metadata", func(t *testing.T) {
		m = map[string]string{
			"storageAccount":    "account",
			"storageAccessKey":  "key",
			"container":         "test",
			"getBlobRetryCount": "5",
			"decodeBase64":      "true",
		}
		meta, err := parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "test", meta.ContainerName)
		assert.Equal(t, "account", meta.AccountName)
		// storageAccessKey is parsed in the azauth package
		assert.Equal(t, true, meta.DecodeBase64)
		assert.Equal(t, int32(5), meta.RetryCount)
		assert.Equal(t, "", string(meta.PublicAccessLevel))
	})

	t.Run("parse metadata with publicAccessLevel = blob", func(t *testing.T) {
		m = map[string]string{
			"storageAccount":    "account",
			"storageAccessKey":  "key",
			"container":         "test",
			"publicAccessLevel": "blob",
		}
		meta, err := parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, azblob.PublicAccessTypeBlob, meta.PublicAccessLevel)
	})

	t.Run("parse metadata with publicAccessLevel = container", func(t *testing.T) {
		m = map[string]string{
			"storageAccount":    "account",
			"storageAccessKey":  "key",
			"container":         "test",
			"publicAccessLevel": "container",
		}
		meta, err := parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, azblob.PublicAccessTypeContainer, meta.PublicAccessLevel)
	})

	t.Run("parse metadata with invalid publicAccessLevel", func(t *testing.T) {
		m = map[string]string{
			"storageAccount":    "account",
			"storageAccessKey":  "key",
			"container":         "test",
			"publicAccessLevel": "invalid",
		}
		_, err := parseMetadata(m)
		assert.Error(t, err)
	})

	t.Run("sanitize metadata if necessary", func(t *testing.T) {
		m = map[string]string{
			"somecustomfield": "some-custom-value",
			"specialfield":    "special:valueÜ",
			"not-allowed:":    "not-allowed",
		}
		meta := SanitizeMetadata(logger, m)
		assert.Equal(t, meta["somecustomfield"], "some-custom-value")
		assert.Equal(t, meta["specialfield"], "special:value")
		assert.Equal(t, meta["notallowed"], "not-allowed")
	})
}
