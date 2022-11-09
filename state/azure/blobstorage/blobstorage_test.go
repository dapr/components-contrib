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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	m := state.Metadata{}
	s := NewAzureBlobStorageStore(logger.NewLogger("logger")).(*StateStore)
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"accountName":   "acc",
			"accountKey":    "e+Dnvl8EOxYxV94nurVaRQ==",
			"containerName": "dapr",
		}
		err := s.Init(m)
		assert.Nil(t, err)
		assert.Equal(t, "acc.blob.core.windows.net", s.containerURL.URL().Host)
		assert.Equal(t, "/dapr", s.containerURL.URL().Path)
	})

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"invalidValue": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing or empty accountName field from metadata"))
	})

	t.Run("Init with invalid account name", func(t *testing.T) {
		m.Properties = map[string]string{
			"accountName":   "invalid-account",
			"accountKey":    "e+Dnvl8EOxYxV94nurVaRQ==",
			"containerName": "dapr",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
	})
}

func TestGetBlobStorageMetaData(t *testing.T) {
	t.Run("Nothing at all passed", func(t *testing.T) {
		m := make(map[string]string)
		_, err := getBlobStorageMetadata(m)

		assert.NotNil(t, err)
	})

	t.Run("All parameters passed and parsed", func(t *testing.T) {
		m := make(map[string]string)
		m["accountName"] = "acc"
		m["containerName"] = "dapr"
		meta, err := getBlobStorageMetadata(m)

		assert.Nil(t, err)
		assert.Equal(t, "acc", meta.AccountName)
		assert.Equal(t, "dapr", meta.ContainerName)
	})
}

func TestFileName(t *testing.T) {
	t.Run("Valid composite key", func(t *testing.T) {
		key := getFileName("app_id||key")
		assert.Equal(t, "key", key)
	})

	t.Run("No delimiter present", func(t *testing.T) {
		key := getFileName("key")
		assert.Equal(t, "key", key)
	})
}

func TestBlobHTTPHeaderGeneration(t *testing.T) {
	s := NewAzureBlobStorageStore(logger.NewLogger("logger")).(*StateStore)
	t.Run("Content type is set from request, forward compatibility", func(t *testing.T) {
		contentType := "application/json"
		req := &state.SetRequest{
			ContentType: &contentType,
		}

		blobHeaders, err := s.createBlobHTTPHeadersFromRequest(req)
		assert.Nil(t, err)
		assert.Equal(t, "application/json", blobHeaders.ContentType)
	})
	t.Run("Content type and metadata provided (conflict), content type chosen", func(t *testing.T) {
		contentType := "application/json"
		req := &state.SetRequest{
			ContentType: &contentType,
			Metadata: map[string]string{
				contentType: "text/plain",
			},
		}

		blobHeaders, err := s.createBlobHTTPHeadersFromRequest(req)
		assert.Nil(t, err)
		assert.Equal(t, "application/json", blobHeaders.ContentType)
	})
	t.Run("ContentType not provided, metadata provided set backward compatibility", func(t *testing.T) {
		req := &state.SetRequest{
			Metadata: map[string]string{
				contentType: "text/plain",
			},
		}

		blobHeaders, err := s.createBlobHTTPHeadersFromRequest(req)
		assert.Nil(t, err)
		assert.Equal(t, "text/plain", blobHeaders.ContentType)
	})
}
