// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
	s := NewAzureBlobStorageStore(logger.NewLogger("logger"))
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
		assert.Equal(t, "acc", meta.accountName)
		assert.Equal(t, "dapr", meta.containerName)
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
	t.Run("Content type is set from request", func(t *testing.T) {
		req := &state.SetRequest{
			ContentType: "application/json",
		}

		blobHeaders, err := createBlobHTTPHeadersFromRequest(req)
		assert.Nil(t, err)
		assert.Equal(t, "application/json", blobHeaders.ContentType)
	})
}
