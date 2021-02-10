// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package blobstorage

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccount": "account", "storageAccessKey": "key", "container": "test", "decodeBase64": "true"}
	blonStorage := NewAzureBlobStorage(logger.NewLogger("test"))
	meta, err := blonStorage.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "test", meta.Container)
	assert.Equal(t, "account", meta.StorageAccount)
	assert.Equal(t, "key", meta.StorageAccessKey)
	assert.Equal(t, "true", meta.DecodeBase64)
}
