// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package blobstorage

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccount": "account", "storageAccessKey": "key", "container": "test"}
	blonStorage := NewAzureBlobStorage()
	meta, err := blonStorage.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "test", meta.Container)
	assert.Equal(t, "account", meta.StorageAccount)
	storageAccessKey, err := meta.StorageAccessKey.Open()
	assert.NoError(t, err)
	defer storageAccessKey.Destroy()
	assert.Equal(t, "key", storageAccessKey.String())
}
