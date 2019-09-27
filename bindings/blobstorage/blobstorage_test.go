package blobstorage

import (
	"testing"

	"github.com/actionscore/components-contrib/bindings"
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
	assert.Equal(t, "key", meta.StorageAccessKey)
}
