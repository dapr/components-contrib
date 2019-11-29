// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package storagequeues

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestWriteQueue(t *testing.T) {
	a := NewAzureStorageQueues()
	a.Write(nil)
}

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "myKey", "queueName": "queue1"}

	a := NewAzureStorageQueues()
	meta, err := a.parseMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "myKey", meta.AccountKey)
	assert.Equal(t, "queue1", meta.QueueName)
}
