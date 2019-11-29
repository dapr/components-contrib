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
	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "LDCIro1iN8LPqFo822X4Oecap/8s8anCNslB2pcxE1/pax/svHY7StnpGnOcIE1JiSU8IAVrag8t6QrjcYH/JQ==", "queueName": "queue1", "accountName": "daprcomp"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.WriteRequest{Data: []byte("This is my message")}

	err = a.Write(&r)

	assert.Nil(t, err)
}

func TestParseMetadata(t *testing.T) {
	//	var accountName = "daprcomp"
	//	var accountKey = "LDCIro1iN8LPqFo822X4Oecap/8s8anCNslB2pcxE1/pax/svHY7StnpGnOcIE1JiSU8IAVrag8t6QrjcYH/JQ=="

	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "myKey", "queueName": "queue1", "accountName": "daprcomp"}

	a := NewAzureStorageQueues()
	meta, err := a.parseMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "myKey", meta.AccountKey)
	assert.Equal(t, "queue1", meta.QueueName)
}
