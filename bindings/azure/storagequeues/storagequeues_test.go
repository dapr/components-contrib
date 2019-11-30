// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package storagequeues

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
)

func TestWriteQueue(t *testing.T) {
	a := NewAzureStorageQueues()
	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1", "requestURI": "http://127.0.0.1:10001/%s/%s"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.WriteRequest{Data: []byte("This is my message")}

	err = a.Write(&r)

	assert.Nil(t, err)
}

func TestReadQueue(t *testing.T) {
	a := NewAzureStorageQueues()
	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1", "requestURI": "http://127.0.0.1:10001/%s/%s"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.WriteRequest{Data: []byte("This is my message")}

	err = a.Write(&r)

	assert.Nil(t, err)

	var handler = func(data *bindings.ReadResponse) error {
		s := string(data.Data[:])
		assert.Equal(t, s, "This is my message")
		return nil
	}

	_ = a.Read(handler)

	time.Sleep(30 * time.Second)

}
func TestReadQueueNoMessage(t *testing.T) {
	a := NewAzureStorageQueues()
	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1", "requestURI": "http://127.0.0.1:10001/%s/%s"}

	err := a.Init(m)
	assert.Nil(t, err)

	var handler = func(data *bindings.ReadResponse) error {
		s := string(data.Data[:])
		assert.Equal(t, s, "This is my message")
		return nil
	}

	_ = a.Read(handler)

	time.Sleep(30 * time.Second)

}

func TestParseMetadata(t *testing.T) {

	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "myKey", "queueName": "queue1", "accountName": "devstoreaccount1"}

	a := NewAzureStorageQueues()
	meta, err := a.parseMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "myKey", meta.AccountKey)
	assert.Equal(t, "queue1", meta.QueueName)
}
