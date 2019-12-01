// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package storagequeues

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockHelper struct {
	mock.Mock
}

func (m *MockHelper) Init(accountName string, accountKey string, queueName string) error {

}

func (m *MockHelper) Write(data []byte) error {

}

func (m *MockHelper) Read(ctx context.Context, consumer *consumer) error {

}

func (m *MockHelper) NewSharedKeyCredential(accountName string, accountKey string) (*azqueue.SharedKeyCredential, error) {
	retvals := m.Called(accountName, accountKey)
	return retvals.Get(0).(*azqueue.SharedKeyCredential), retvals.Error(1)
}

func (m *MockHelper) NewQueueURL(url url.URL, p pipeline.Pipeline) azqueue.QueueURL {
	retvals := m.Called(url, p)
	return retvals.Get(0).(azqueue.QueueURL)
}

func (m *MockHelper) NewQueue() error {
	return nil
}

func TestWriteQueue(t *testing.T) {

	mm := new(MockHelper)
	mm.On("NewSharedKeyCredential", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(&azqueue.SharedKeyCredential{}, nil)
	//	mm.On("NewQueueURL", mock.AnythingOfType("url.URL"), mock.AnythingOfType("pipeline.Pipeline")).Return()
	a := AzureStorageQueues{helper: mm}

	//a := NewAzureStorageQueues()
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
		s := string(data.Data)
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
		s := string(data.Data)
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
