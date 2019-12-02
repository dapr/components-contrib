// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package storagequeues

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockHelper struct {
	mock.Mock
}

func (m *MockHelper) Init(accountName string, accountKey string, queueName string) error {
	retvals := m.Called(accountName, accountKey, queueName)
	return retvals.Error(0)
}

func (m *MockHelper) Write(data []byte) error {
	return nil
}

func (m *MockHelper) Read(ctx context.Context, consumer *consumer) error {
	return nil
}

func TestWriteQueue(t *testing.T) {

	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil)

	a := AzureStorageQueues{helper: mm}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.WriteRequest{Data: []byte("This is my message")}

	err = a.Write(&r)

	assert.Nil(t, err)
}

// Uncomment this function to write a message to local storage queue
/* func TestWriteLocalQueue(t *testing.T) {

	a := AzureStorageQueues{helper: &AzureQueueHelper{reqURI: "http://127.0.0.1:10001/%s/%s"}}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.WriteRequest{Data: []byte("This is my message")}

	err = a.Write(&r)

	assert.Nil(t, err)
} */

func TestReadQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil)

	a := AzureStorageQueues{helper: mm}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1"}

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

	go a.Read(handler)

	time.Sleep(5 * time.Second)

	pid := syscall.Getpid()
	proc, err := os.FindProcess(pid)
	proc.Signal(os.Interrupt)
}

// Uncomment this function to test reding from local queue
/* func TestReadLocalQueue(t *testing.T) {
	a := AzureStorageQueues{helper: &AzureQueueHelper{reqURI: "http://127.0.0.1:10001/%s/%s"}}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1"}

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
*/
func TestReadQueueNoMessage(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(nil)

	a := AzureStorageQueues{helper: mm}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"accountKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queueName": "queue1", "accountName": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	var handler = func(data *bindings.ReadResponse) error {
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")
		return nil
	}

	go a.Read(handler)

	time.Sleep(5 * time.Second)

	pid := syscall.Getpid()
	proc, err := os.FindProcess(pid)
	proc.Signal(os.Interrupt)

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
