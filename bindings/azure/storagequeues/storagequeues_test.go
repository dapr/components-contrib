// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
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
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockHelper struct {
	mock.Mock
}

func (m *MockHelper) Init(accountName string, accountKey string, queueName string, decodeBase64 bool) error {
	retvals := m.Called(accountName, accountKey, queueName, decodeBase64)

	return retvals.Error(0)
}

func (m *MockHelper) Write(data []byte, ttl *time.Duration) error {
	retvals := m.Called(data, ttl)

	return retvals.Error(0)
}

func (m *MockHelper) Read(ctx context.Context, consumer *consumer) error {
	return nil
}

func TestWriteQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(in *time.Duration) bool {
		return in == nil
	})).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(&r)

	assert.Nil(t, err)
}

func TestWriteWithTTLInQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfTypeArgument("[]uint8"), mock.MatchedBy(func(in *time.Duration) bool {
		return in != nil && *in == time.Second
	})).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(&r)

	assert.Nil(t, err)
}

func TestWriteWithTTLInWrite(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfTypeArgument("[]uint8"), mock.MatchedBy(func(in *time.Duration) bool {
		return in != nil && *in == time.Second
	})).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{
		Data:     []byte("This is my message"),
		Metadata: map[string]string{metadata.TTLMetadataKey: "1"},
	}

	_, err = a.Invoke(&r)

	assert.Nil(t, err)
}

// Uncomment this function to write a message to local storage queue
/* func TestWriteLocalQueue(t *testing.T) {

	a := AzureStorageQueues{helper: &AzureQueueHelper{reqURI: "http://127.0.0.1:10001/%s/%s"}}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	err = a.Write(&r)

	assert.Nil(t, err)
} */

func TestReadQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)
	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(&r)

	assert.Nil(t, err)

	handler := func(data *bindings.ReadResponse) ([]byte, error) {
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")

		return nil, nil
	}

	go a.Read(handler)

	time.Sleep(5 * time.Second)

	pid := syscall.Getpid()
	proc, _ := os.FindProcess(pid)
	proc.Signal(os.Interrupt)
}

func TestReadQueueDecode(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1", "decodeBase64": "true"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("VGhpcyBpcyBteSBtZXNzYWdl")}

	_, err = a.Invoke(&r)

	assert.Nil(t, err)

	handler := func(data *bindings.ReadResponse) ([]byte, error) {
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")

		return nil, nil
	}

	go a.Read(handler)

	time.Sleep(5 * time.Second)

	pid := syscall.Getpid()
	proc, _ := os.FindProcess(pid)
	proc.Signal(os.Interrupt)
}

// Uncomment this function to test reding from local queue
/* func TestReadLocalQueue(t *testing.T) {
	a := AzureStorageQueues{helper: &AzureQueueHelper{reqURI: "http://127.0.0.1:10001/%s/%s"}}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	err = a.Write(&r)

	assert.Nil(t, err)

	var handler = func(data *bindings.ReadResponse) ([]byte, error) {
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")
		return nil, nil
	}

	_ = a.Read(handler)

	time.Sleep(30 * time.Second)

}
*/
func TestReadQueueNoMessage(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), false).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	handler := func(data *bindings.ReadResponse) ([]byte, error) {
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")

		return nil, nil
	}

	go a.Read(handler)

	time.Sleep(5 * time.Second)

	pid := syscall.Getpid()
	proc, _ := os.FindProcess(pid)
	proc.Signal(os.Interrupt)
}

func TestParseMetadata(t *testing.T) {
	var oneSecondDuration time.Duration = time.Second

	testCases := []struct {
		name               string
		properties         map[string]string
		expectedAccountKey string
		expectedQueueName  string
		expectedTTL        *time.Duration
	}{
		{
			name:               "Account and key",
			properties:         map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1"},
			expectedAccountKey: "myKey",
			expectedQueueName:  "queue1",
		},
		{
			name:               "Empty TTL",
			properties:         map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: ""},
			expectedAccountKey: "myKey",
			expectedQueueName:  "queue1",
		},
		{
			name:               "With TTL",
			properties:         map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "1"},
			expectedAccountKey: "myKey",
			expectedQueueName:  "queue1",
			expectedTTL:        &oneSecondDuration,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties

			a := NewAzureStorageQueues(logger.NewLogger("test"))
			meta, err := a.parseMetadata(m)

			assert.Nil(t, err)
			assert.Equal(t, tt.expectedAccountKey, meta.AccountKey)
			assert.Equal(t, tt.expectedQueueName, meta.QueueName)
			assert.Equal(t, tt.expectedTTL, meta.ttl)
		})
	}
}

func TestParseMetadataWithInvalidTTL(t *testing.T) {
	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces TTL",
			properties: map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "  "},
		},
		{
			name:       "Negative ttl",
			properties: map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "-1"},
		},
		{
			name:       "Non-numeric ttl",
			properties: map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "abc"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties

			a := NewAzureStorageQueues(logger.NewLogger("test"))
			_, err := a.parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
}
