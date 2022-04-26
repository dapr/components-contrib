/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storagequeues

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type MockHelper struct {
	mock.Mock
}

func (m *MockHelper) Init(endpoint, accountName, accountKey, queueName string, decodeBase64 bool) error {
	retvals := m.Called(endpoint, accountName, accountKey, queueName, decodeBase64)

	return retvals.Error(0)
}

func (m *MockHelper) Write(ctx context.Context, data []byte, ttl *time.Duration) error {
	retvals := m.Called(data, ttl)

	return retvals.Error(0)
}

func (m *MockHelper) Read(ctx context.Context, consumer *consumer) error {
	return nil
}

func TestWriteQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", "", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(in *time.Duration) bool {
		return in == nil
	})).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(context.TODO(), &r)

	assert.Nil(t, err)
}

func TestWriteWithTTLInQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", "", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfTypeArgument("[]uint8"), mock.MatchedBy(func(in *time.Duration) bool {
		return in != nil && *in == time.Second
	})).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(context.TODO(), &r)

	assert.Nil(t, err)
}

func TestWriteWithTTLInWrite(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Init", "", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
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

	_, err = a.Invoke(context.TODO(), &r)

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
	mm.On("Init", "", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)
	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(context.TODO(), &r)

	assert.Nil(t, err)

	handler := func(ctx context.Context, data *bindings.ReadResponse) ([]byte, error) {
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
	mm.On("Init", "", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("bool")).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1", "decodeBase64": "true"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("VGhpcyBpcyBteSBtZXNzYWdl")}

	_, err = a.Invoke(context.TODO(), &r)

	assert.Nil(t, err)

	handler := func(ctx context.Context, data *bindings.ReadResponse) ([]byte, error) {
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
//nolint:godot
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
	mm.On("Init", "", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string"), false).Return(nil)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	handler := func(ctx context.Context, data *bindings.ReadResponse) ([]byte, error) {
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
		expectedQueueEndpoint string
		expectedTTL        *time.Duration
	}{
		{
			name:               "Account and key",
			properties:         map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1"},
			expectedAccountKey: "myKey",
			expectedQueueName:  "queue1",
			expectedQueueEndpoint: "",
		},
		{
			name:               "Accout, key, and endpoint",
			properties:         map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "someAccount", "queueEndpoint": "https://foo.example.com:10001"},
			expectedAccountKey: "myKey",
			expectedQueueName:  "queue1",
			expectedQueueEndpoint: "https://foo.example.com:10001",
		},
		{
			name:               "Empty TTL",
			properties:         map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: ""},
			expectedAccountKey: "myKey",
			expectedQueueName:  "queue1",
			expectedQueueEndpoint: "",
		},
		{
			name:               "With TTL",
			properties:         map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "1"},
			expectedAccountKey: "myKey",
			expectedQueueName:  "queue1",
			expectedTTL:        &oneSecondDuration,
			expectedQueueEndpoint: "",
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
			assert.Equal(t, tt.expectedQueueEndpoint, meta.QueueEndpoint)
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
