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
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type MockHelper struct {
	mock.Mock
	messages chan []byte
	metadata *storageQueuesMetadata
}

func (m *MockHelper) Init(metadata bindings.Metadata) (*storageQueuesMetadata, error) {
	m.messages = make(chan []byte, 10)
	var err error
	m.metadata, err = parseMetadata(metadata)
	return m.metadata, err
}

func (m *MockHelper) Write(ctx context.Context, data []byte, ttl *time.Duration) error {
	m.messages <- data
	retvals := m.Called(data, ttl)
	return retvals.Error(0)
}

func (m *MockHelper) Read(ctx context.Context, consumer *consumer) error {
	retvals := m.Called(ctx, consumer)

	go func() {
		for msg := range m.messages {
			if m.metadata.DecodeBase64 {
				msg, _ = base64.StdEncoding.DecodeString(string(msg))
			}
			go consumer.callback(ctx, &bindings.ReadResponse{
				Data: msg,
			})
		}
	}()

	return retvals.Error(0)
}

func TestWriteQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.MatchedBy(func(in *time.Duration) bool {
		return in == nil
	})).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(context.Background(), &r)

	assert.Nil(t, err)
}

func TestWriteWithTTLInQueue(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Write", mock.AnythingOfTypeArgument("[]uint8"), mock.MatchedBy(func(in *time.Duration) bool {
		return in != nil && *in == time.Second
	})).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	_, err = a.Invoke(context.Background(), &r)

	assert.Nil(t, err)
}

func TestWriteWithTTLInWrite(t *testing.T) {
	mm := new(MockHelper)
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

	_, err = a.Invoke(context.Background(), &r)

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
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)
	mm.On("Read", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*storagequeues.consumer")).Return(nil)
	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("This is my message")}

	ctx, cancel := context.WithCancel(context.Background())
	_, err = a.Invoke(ctx, &r)

	assert.Nil(t, err)

	received := 0
	handler := func(ctx context.Context, data *bindings.ReadResponse) ([]byte, error) {
		received++
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")
		cancel()

		return nil, nil
	}

	a.Read(ctx, handler)
	select {
	case <-ctx.Done():
		// do nothing
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Timeout waiting for messages")
	}
	assert.Equal(t, 1, received)
}

func TestReadQueueDecode(t *testing.T) {
	mm := new(MockHelper)
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)
	mm.On("Read", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*storagequeues.consumer")).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1", "decodeBase64": "true"}

	err := a.Init(m)
	assert.Nil(t, err)

	r := bindings.InvokeRequest{Data: []byte("VGhpcyBpcyBteSBtZXNzYWdl")}

	ctx, cancel := context.WithCancel(context.Background())
	_, err = a.Invoke(ctx, &r)

	assert.Nil(t, err)

	received := 0
	handler := func(ctx context.Context, data *bindings.ReadResponse) ([]byte, error) {
		received++
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")
		cancel()

		return nil, nil
	}

	a.Read(ctx, handler)
	select {
	case <-ctx.Done():
		// do nothing
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Timeout waiting for messages")
	}
	assert.Equal(t, 1, received)
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
	mm.On("Write", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("*time.Duration")).Return(nil)
	mm.On("Read", mock.AnythingOfType("*context.cancelCtx"), mock.AnythingOfType("*storagequeues.consumer")).Return(nil)

	a := AzureStorageQueues{helper: mm, logger: logger.NewLogger("test")}

	m := bindings.Metadata{}
	m.Properties = map[string]string{"storageAccessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "queue": "queue1", "storageAccount": "devstoreaccount1"}

	err := a.Init(m)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	received := 0
	handler := func(ctx context.Context, data *bindings.ReadResponse) ([]byte, error) {
		received++
		s := string(data.Data)
		assert.Equal(t, s, "This is my message")

		return nil, nil
	}

	a.Read(ctx, handler)
	time.Sleep(1 * time.Second)
	cancel()
	assert.Equal(t, 0, received)
}

func TestParseMetadata(t *testing.T) {
	oneSecondDuration := time.Second

	testCases := []struct {
		name       string
		properties map[string]string
		// Account key is parsed in azauth
		// expectedAccountKey       string
		expectedQueueName         string
		expectedQueueEndpointURL  string
		expectedTTL               *time.Duration
		expectedVisibilityTimeout *time.Duration
	}{
		{
			name:       "Account and key",
			properties: map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1"},
			// expectedAccountKey:       "myKey",
			expectedQueueName:         "queue1",
			expectedQueueEndpointURL:  "",
			expectedVisibilityTimeout: ptr.Of(30 * time.Second),
		},
		{
			name:       "Accout, key, and endpoint",
			properties: map[string]string{"accountKey": "myKey", "queueName": "queue1", "storageAccount": "someAccount", "queueEndpointUrl": "https://foo.example.com:10001"},
			// expectedAccountKey:       "myKey",
			expectedQueueName:         "queue1",
			expectedQueueEndpointURL:  "https://foo.example.com:10001",
			expectedVisibilityTimeout: ptr.Of(30 * time.Second),
		},
		{
			name:       "Empty TTL",
			properties: map[string]string{"storageAccessKey": "myKey", "queue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: ""},
			// expectedAccountKey:       "myKey",
			expectedQueueName:         "queue1",
			expectedQueueEndpointURL:  "",
			expectedVisibilityTimeout: ptr.Of(30 * time.Second),
		},
		{
			name:       "With TTL",
			properties: map[string]string{"accessKey": "myKey", "storageAccountQueue": "queue1", "storageAccount": "devstoreaccount1", metadata.TTLMetadataKey: "1"},
			// expectedAccountKey:       "myKey",
			expectedQueueName:         "queue1",
			expectedTTL:               &oneSecondDuration,
			expectedQueueEndpointURL:  "",
			expectedVisibilityTimeout: ptr.Of(30 * time.Second),
		},
		{
			name:                      "With visibility timeout",
			properties:                map[string]string{"accessKey": "myKey", "storageAccountQueue": "queue1", "storageAccount": "devstoreaccount1", "visibilityTimeout": "5s"},
			expectedQueueName:         "queue1",
			expectedVisibilityTimeout: ptr.Of(5 * time.Second),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties

			meta, err := parseMetadata(m)

			assert.Nil(t, err)
			// assert.Equal(t, tt.expectedAccountKey, meta.AccountKey)
			assert.Equal(t, tt.expectedQueueName, meta.QueueName)
			assert.Equal(t, tt.expectedTTL, meta.ttl)
			assert.Equal(t, tt.expectedQueueEndpointURL, meta.QueueEndpoint)
			assert.Equal(t, tt.expectedVisibilityTimeout, meta.VisibilityTimeout)
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

			_, err := parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
}
