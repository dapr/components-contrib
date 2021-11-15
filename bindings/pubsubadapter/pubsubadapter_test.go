// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsubadapter_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/pubsubadapter"
	"github.com/dapr/components-contrib/pubsub"
)

type mockPubSub struct {
	pubsub.PubSub
	metadata         pubsub.Metadata
	initErr          error
	publishRequest   *pubsub.PublishRequest
	publishErr       error
	subscribeRequest pubsub.SubscribeRequest
	subscribeErr     error
	handler          pubsub.Handler
	closeCalled      bool
	closeErr         error
}

func (m *mockPubSub) Init(metadata pubsub.Metadata) error {
	m.metadata = metadata

	return m.initErr
}

func (m *mockPubSub) Publish(req *pubsub.PublishRequest) error {
	m.publishRequest = req

	return m.publishErr
}

func (m *mockPubSub) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	m.subscribeRequest = req
	m.handler = handler

	return m.subscribeErr
}

func (m *mockPubSub) Close() error {
	m.closeCalled = true

	return m.closeErr
}

func TestAdapter(t *testing.T) {
	testErr := errors.New("test")
	mock := mockPubSub{
		initErr:      testErr,
		publishErr:   testErr,
		subscribeErr: testErr,
		closeErr:     testErr,
	}
	a := pubsubadapter.New(&mock,
		pubsubadapter.WithReadTopicAttribute("myReadTopicAttribute"),
		pubsubadapter.WithInvokeTopicAttribute("myInvokeTopicAttribute"),
		pubsubadapter.WithInvokeTopicMetadata("myInvokeTopicMetadata"))

	data := []byte("hello test")
	metadata := map[string]string{
		"hello": "test",
	}

	t.Run("init", func(t *testing.T) {
		err := a.Init(bindings.Metadata{
			Name:       "testPubSub",
			Properties: map[string]string{},
		})
		require.EqualError(t, err, `pubsub adapter: "myReadTopicAttribute" attribute was missing`)
		err = a.Init(bindings.Metadata{
			Name: "testPubSub",
			Properties: map[string]string{
				"myReadTopicAttribute": "test",
			},
		})
		require.EqualError(t, err, `pubsub adapter: "myInvokeTopicAttribute" attribute was missing`)

		properties := map[string]string{
			"myReadTopicAttribute":   "myTopic",
			"myInvokeTopicAttribute": "myTopic",
			"myInvokeTopicMetadata":  "myTopic",
			"other":                  "test",
		}
		err = a.Init(bindings.Metadata{
			Name:       "testPubSub",
			Properties: properties,
		})
		assert.Same(t, testErr, err)

		mock.initErr = nil
		err = a.Init(bindings.Metadata{
			Name:       "testPubSub",
			Properties: properties,
		})
		require.NoError(t, err)
		assert.Equal(t, properties, mock.metadata.Properties)
	})

	t.Run("read", func(t *testing.T) {
		err := a.Read(func(resp *bindings.ReadResponse) ([]byte, error) {
			return nil, nil
		})
		assert.Same(t, testErr, err)

		var readResp *bindings.ReadResponse
		mock.subscribeErr = nil
		err = a.Read(func(resp *bindings.ReadResponse) ([]byte, error) {
			readResp = resp
			return nil, nil
		})
		assert.NoError(t, err)

		mock.handler(context.Background(), &pubsub.NewMessage{
			Data:     data,
			Metadata: metadata,
		})
		if assert.NotNil(t, readResp) {
			assert.Equal(t, data, readResp.Data)
			assert.Equal(t, metadata, readResp.Metadata)
		}
	})

	t.Run("invoke", func(t *testing.T) {
		_, err := a.Invoke(&bindings.InvokeRequest{
			Data:     data,
			Metadata: metadata,
		})
		assert.Same(t, testErr, err)

		mock.publishErr = nil
		_, err = a.Invoke(&bindings.InvokeRequest{
			Data:     data,
			Metadata: metadata,
		})
		require.NoError(t, err)
		assert.Equal(t, "myTopic", mock.publishRequest.Topic)
		assert.Equal(t, data, mock.publishRequest.Data)
		assert.Equal(t, metadata, mock.publishRequest.Metadata)

		metadata["myInvokeTopicMetadata"] = "myOtherTopic"
		_, err = a.Invoke(&bindings.InvokeRequest{
			Data:     data,
			Metadata: metadata,
		})
		require.NoError(t, err)
		assert.Equal(t, "testPubSub", mock.publishRequest.PubsubName)
		assert.Equal(t, "myOtherTopic", mock.publishRequest.Topic)
		assert.Equal(t, data, mock.publishRequest.Data)
		assert.Equal(t, metadata, mock.publishRequest.Metadata)
	})

	t.Run("operations", func(t *testing.T) {
		opers := a.Operations()
		assert.Equal(t,
			[]bindings.OperationKind{
				bindings.CreateOperation,
				bindings.OperationKind("publish"),
			},
			opers)
	})

	t.Run("close", func(t *testing.T) {
		assert.Same(t, testErr, a.Close())
		assert.True(t, mock.closeCalled)
	})
}
