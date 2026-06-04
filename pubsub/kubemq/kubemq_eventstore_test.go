/*
Copyright 2026 The Dapr Authors
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

package kubemq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type kubemqEventsStoreMock struct {
	resultError    error
	subscribeErr   error
	resultCh       chan error
	publishError   error
	publishTimeout time.Duration
}

func (k *kubemqEventsStoreMock) publish(msg *kubemq.EventStore) error {
	if k.publishError != nil {
		return k.publishError
	}
	go func() {
		if k.publishTimeout > 0 {
			time.Sleep(k.publishTimeout)
		}
		k.resultCh <- k.resultError
	}()

	return nil
}

func (k *kubemqEventsStoreMock) Stream(ctx context.Context, onResult func(result *kubemq.EventStoreResult, err error)) (func(msg *kubemq.EventStore) error, error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case result := <-k.resultCh:
				onResult(&kubemq.EventStoreResult{
					Id:   "",
					Sent: false,
					Err:  result,
				}, nil)
			}
		}
	}()
	return k.publish, nil
}

func (k *kubemqEventsStoreMock) Subscribe(ctx context.Context, request *kubemq.EventsStoreSubscription, onEvent func(msg *kubemq.EventStoreReceive, err error)) error {
	return k.subscribeErr
}

func (k *kubemqEventsStoreMock) Close() error {
	return nil
}

func (k *kubemqEventsStoreMock) setResultError(err error) *kubemqEventsStoreMock {
	k.resultError = err
	return k
}

func (k *kubemqEventsStoreMock) setSubscribeError(err error) *kubemqEventsStoreMock {
	k.subscribeErr = err
	return k
}

func (k *kubemqEventsStoreMock) setPublishTimeout(timeout time.Duration) *kubemqEventsStoreMock {
	k.publishTimeout = timeout
	return k
}

func (k *kubemqEventsStoreMock) setPublishError(err error) *kubemqEventsStoreMock {
	k.publishError = err
	return k
}

func newKubemqEventsStoreMock() *kubemqEventsStoreMock {
	return &kubemqEventsStoreMock{
		resultError:  nil,
		subscribeErr: nil,
		resultCh:     make(chan error, 1),
	}
}

func Test_kubeMQEventsStore_Publish(t *testing.T) {
	tests := []struct {
		name        string
		req         *pubsub.PublishRequest
		timeout     time.Duration
		publishErr  error
		resultError error
		wantErr     bool
	}{
		{
			name: "publish with no error",
			req: &pubsub.PublishRequest{
				Data:  []byte("data"),
				Topic: "some-topic",
			},
			resultError: nil,

			wantErr: false,
		},
		{
			name: "publish with  error",
			req: &pubsub.PublishRequest{
				Data:  []byte("data"),
				Topic: "some-topic",
			},
			resultError: errors.New("some error"),
			wantErr:     true,
		},
		{
			name: "publish with timeout error",
			req: &pubsub.PublishRequest{
				Data:  []byte("data"),
				Topic: "some-topic",
			},
			resultError: nil,
			timeout:     3 * time.Second,
			wantErr:     true,
		},
		{
			name: "publish with publish error",
			req: &pubsub.PublishRequest{
				Data:  []byte("data"),
				Topic: "some-topic",
			},
			resultError: nil,
			publishErr:  errors.New("some error"),
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		k := newKubeMQEventsStore(logger.NewLogger("kubemq-test"))
		k.ctx, k.ctxCancel = context.WithCancel(t.Context())
		client := newKubemqEventsStoreMock().
			setResultError(tt.resultError).
			setPublishError(tt.publishErr)
		k.isInitialized = true
		k.metadata = &kubemqMetadata{
			internalHost: "",
			internalPort: 0,
			ClientID:     "some-client-id",
			AuthToken:    "",
			Group:        "",
			IsStore:      true,
		}
		if tt.timeout > 0 {
			k.waitForResultTimeout = tt.timeout - 1*time.Second
			client.setPublishTimeout(tt.timeout)
		}
		k.client = client
		_ = k.setPublishStream()
		err := k.Publish(tt.req)
		if tt.wantErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		_ = k.Features()
		_ = k.Close()
	}
}

func newInitializedEventsStoreClient(client kubemqEventsStoreClient, resultTimeout time.Duration) *kubeMQEventStore {
	k := newKubeMQEventsStore(logger.NewLogger("kubemq-test"))
	k.ctx, k.ctxCancel = context.WithCancel(context.Background())
	k.isInitialized = true
	k.metadata = &kubemqMetadata{ClientID: "some-client-id", IsStore: true}
	if resultTimeout > 0 {
		k.waitForResultTimeout = resultTimeout
	}
	k.client = client
	_ = k.setPublishStream()
	return k
}

func Test_kubeMQEventsStore_Publish_ErrorClassification(t *testing.T) {
	t.Run("empty topic is terminal", func(t *testing.T) {
		k := newInitializedEventsStoreClient(newKubemqEventsStoreMock(), 0)
		defer k.Close()

		err := k.Publish(&pubsub.PublishRequest{Data: []byte("data"), Topic: ""})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("broker publish failure is retriable", func(t *testing.T) {
		client := newKubemqEventsStoreMock().setPublishError(errors.New("broker boom"))
		k := newInitializedEventsStoreClient(client, 0)
		defer k.Close()

		err := k.Publish(&pubsub.PublishRequest{Data: []byte("data"), Topic: "some-topic"})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
	})

	t.Run("broker result error is retriable", func(t *testing.T) {
		client := newKubemqEventsStoreMock().setResultError(errors.New("broker rejected"))
		k := newInitializedEventsStoreClient(client, 0)
		defer k.Close()

		err := k.Publish(&pubsub.PublishRequest{Data: []byte("data"), Topic: "some-topic"})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
	})

	t.Run("result timeout is retriable", func(t *testing.T) {
		client := newKubemqEventsStoreMock().setPublishTimeout(3 * time.Second)
		k := newInitializedEventsStoreClient(client, 1*time.Second)
		defer k.Close()

		err := k.Publish(&pubsub.PublishRequest{Data: []byte("data"), Topic: "some-topic"})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
	})
}

func Test_kubeMQkubeMQEventsStore_Subscribe(t *testing.T) {
	tests := []struct {
		name             string
		reqMsg           *pubsub.NewMessage
		subscribeError   error
		subscribeHandler pubsub.Handler
		wantErr          bool
	}{
		{
			name: "subscribe with no error",
			reqMsg: &pubsub.NewMessage{
				Data:  []byte("data"),
				Topic: "some-topic",
			},
			subscribeHandler: func(ctx context.Context, msg *pubsub.NewMessage) error {
				return nil
			},
			subscribeError: nil,
			wantErr:        false,
		}, {
			name: "subscribe with error",
			reqMsg: &pubsub.NewMessage{
				Data:  []byte("data"),
				Topic: "some-topic",
			},
			subscribeHandler: func(ctx context.Context, msg *pubsub.NewMessage) error {
				return nil
			},
			subscribeError: errors.New("some error"),
			wantErr:        true,
		},
	}
	for _, tt := range tests {
		k := newKubeMQEventsStore(logger.NewLogger("kubemq-test"))
		k.ctx, k.ctxCancel = context.WithCancel(t.Context())
		k.client = newKubemqEventsStoreMock().
			setSubscribeError(tt.subscribeError)
		k.isInitialized = true
		k.metadata = &kubemqMetadata{
			internalHost: "",
			internalPort: 0,
			ClientID:     "some-client-id",
			AuthToken:    "",
			Group:        "",
			IsStore:      true,
		}
		err := k.Subscribe(k.ctx, pubsub.SubscribeRequest{Topic: "some-topic"}, tt.subscribeHandler)
		if tt.wantErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		_ = k.Features()
		_ = k.Close()
	}
}
