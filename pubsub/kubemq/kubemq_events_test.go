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

type kubemqEventsMock struct {
	resultError    error
	subscribeErr   error
	resultCh       chan error
	publishError   error
	publishTimeout time.Duration
}

func (k *kubemqEventsMock) publish(msg *kubemq.Event) error {
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

func (k *kubemqEventsMock) Stream(ctx context.Context, onError func(err error)) (func(msg *kubemq.Event) error, error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case result := <-k.resultCh:
				onError(result)
			}
		}
	}()
	return k.publish, nil
}

func (k *kubemqEventsMock) Subscribe(ctx context.Context, request *kubemq.EventsSubscription, onEvent func(msg *kubemq.Event, err error)) error {
	return k.subscribeErr
}

func (k *kubemqEventsMock) Close() error {
	return nil
}

func (k *kubemqEventsMock) setResultError(err error) *kubemqEventsMock {
	k.resultError = err
	return k
}

func (k *kubemqEventsMock) setSubscribeError(err error) *kubemqEventsMock {
	k.subscribeErr = err
	return k
}

func (k *kubemqEventsMock) setPublishTimeout(timeout time.Duration) *kubemqEventsMock {
	k.publishTimeout = timeout
	return k
}

func (k *kubemqEventsMock) setPublishError(err error) *kubemqEventsMock {
	k.publishError = err
	return k
}

func newKubemqEventsMock() *kubemqEventsMock {
	return &kubemqEventsMock{
		resultError:  nil,
		subscribeErr: nil,
		resultCh:     make(chan error, 1),
	}
}

func Test_kubeMQEvents_Publish(t *testing.T) {
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
		k := newkubeMQEvents(logger.NewLogger("kubemq-test"))
		k.ctx, k.ctxCancel = context.WithCancel(t.Context())
		client := newKubemqEventsMock().
			setResultError(tt.resultError).
			setPublishError(tt.publishErr)
		k.isInitialized = true
		k.metadata = &kubemqMetadata{
			internalHost: "",
			internalPort: 0,
			ClientID:     "some-client-id",
			AuthToken:    "",
			Group:        "",
			IsStore:      false,
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

func newInitializedEventsClient(t *testing.T, client kubemqEventsClient) *kubeMQEvents {
	t.Helper()
	k := newkubeMQEvents(logger.NewLogger("kubemq-test"))
	k.ctx, k.ctxCancel = context.WithCancel(t.Context())
	k.isInitialized = true
	k.metadata = &kubemqMetadata{ClientID: "some-client-id"}
	k.client = client
	require.NoError(t, k.setPublishStream())
	return k
}

func Test_kubeMQEvents_Publish_ErrorClassification(t *testing.T) {
	t.Run("empty topic is terminal", func(t *testing.T) {
		k := newInitializedEventsClient(t, newKubemqEventsMock())
		defer k.Close()

		err := k.Publish(&pubsub.PublishRequest{Data: []byte("data"), Topic: ""})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("broker publish failure is retriable", func(t *testing.T) {
		client := newKubemqEventsMock().setPublishError(errors.New("broker boom"))
		k := newInitializedEventsClient(t, client)
		defer k.Close()

		err := k.Publish(&pubsub.PublishRequest{Data: []byte("data"), Topic: "some-topic"})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
	})
}

func Test_kubeMQEvents_Subscribe(t *testing.T) {
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
		k := newkubeMQEvents(logger.NewLogger("kubemq-test"))
		k.ctx, k.ctxCancel = context.WithCancel(t.Context())
		k.client = newKubemqEventsMock().
			setSubscribeError(tt.subscribeError)
		k.isInitialized = true
		k.metadata = &kubemqMetadata{
			internalHost: "",
			internalPort: 0,
			ClientID:     "some-client-id",
			AuthToken:    "",
			Group:        "",
			IsStore:      false,
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
