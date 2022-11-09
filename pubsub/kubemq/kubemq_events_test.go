package kubemq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go"
	"github.com/stretchr/testify/assert"

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
			publishErr:  fmt.Errorf("some error"),
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		k := newkubeMQEvents(logger.NewLogger("kubemq-test"))
		k.ctx, k.ctxCancel = context.WithCancel(context.Background())
		client := newKubemqEventsMock().
			setResultError(tt.resultError).
			setPublishError(tt.publishErr)
		k.isInitialized = true
		k.metadata = &metadata{
			host:      "",
			port:      0,
			clientID:  "some-client-id",
			authToken: "",
			group:     "",
			isStore:   false,
		}
		if tt.timeout > 0 {
			k.waitForResultTimeout = tt.timeout - 1*time.Second
			client.setPublishTimeout(tt.timeout)
		}
		k.client = client
		_ = k.setPublishStream()
		err := k.Publish(tt.req)
		if tt.wantErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		_ = k.Features()
		_ = k.Close()
	}
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
			subscribeError: fmt.Errorf("some error"),
			wantErr:        true,
		},
	}
	for _, tt := range tests {
		k := newkubeMQEvents(logger.NewLogger("kubemq-test"))
		k.ctx, k.ctxCancel = context.WithCancel(context.Background())
		k.client = newKubemqEventsMock().
			setSubscribeError(tt.subscribeError)
		k.isInitialized = true
		k.metadata = &metadata{
			host:      "",
			port:      0,
			clientID:  "some-client-id",
			authToken: "",
			group:     "",
			isStore:   false,
		}
		err := k.Subscribe(k.ctx, pubsub.SubscribeRequest{Topic: "some-topic"}, tt.subscribeHandler)
		if tt.wantErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		_ = k.Features()
		_ = k.Close()
	}
}
