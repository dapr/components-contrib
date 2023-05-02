package kubemq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func getMockEventsClient() *kubeMQEvents {
	return &kubeMQEvents{
		client:               newKubemqEventsMock(),
		metadata:             nil,
		logger:               nil,
		publishFunc:          nil,
		resultChan:           nil,
		waitForResultTimeout: 0,
		isInitialized:        true,
	}
}

func getMockEventsStoreClient() *kubeMQEventStore {
	return &kubeMQEventStore{
		client:               newKubemqEventsStoreMock(),
		metadata:             nil,
		logger:               nil,
		publishFunc:          nil,
		resultChan:           nil,
		waitForResultTimeout: 0,
		isInitialized:        true,
	}
}

func Test_kubeMQ_Init(t *testing.T) {
	tests := []struct {
		name             string
		meta             pubsub.Metadata
		eventsClient     *kubeMQEvents
		eventStoreClient *kubeMQEventStore
		wantErr          bool
	}{
		{
			name: "init events store client",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":   "localhost:50000",
						"channel":   "test",
						"clientID":  "clientID",
						"authToken": "authToken",
						"group":     "group",
						"store":     "true",
						"useMock":   "true",
					},
				},
			},
			eventsClient:     nil,
			eventStoreClient: getMockEventsStoreClient(),
			wantErr:          false,
		},
		{
			name: "init events client",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":   "localhost:50000",
						"channel":   "test",
						"clientID":  "clientID",
						"authToken": "authToken",
						"group":     "group",
						"store":     "false",
						"useMock":   "true",
					},
				},
			},
			eventsClient:     getMockEventsClient(),
			eventStoreClient: nil,
			wantErr:          false,
		},
		{
			name: "init error",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address": "badaddress",
					},
				},
			},
			eventsClient:     nil,
			eventStoreClient: nil,
			wantErr:          true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := NewKubeMQ(logger.NewLogger("test"))
			err := k.Init(context.Background(), tt.meta)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func Test_kubeMQ_Close(t *testing.T) {
	type fields struct {
		metadata         *kubemqMetadata
		logger           logger.Logger
		eventsClient     *kubeMQEvents
		eventStoreClient *kubeMQEventStore
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "close events client",
			fields: fields{
				metadata: &kubemqMetadata{
					IsStore: false,
				},
				eventsClient:     getMockEventsClient(),
				eventStoreClient: nil,
			},
			wantErr: assert.NoError,
		},
		{
			name: "close events store client",
			fields: fields{
				metadata: &kubemqMetadata{
					IsStore: true,
				},
				eventsClient:     nil,
				eventStoreClient: getMockEventsStoreClient(),
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &kubeMQ{
				metadata:         tt.fields.metadata,
				logger:           tt.fields.logger,
				eventsClient:     tt.fields.eventsClient,
				eventStoreClient: tt.fields.eventStoreClient,
			}
			tt.wantErr(t, k.Close(), "Close()")
		})
	}
}
