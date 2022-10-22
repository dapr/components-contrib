package kubemq

import (
	"context"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type kubeMQ struct {
	metadata         *metadata
	logger           logger.Logger
	ctx              context.Context
	ctxCancel        context.CancelFunc
	eventsClient     *kubeMQEvents
	eventStoreClient *kubeMQEventStore
}

func NewKubeMQ(logger logger.Logger) pubsub.PubSub {
	return &kubeMQ{
		metadata:         nil,
		logger:           logger,
		ctx:              nil,
		ctxCancel:        nil,
		eventsClient:     nil,
		eventStoreClient: nil,
	}
}

func (k *kubeMQ) Init(metadata pubsub.Metadata) error {
	meta, err := createMetadata(metadata)
	if err != nil {
		k.logger.Errorf("error init kubemq client error: %s", err.Error())
		return err
	}
	k.metadata = meta
	k.ctx, k.ctxCancel = context.WithCancel(context.Background())
	if meta.isStore {
		k.eventStoreClient = newKubeMQEventsStore(k.logger)
		_ = k.eventStoreClient.Init(meta)
	} else {
		k.eventsClient = newkubeMQEvents(k.logger)
		_ = k.eventsClient.Init(meta)
	}
	return nil
}

func (k *kubeMQ) Features() []pubsub.Feature {
	return nil
}

func (k *kubeMQ) Publish(req *pubsub.PublishRequest) error {
	if k.metadata.isStore {
		return k.eventStoreClient.Publish(req)
	} else {
		return k.eventsClient.Publish(req)
	}
}

func (k *kubeMQ) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if k.metadata.isStore {
		return k.eventStoreClient.Subscribe(ctx, req, handler)
	} else {
		return k.eventsClient.Subscribe(ctx, req, handler)
	}
}

func (k *kubeMQ) Close() error {
	if k.metadata.isStore {
		return k.eventStoreClient.Close()
	} else {
		return k.eventsClient.Close()
	}
}
