package inmemory

import (
	"context"

	"github.com/asaskevich/EventBus"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type bus struct {
	bus EventBus.Bus
	ctx context.Context
	log logger.Logger
}

func New(logger logger.Logger) pubsub.PubSub {
	return &bus{
		log: logger,
	}
}

func (a *bus) Close() error {
	return nil
}

func (a *bus) Features() []pubsub.Feature {
	return nil
}

func (a *bus) Init(metadata pubsub.Metadata) error {
	a.bus = EventBus.New()
	a.ctx = context.Background()

	return nil
}

func (a *bus) Publish(req *pubsub.PublishRequest) error {
	a.bus.Publish(req.Topic, a.ctx, req.Data)

	return nil
}

func (a *bus) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	return a.bus.Subscribe(req.Topic, func(ctx context.Context, data []byte) {
		for i := 0; i < 10; i++ {
			if err := handler(ctx, &pubsub.NewMessage{Data: data, Topic: req.Topic, Metadata: req.Metadata}); err != nil {
				a.log.Error(err)

				continue
			}

			return
		}
	})
}
