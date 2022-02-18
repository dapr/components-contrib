/*
Copyright 2022 The Dapr Authors
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
