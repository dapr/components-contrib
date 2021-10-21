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

package inmemory

import (
	"context"

	"github.com/asaskevich/EventBus"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type bus struct {
	bus EventBus.Bus
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

	return nil
}

func (a *bus) Publish(req *pubsub.PublishRequest) error {
	a.bus.Publish(req.Topic, req.Data)

	return nil
}

func (a *bus) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	retryHandler := func(data []byte) {
		for i := 0; i < 10; i++ {
			handleErr := handler(ctx, &pubsub.NewMessage{Data: data, Topic: req.Topic, Metadata: req.Metadata})
			if handleErr == nil {
				break
			}
			a.log.Error(handleErr)
		}
	}
	err := a.bus.SubscribeAsync(req.Topic, retryHandler, true)
	if err != nil {
		return err
	}

	// Unsubscribe when context is done
	go func() {
		<-ctx.Done()
		err := a.bus.Unsubscribe(req.Topic, retryHandler)
		if err != nil {
			a.log.Errorf("error while unsubscribing from topic %s: %v", req.Topic, err)
		}
	}()

	return nil
}

func (a *bus) Ping() error {
	return nil
}
