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

package kafka

import (
	"context"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/internal/component/kafka"
	"github.com/dapr/components-contrib/pubsub"
)

type PubSub struct {
	kafka *kafka.Kafka
}

func (p *PubSub) Init(metadata pubsub.Metadata) error {
	return p.kafka.Init(metadata.Properties)
}

func (p *PubSub) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	return p.kafka.Subscribe(req.Topic, req.Metadata, newSubscribeAdapter(handler).adapter)
}

// NewKafka returns a new kafka pubsub instance.
func NewKafka(l logger.Logger) pubsub.PubSub {
	return &PubSub{
		kafka: kafka.NewKafka(l),
	}
}

// Publish message to Kafka cluster.
func (p *PubSub) Publish(req *pubsub.PublishRequest) error {
	return p.kafka.Publish(req.Topic, req.Data, req.Metadata)
}

func (p *PubSub) Close() (err error) {
	return p.kafka.Close()
}

func (p *PubSub) Features() []pubsub.Feature {
	return nil
}

type subscribeAdapter struct {
	handler pubsub.Handler
}

func newSubscribeAdapter(handler pubsub.Handler) *subscribeAdapter {
	return &subscribeAdapter{handler: handler}
}

func (a *subscribeAdapter) adapter(ctx context.Context, event *kafka.NewEvent) error {
	return a.handler(ctx, &pubsub.NewMessage{
		Topic:       event.Topic,
		Data:        event.Data,
		Metadata:    event.Metadata,
		ContentType: event.ContentType,
	})
}
