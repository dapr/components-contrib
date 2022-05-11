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
	kafka  *kafka.Kafka
	topics map[string]bool
}

func (p *PubSub) Init(metadata pubsub.Metadata) error {
	p.topics = make(map[string]bool)
	return p.kafka.Init(metadata.Properties)
}

func (p *PubSub) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	topics := p.addTopic(req.Topic)

	return p.kafka.Subscribe(topics, req.Metadata, newSubscribeAdapter(handler).adapter)
}

func (p *PubSub) addTopic(newTopic string) []string {
	// Add topic to our map of topics
	p.topics[newTopic] = true

	topics := make([]string, len(p.topics))

	i := 0
	for topic := range p.topics {
		topics[i] = topic
		i++
	}

	return topics
}

// NewKafka returns a new kafka pubsub instance.
func NewKafka(logger logger.Logger) pubsub.PubSub {
	k := kafka.NewKafka(logger)
	// in kafka pubsub component, enable consumer retry by default
	k.DefaultConsumeRetryEnabled = true
	return &PubSub{
		kafka: k,
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

// subscribeAdapter is used to adapter pubsub.Handler to kafka.EventHandler with the same content.
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
