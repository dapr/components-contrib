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
	"fmt"
	"sync"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/internal/component/kafka"
	"github.com/dapr/components-contrib/pubsub"
)

type PubSub struct {
	kafka  *kafka.Kafka
	topics map[string]pubsub.Handler
	lock   sync.RWMutex
}

func (p *PubSub) Init(metadata pubsub.Metadata) error {
	return p.kafka.Init(metadata.Properties)
}

func (p *PubSub) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	// Add the topic to the list of those we subscribe to
	p.lock.Lock()
	p.topics[req.Topic] = handler
	topics := p.topicList()
	p.lock.Unlock()

	return p.kafka.Subscribe(context.Background(), topics, req.Metadata, p.handleMessage)
}

func (p *PubSub) handleMessage(ctx context.Context, event *kafka.NewEvent) error {
	// Get the handler
	handler, ok := p.topics[event.Topic]
	if !ok || handler == nil {
		return fmt.Errorf("handler for messages of topic %s not found", event.Topic)
	}

	// Invoke the handler
	msg := &pubsub.NewMessage{
		Topic:       event.Topic,
		Data:        event.Data,
		Metadata:    event.Metadata,
		ContentType: event.ContentType,
	}
	return handler(ctx, msg)
}

func (p *PubSub) topicList() []string {
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
		kafka:  k,
		topics: make(map[string]pubsub.Handler),
		lock:   sync.RWMutex{},
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
