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
	"strconv"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/internal/component/kafka"
	"github.com/dapr/components-contrib/pubsub"
)

type PubSub struct {
	kafka           *kafka.Kafka
	logger          logger.Logger
	subscribeCtx    context.Context
	subscribeCancel context.CancelFunc
	pubsub.BulkSubscribeConfig
}

func (p *PubSub) Init(metadata pubsub.Metadata) error {
	p.subscribeCtx, p.subscribeCancel = context.WithCancel(context.Background())

	return p.kafka.Init(metadata.Properties)
}

func (p *PubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	p.kafka.AddTopicHandler(req.Topic, adaptHandler(handler))

	go func() {
		// Wait for context cancelation
		select {
		case <-ctx.Done():
		case <-p.subscribeCtx.Done():
		}

		// Remove the topic handler before restarting the subscriber
		p.kafka.RemoveTopicHandler(req.Topic)

		// If the component's context has been canceled, do not re-subscribe
		if p.subscribeCtx.Err() != nil {
			return
		}

		err := p.kafka.Subscribe(p.subscribeCtx)
		if err != nil {
			p.logger.Errorf("kafka pubsub: error re-subscribing: %v", err)
		}
	}()

	return p.kafka.Subscribe(p.subscribeCtx)
}

func (p *PubSub) BulkSubscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.BulkHandler) error {
	p.kafka.AddTopicBulkHandler(req.Topic, adaptBulkHandler(handler))
	maxBulkCount, err := strconv.Atoi(req.Metadata["maxBulkCount"])
	if err != nil {
		maxBulkCount = 20
	}
	maxBulkAwaitDurationMilliSeconds, err := strconv.Atoi(req.Metadata["maxBulkAwaitDurationMilliSeconds"])
	if err != nil {
		maxBulkAwaitDurationMilliSeconds = 20
	}
	maxBulkSizeBytes, err := strconv.Atoi(req.Metadata["maxBulkSizeBytes"])
	if err != nil {
		maxBulkSizeBytes = 20
	}
	p.kafka.AddBulkSubscribeConfig(maxBulkCount, maxBulkAwaitDurationMilliSeconds, maxBulkSizeBytes)

	go func() {
		// Wait for context cancelation
		select {
		case <-ctx.Done():
		case <-p.subscribeCtx.Done():
		}

		// Remove the topic handler before restarting the subscriber
		p.kafka.RemoveTopicBulkHandler(req.Topic)

		// If the component's context has been canceled, do not re-subscribe
		if p.subscribeCtx.Err() != nil {
			return
		}

		err := p.kafka.BulkSubscribe(p.subscribeCtx)
		if err != nil {
			p.logger.Errorf("kafka pubsub: error re-subscribing: %v", err)
		}
	}()

	return p.kafka.BulkSubscribe(p.subscribeCtx)
}

// NewKafka returns a new kafka pubsub instance.
func NewKafka(logger logger.Logger) pubsub.PubSub {
	k := kafka.NewKafka(logger)
	// in kafka pubsub component, enable consumer retry by default
	k.DefaultConsumeRetryEnabled = true
	return &PubSub{
		kafka:  k,
		logger: logger,
	}
}

// Publish message to Kafka cluster.
func (p *PubSub) Publish(req *pubsub.PublishRequest) error {
	return p.kafka.Publish(req.Topic, req.Data, req.Metadata)
}

func (p *PubSub) Close() (err error) {
	p.subscribeCancel()
	return p.kafka.Close()
}

func (p *PubSub) Features() []pubsub.Feature {
	return nil
}

func adaptHandler(handler pubsub.Handler) kafka.EventHandler {
	return func(ctx context.Context, event *kafka.NewEvent) error {
		return handler(ctx, &pubsub.NewMessage{
			Topic:       event.Topic,
			Data:        event.Data,
			Metadata:    event.Metadata,
			ContentType: event.ContentType,
		})
	}
}

func adaptBulkHandler(handler pubsub.BulkHandler) kafka.BulkEventHandler {
	return func(ctx context.Context, event *kafka.KafkaBulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error) {
		messages := make([]pubsub.BulkMessageEntry, 0)
		for _, leafEvent := range event.Entries {
			message := pubsub.BulkMessageEntry{
				EntryID:     leafEvent.EntryID,
				Event:       leafEvent.Event,
				Metadata:    leafEvent.Metadata,
				ContentType: leafEvent.ContentType,
			}
			messages = append(messages, message)
		}

		return handler(ctx, &pubsub.BulkMessage{
			Topic:    event.Topic,
			Entries:  messages,
			Metadata: event.Metadata,
		})
	}
}
