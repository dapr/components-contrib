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
	"errors"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/common/component/kafka"
	commonutils "github.com/dapr/components-contrib/common/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

type PubSub struct {
	kafka  *kafka.Kafka
	logger logger.Logger

	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func (p *PubSub) Init(ctx context.Context, metadata pubsub.Metadata) error {
	return p.kafka.Init(ctx, metadata.Properties)
}

func (p *PubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if p.closed.Load() {
		return errors.New("component is closed")
	}
	valueSchemaType, err := kafka.GetValueSchemaType(req.Metadata)
	if err != nil {
		return err
	}
	handlerConfig := kafka.SubscriptionHandlerConfig{
		IsBulkSubscribe: false,
		Handler:         adaptHandler(handler),
		ValueSchemaType: valueSchemaType,
	}

	p.subscribeUtil(ctx, req, handlerConfig)
	return nil
}

func (p *PubSub) BulkSubscribe(ctx context.Context, req pubsub.SubscribeRequest,
	handler pubsub.BulkHandler,
) error {
	if p.closed.Load() {
		return errors.New("component is closed")
	}

	subConfig := pubsub.BulkSubscribeConfig{
		MaxMessagesCount:   commonutils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxMessagesCount, kafka.DefaultMaxBulkSubCount),
		MaxAwaitDurationMs: commonutils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxAwaitDurationMs, kafka.DefaultMaxBulkSubAwaitDurationMs),
	}
	valueSchemaType, err := kafka.GetValueSchemaType(req.Metadata)
	if err != nil {
		return err
	}
	handlerConfig := kafka.SubscriptionHandlerConfig{
		IsBulkSubscribe: true,
		SubscribeConfig: subConfig,
		BulkHandler:     adaptBulkHandler(handler),
		ValueSchemaType: valueSchemaType,
	}
	p.subscribeUtil(ctx, req, handlerConfig)
	return nil
}

func (p *PubSub) subscribeUtil(ctx context.Context, req pubsub.SubscribeRequest, handlerConfig kafka.SubscriptionHandlerConfig) {
	ctx, cancel := context.WithCancel(ctx)

	p.wg.Add(1)
	go func() {
		select {
		case <-ctx.Done():
		case <-p.closeCh:
		}
		cancel()
		p.wg.Done()
	}()

	p.kafka.Subscribe(ctx, handlerConfig, req.Topic)
}

// NewKafka returns a new kafka pubsub instance.
func NewKafka(logger logger.Logger) pubsub.PubSub {
	k := kafka.NewKafka(logger)
	// in kafka pubsub component, enable consumer retry by default
	k.DefaultConsumeRetryEnabled = true
	return &PubSub{
		kafka:   k,
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Publish message to Kafka cluster.
func (p *PubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if p.closed.Load() {
		return errors.New("component is closed")
	}

	return p.kafka.Publish(ctx, req.Topic, req.Data, req.Metadata)
}

// BatchPublish messages to Kafka cluster.
func (p *PubSub) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	if p.closed.Load() {
		return pubsub.BulkPublishResponse{}, errors.New("component is closed")
	}

	return p.kafka.BulkPublish(ctx, req.Topic, req.Entries, req.Metadata)
}

func (p *PubSub) Close() (err error) {
	defer p.wg.Wait()
	if p.closed.CompareAndSwap(false, true) {
		close(p.closeCh)
	}
	return p.kafka.Close()
}

func (p *PubSub) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureBulkPublish}
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
				EntryId:     leafEvent.EntryId,
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

// GetComponentMetadata returns the metadata of the component.
func (p *PubSub) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := kafka.KafkaMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return
}
