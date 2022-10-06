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
	"strings"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/component/kafka"
)

const (
	publishTopic = "publishTopic"
	topics       = "topics"
)

type Binding struct {
	kafka           *kafka.Kafka
	publishTopic    string
	topics          []string
	logger          logger.Logger
	subscribeCtx    context.Context
	subscribeCancel context.CancelFunc
}

// NewKafka returns a new kafka binding instance.
func NewKafka(logger logger.Logger) bindings.InputOutputBinding {
	k := kafka.NewKafka(logger)
	// in kafka binding component, disable consumer retry by default
	k.DefaultConsumeRetryEnabled = false
	return &Binding{
		kafka:  k,
		logger: logger,
	}
}

func (b *Binding) Init(metadata bindings.Metadata) error {
	b.subscribeCtx, b.subscribeCancel = context.WithCancel(context.Background())

	err := b.kafka.Init(metadata.Properties)
	if err != nil {
		return err
	}

	val, ok := metadata.Properties[publishTopic]
	if ok && val != "" {
		b.publishTopic = val
	}

	val, ok = metadata.Properties[topics]
	if ok && val != "" {
		b.topics = strings.Split(val, ",")
	}

	return nil
}

func (b *Binding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (b *Binding) Close() (err error) {
	b.subscribeCancel()
	return b.kafka.Close()
}

func (b *Binding) Invoke(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	err := b.kafka.Publish(b.publishTopic, req.Data, req.Metadata)
	return nil, err
}

func (b *Binding) Read(ctx context.Context, handler bindings.Handler) error {
	if len(b.topics) == 0 {
		b.logger.Warnf("kafka binding: no topic defined, input bindings will not be started")
		return nil
	}

	handlerConfig := kafka.SubscriptionHandlerConfig{
		IsBulkSubscribe: false,
		Handler:         adaptHandler(handler),
	}
	for _, t := range b.topics {
		b.kafka.AddTopicHandler(t, handlerConfig)
	}

	go func() {
		// Wait for context cancelation
		select {
		case <-ctx.Done():
		case <-b.subscribeCtx.Done():
		}

		// Remove the topic handler before restarting the subscriber
		for _, t := range b.topics {
			b.kafka.RemoveTopicHandler(t)
		}

		// If the component's context has been canceled, do not re-subscribe
		if b.subscribeCtx.Err() != nil {
			return
		}

		err := b.kafka.Subscribe(b.subscribeCtx)
		if err != nil {
			b.logger.Errorf("kafka binding: error re-subscribing: %v", err)
		}
	}()

	return b.kafka.Subscribe(b.subscribeCtx)
}

func adaptHandler(handler bindings.Handler) kafka.EventHandler {
	return func(ctx context.Context, event *kafka.NewEvent) error {
		_, err := handler(ctx, &bindings.ReadResponse{
			Data:        event.Data,
			Metadata:    event.Metadata,
			ContentType: event.ContentType,
		})
		return err
	}
}
