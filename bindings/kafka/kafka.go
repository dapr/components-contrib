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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/component/kafka"
)

const (
	publishTopic = "publishTopic"
	topics       = "topics"
)

type Binding struct {
	kafka        *kafka.Kafka
	publishTopic string
	topics       []string
	logger       logger.Logger
}

// NewKafka returns a new kafka binding instance.
func NewKafka(logger logger.Logger) *Binding {
	k := kafka.NewKafka(logger)
	// in kafka binding component, disable consumer retry by default
	k.DefaultConsumeRetryEnabled = false
	return &Binding{
		kafka:  k,
		logger: logger,
	}
}

func (b *Binding) Init(metadata bindings.Metadata) error {
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

func (p *Binding) Close() (err error) {
	return p.kafka.Close()
}

func (b *Binding) Invoke(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	err := b.kafka.Publish(b.publishTopic, req.Data, req.Metadata)
	return nil, err
}

func (b *Binding) Read(handler bindings.Handler) error {
	if len(b.topics) == 0 {
		b.logger.Warnf("kafka binding: no topic defined, input bindings will not be started")
		return nil
	}

	ah := adaptHandler(handler)
	for _, t := range b.topics {
		b.kafka.AddTopicHandler(t, ah)
	}

	// Subscribe, in a background goroutine
	err := b.kafka.Subscribe(context.Background())
	if err != nil {
		return err
	}

	// Wait until we exit
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sigCh

	return nil
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
