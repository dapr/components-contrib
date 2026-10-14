/*
Copyright 2026 The Dapr Authors
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

// Package queues is the DaprMQ queues pub/sub component. A Dapr topic is the DaprMQ queue of the
// same name, so every subscriber competes for its messages whatever its consumer ID, and the
// queue can be shared with producers and consumers using the DaprMQ SDKs directly.
package queues

import (
	"context"
	"reflect"

	mq "github.com/olitomlinson/dapr-mq/sdks/go"

	"github.com/dapr/components-contrib/common/component/daprmq"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type daprMQQueues struct {
	*daprmq.PubSub
}

// NewDaprMQQueues returns a new DaprMQ queues pub/sub component.
func NewDaprMQQueues(logger logger.Logger) pubsub.PubSub {
	return &daprMQQueues{PubSub: daprmq.New(logger, queueEntity{})}
}

func (d *daprMQQueues) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(daprmq.Metadata{}), &metadataInfo, metadata.PubSubType)
	delete(metadataInfo, "consumerID") // only applies to topics, not queues
	return
}

type queueEntity struct{}

func (queueEntity) Publish(ctx context.Context, client *mq.Client, topic string, items []mq.EnqueueItem) error {
	_, err := client.Enqueue(ctx, topic, items, nil)
	return err
}

func (queueEntity) SubscriberQueue(_ context.Context, _ *mq.Client, _, topic string) (string, error) {
	return topic, nil
}

func (queueEntity) RequiresConsumerID() bool { return false }
