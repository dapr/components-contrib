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

// Package topics is the DaprMQ topics pub/sub component. A Dapr topic is a DaprMQ topic and each
// consumer ID is a subscriber with its own queue, so every subscribing app gets every message and
// the instances of one app compete for them.
package topics

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	mq "github.com/olitomlinson/dapr-mq/sdks/go"

	"github.com/dapr/components-contrib/common/component/daprmq"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type daprMQTopics struct {
	*daprmq.PubSub
}

// NewDaprMQTopics returns a new DaprMQ topics pub/sub component.
func NewDaprMQTopics(logger logger.Logger) pubsub.PubSub {
	return &daprMQTopics{PubSub: daprmq.New(logger, topicEntity{})}
}

func (d *daprMQTopics) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(daprmq.Metadata{}), &metadataInfo, metadata.PubSubType)
	return
}

type topicEntity struct{}

func (topicEntity) Publish(ctx context.Context, client *mq.Client, topic string, items []mq.EnqueueItem) error {
	_, err := client.Publish(ctx, topic, items, nil)
	return err
}

// SubscriberQueue registers the consumer ID on the topic, if it isn't already, and returns its queue.
func (topicEntity) SubscriberQueue(ctx context.Context, client *mq.Client, consumerID, topic string) (string, error) {
	sub, err := client.Subscribe(ctx, topic, consumerID, nil)
	var mqErr *mq.Error
	if errors.As(err, &mqErr) && mqErr.Code == mq.CodeSubscriberExists {
		return mq.TopicSubscriberQueueID(topic, consumerID), nil
	}
	if err != nil {
		return "", fmt.Errorf("daprmq: subscribe %s to topic %s: %w", consumerID, topic, err)
	}
	return sub.QueueID, nil
}

func (topicEntity) RequiresConsumerID() bool { return true }
