// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"github.com/dapr/components-contrib/bindings/pubsubadapter"
	"github.com/dapr/components-contrib/pubsub/kafka"
	"github.com/dapr/kit/logger"
)

func NewKafkaAdapter(log logger.Logger) *pubsubadapter.Adapter {
	return pubsubadapter.New(kafka.NewKafka(log))
}
