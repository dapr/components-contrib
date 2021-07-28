package rocketmq

import (
	rmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type rocketMQ struct {
	metadata rocketMQMetaData
	logger   logger.Logger
	producer rmq.Producer
}

func NewRocketMQ(l logger.Logger) pubsub.PubSub {
	return &rocketMQ{
		logger: l,
	}
}

func (r *rocketMQ) Init(metadata pubsub.Metadata) error {
	panic("implement me")
}

func (r *rocketMQ) Features() []pubsub.Feature {
	panic("implement me")
}

func (r *rocketMQ) Publish(req *pubsub.PublishRequest) error {
	panic("implement me")
}

func (r *rocketMQ) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	panic("implement me")
}

func (r rocketMQ) Close() error {
	panic("implement me")
}
