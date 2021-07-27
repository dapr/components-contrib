package rocketmq

import (
	"github.com/dapr/components-contrib/pubsub"
)

type rocketMQ struct {
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
