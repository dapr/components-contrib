package rocketmq

import (
	"context"
	"fmt"
	rmq "github.com/apache/rocketmq-client-go/v2"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqp "github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
	jsoniter "github.com/json-iterator/go"
)

type rocketMQ struct {
	name         string
	metadata     rocketMQMetaData
	producer     rmq.Producer
	txProducer   rmq.TransactionProducer
	pushConsumer rmq.PushConsumer
	pullConsumer rmq.PullConsumer
	logger       logger.Logger
	json         jsoniter.API
	topics       map[string]mqc.MessageSelector

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

func NewRocketMQ(l logger.Logger) pubsub.PubSub {
	return &rocketMQ{
		logger: l,
	}
}

func (r *rocketMQ) setupPublisher() (rmq.Producer, error) {
	opts := make([]mqp.Option, 0)
	if len(r.metadata.Resolvers) != 0 {
		opts = append(opts, mqp.WithNsResolver(primitive.NewPassthroughResolver(r.metadata.Resolvers)))
	}
	if r.metadata.RetryTimes != 0 {
		opts = append(opts, mqp.WithRetry(r.metadata.RetryTimes))
	}
	if r.metadata.AccessKey != "" && r.metadata.SecretKey != "" {
		opts = append(opts, mqp.WithCredentials(primitive.Credentials{
			AccessKey: r.metadata.AccessKey,
			SecretKey: r.metadata.SecretKey,
		}))
	}
	p, err := rmq.NewProducer(opts...)
	if err != nil {
		fmt.Println("init producer error: " + err.Error())
		return nil, err
	}
	err = p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		return nil, err
	}
	return p, nil
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
