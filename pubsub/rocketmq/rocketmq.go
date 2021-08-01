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
	"time"
)

type rocketMQ struct {
	name         string
	metadata     *rocketMQMetaData
	producer     rmq.Producer
	txProducer   rmq.TransactionProducer
	pushConsumer rmq.PushConsumer
	pullConsumer rmq.PullConsumer

	logger logger.Logger
	topics map[string]mqc.MessageSelector

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

func NewRocketMQ(l logger.Logger) pubsub.PubSub {
	return &rocketMQ{
		logger: l,
	}
}

func (r *rocketMQ) Init(metadata pubsub.Metadata) error {
	r.metadata = parseRocketMQMetaData(metadata)
	producer,err := r.setUpProducer()
	if err != nil {

	}
	r.producer = producer
	consumer,err := r.setUpConsumer()
	if err != nil {

	}
	r.pushConsumer = consumer

	err = r.producer.Start()
	err = r.pushConsumer.Start()
	return err
}

func (r *rocketMQ)setUpConsumer()(rmq.PushConsumer,error){
	opts := make([]mqc.Option,0)

	return rmq.NewPushConsumer(opts...)

}

func (r *rocketMQ) setUpProducer() (rmq.Producer, error) {
	opts := make([]mqp.Option, 0)
	//if len(r.metadata.Resolvers) != 0 {
	//	opts = append(opts, mqp.WithNsResolver(primitive.NewPassthroughResolver(r.metadata.Resolvers)))
	//}
	if r.metadata.RetryTimes != 0 {
		opts = append(opts, mqp.WithRetry(r.metadata.RetryTimes))
	}
	if r.metadata.AccessKey != "" && r.metadata.SecretKey != "" {
		opts = append(opts, mqp.WithCredentials(primitive.Credentials{
			AccessKey: r.metadata.AccessKey,
			SecretKey: r.metadata.SecretKey,
		}))
	}
	return rmq.NewProducer(opts...)
}

func (r *rocketMQ) Features() []pubsub.Feature {
	panic("implement me")
}

func (r *rocketMQ) Publish(req *pubsub.PublishRequest) error {
	msg := primitive.NewMessage(req.Topic, req.Data).WithTag(req.Metadata[metadataRocketmqTag]).
		WithKeys([]string{req.Metadata[metadataRocketmqKey]})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := r.producer.SendSync(ctx, msg)

	if result != nil {
		r.logger.Debugf("rocketmq send result topic:%s tag:%s status:%v", req.Topic, msg.GetTags(), result.Status)
	}

	if err != nil {
		r.logger.Errorf("error send message topic:%s : %v", req.Topic, err)

		return fmt.Errorf("publish message failed. %w", err)
	}
}

func (r *rocketMQ) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	panic("implement me")
}

func (r rocketMQ) Close() error {
	r.cancel()
	err := r.producer.Shutdown()
	err = r.pushConsumer.Shutdown()
	return err
}
