package rocketmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mq "github.com/apache/rocketmq-client-go/v2"
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
	producer     mq.Producer
	txProducer   mq.TransactionProducer
	pushConsumer mq.PushConsumer
	pullConsumer mq.PullConsumer

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
	var err error
	r.metadata, err = parseRocketMQMetaData(metadata)
	if err != nil {
		return err
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	// Default retry configuration is used if no
	// backOff properties are set.
	if err = retry.DecodeConfigWithPrefix(
		&r.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return fmt.Errorf("retry configuration error: %w", err)
	}
	r.producer, err = r.setUpProducer()
	if err != nil {
		return err
	}
	r.pushConsumer, err = r.setUpConsumer()
	if err != nil {
		return err
	}
	err = r.producer.Start()
	err = r.pushConsumer.Start()
	return err
}

func (r *rocketMQ) setUpConsumer() (mq.PushConsumer, error) {
	opts := make([]mqc.Option, 0)

	return mq.NewPushConsumer(opts...)

}

func (r *rocketMQ) setUpProducer() (mq.Producer, error) {
	opts := make([]mqp.Option, 0)
	if r.metadata.Retries != 0 {
		opts = append(opts, mqp.WithRetry(r.metadata.Retries))
	}
	if r.metadata.AccessKey != "" && r.metadata.SecretKey != "" {
		opts = append(opts, mqp.WithCredentials(primitive.Credentials{
			AccessKey: r.metadata.AccessKey,
			SecretKey: r.metadata.SecretKey,
		}))
	}
	return mq.NewProducer(opts...)
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
	return nil
}

type mqCallback func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error)

func (r *rocketMQ) adaptCallback(topic, consumerGroup, mqType, mqExpr string, handler pubsub.Handler) mqCallback {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
		success := true
		for _, v := range msgs {
			data := pubsub.NewCloudEventsEnvelope(v.MsgId, v.StoreHost, r.name,
				v.GetProperty(primitive.PropertyKeys), v.Topic, r.name, r.metadata.ContentType, v.Body, "")
			dataBytes, err := json.Marshal(data)
			if err != nil {
				r.logger.Warn("rocketmq fail to marshal data message, topic:%s data-length:%d err:%v ", v.Topic, len(v.Body), err)
				success = false

				continue
			}
			metadata := map[string]string{
				metadataRocketmqType:          mqType,
				metadataRocketmqExpression:    mqExpr,
				metadataRocketmqConsumerGroup: consumerGroup,
			}
			if v.Queue != nil {
				metadata[metadataRocketmqBrokerName] = v.Queue.BrokerName
			}
			msg := pubsub.NewMessage{
				Topic:    topic,
				Data:     dataBytes,
				Metadata: metadata,
			}

			b := r.backOffConfig.NewBackOffWithContext(r.ctx)

			rerr := retry.NotifyRecover(func() error {
				herr := handler(ctx, &msg)
				if herr != nil {
					r.logger.Errorf("rocketmq error: fail to send message to dapr application. topic:%s data-length:%d err:%v ", v.Topic, len(v.Body), herr)
					success = false
				}

				return herr
			}, b, func(err error, d time.Duration) {
				r.logger.Errorf("rocketmq error: fail to processing message. topic:%s data-length:%d. Retrying...", v.Topic, len(v.Body))
			}, func() {
				r.logger.Infof("rocketmq successfully processed message after it previously failed. topic:%s data-length:%d.", v.Topic, len(v.Body))
			})
			if rerr != nil && !errors.Is(rerr, context.Canceled) {
				r.logger.Errorf("rocketmq error: processing message and retries are exhausted. topic:%s data-length:%d.", v.Topic, len(v.Body))
			}
		}

		if !success {
			return mqc.ConsumeRetryLater, nil
		}

		return mqc.ConsumeSuccess, nil
	}
}

func (r *rocketMQ) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if req.Metadata == nil {
		req.Metadata = make(map[string]string, 1)
	}

	consumerGroup := req.Metadata[metadataRocketmqConsumerGroup]
	if len(consumerGroup) == 0 {
		consumerGroup = r.metadata.ConsumerGroup
	}

	mqType := req.Metadata[metadataRocketmqType]
	mqExpr := req.Metadata[metadataRocketmqExpression]
	if len(mqType) != 0 &&
		(mqType != string(mqc.SQL92) &&
			mqType != string(mqc.TAG)) {
		r.logger.Warnf("rocketmq subscribe to topic %s failed because some illegal type(%s).", req.Topic, req.Metadata[metadataRocketmqType])

		return nil
	}

	r.closeSubscripionResources()

	var err error
	if r.pushConsumer, err = r.setUpConsumer(); err != nil {
		return err
	}

	topics := r.addTopic(req.Topic, mqc.MessageSelector{Type: mqc.ExpressionType(mqType), Expression: mqExpr})

	err = r.subscribeAllTopics(topics, consumerGroup, handler)
	if err != nil {
		return fmt.Errorf("rocketmq error: %w", err)
	}

	err = r.pushConsumer.Start()
	if err != nil {
		return fmt.Errorf("consumer start failed. %w", err)
	}

	return nil
}

func (r *rocketMQ) closeSubscripionResources() {
	if r.pushConsumer != nil {
		if len(r.topics) > 0 {
			_ = r.pushConsumer.Shutdown()
		}
	}
}

func (r *rocketMQ) subscribeAllTopics(topics []string, consumerGroup string, handler pubsub.Handler) error {
	for _, topic := range topics {
		selector, ok := r.topics[topic]
		if !ok {
			return errors.New("no selector for topic:" + topic)
		}
		err := r.pushConsumer.Subscribe(topic, selector, r.adaptCallback(topic, consumerGroup, string(selector.Type), selector.Expression, handler))
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *rocketMQ) addTopic(topic string, selector mqc.MessageSelector) []string {
	r.topics[topic] = selector
	topics := make([]string, 0)
	for topic, _ := range r.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (r rocketMQ) Close() error {
	r.cancel()

	if r.pushConsumer != nil {
		_ = r.pushConsumer.Shutdown()
	}

	if r.producer != nil {
		_ = r.producer.Shutdown()
	}

	return nil
}
