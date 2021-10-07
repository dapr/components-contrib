// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	mq "github.com/apache/rocketmq-client-go/v2"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqp "github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
	"github.com/patrickmn/go-cache"
)

type rocketMQ struct {
	name         string
	metadata     *rocketMQMetaData
	pushConsumer mq.PushConsumer

	logger logger.Logger
	lock   sync.Mutex
	topics map[string]mqc.MessageSelector

	producerPool  *cache.Cache
	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

func NewRocketMQ(l logger.Logger) pubsub.PubSub {
	return &rocketMQ{
		name:         "rocketmq",
		logger:       l,
		topics:       make(map[string]mqc.MessageSelector),
		producerPool: cache.New(5*time.Minute, 10*time.Minute),
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
	r.producerPool.OnEvicted(func(s string, i interface{}) {
		producer := i.(mq.Producer)
		if producer != nil {
			_ = producer.Shutdown()
		}
	})
	return nil
}

func (r *rocketMQ) setUpConsumer() (mq.PushConsumer, error) {
	opts := make([]mqc.Option, 0)
	if r.metadata.ConsumerGroup != "" {
		opts = append(opts, mqc.WithGroupName(r.metadata.ConsumerGroup))
	}
	if r.metadata.NameSpace != "" {
		opts = append(opts, mqc.WithNamespace(r.metadata.NameSpace))
	}
	if r.metadata.Retries != 0 {
		opts = append(opts, mqc.WithRetry(r.metadata.Retries))
	}
	if r.metadata.NameServerDomain != "" {
		opts = append(opts, mqc.WithNameServerDomain(r.metadata.NameServerDomain))
	}
	if r.metadata.NameServer != "" {
		opts = append(opts, mqc.WithNameServer(primitive.NamesrvAddr{r.metadata.NameServer}))
	}
	if r.metadata.AccessKey != "" && r.metadata.SecretKey != "" {
		opts = append(opts, mqc.WithCredentials(primitive.Credentials{
			AccessKey: r.metadata.AccessKey,
			SecretKey: r.metadata.SecretKey,
		}))
	}
	return mq.NewPushConsumer(opts...)
}

func (r *rocketMQ) setUpProducer() (mq.Producer, error) {
	opts := make([]mqp.Option, 0)
	if r.metadata.Retries != 0 {
		opts = append(opts, mqp.WithRetry(r.metadata.Retries))
	}
	if r.metadata.GroupName != "" {
		opts = append(opts, mqp.WithGroupName(r.metadata.GroupName))
	}
	if r.metadata.NameServerDomain != "" {
		opts = append(opts, mqp.WithNameServerDomain(r.metadata.NameServerDomain))
	}
	if r.metadata.NameSpace != "" {
		opts = append(opts, mqp.WithNamespace(r.metadata.NameSpace))
	}
	if r.metadata.NameServer != "" {
		opts = append(opts, mqp.WithNameServer(primitive.NamesrvAddr{r.metadata.NameServer}))
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
	return nil
}

func (r *rocketMQ) Publish(req *pubsub.PublishRequest) error {
	r.logger.Debugf("rocketmq publish topic:%s with data:%v", req.Topic, req.Data)
	msg := newRocketMQMessage(req)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.metadata.SendTimeOut))
	defer cancel()
	producer, err := r.getOrCreateProducer(req.Topic)
	if err != nil {
		return err
	}
	result, err := producer.SendSync(ctx, msg)
	if err != nil {
		r.logger.Errorf("error send message topic:%s : %v", req.Topic, err)
		return ErrRocketmqPublishMsg
	}
	r.logger.Debugf("rocketmq send result topic:%s tag:%s status:%v", req.Topic, msg.GetTags(), result.Status)
	return nil
}

func (r *rocketMQ) getOrCreateProducer(topic string) (producer mq.Producer, err error) {
	if p, ok := r.producerPool.Get(topic); ok {
		producer = p.(mq.Producer)
		return
	}
	r.logger.Debugf("create producer for topic:%v", topic)
	producer, err = r.setUpProducer()
	if err != nil {
		return
	}
	err = r.producerPool.Add(topic, producer, 5*time.Minute)
	return
}

func newRocketMQMessage(req *pubsub.PublishRequest) *primitive.Message {
	return primitive.NewMessage(req.Topic, req.Data).
		WithTag(req.Metadata[metadataRocketmqTag]).
		WithKeys([]string{req.Metadata[metadataRocketmqKey]}).
		WithShardingKey(req.Metadata[metadataRocketmqShardingKey])
}

type mqSubscribeCallback func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error)

func (r *rocketMQ) adaptCallback(topic, consumerGroup, mqType, mqExpr string, handler pubsub.Handler) mqSubscribeCallback {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
		success := true
		for _, msg := range msgs {
			cloudEventsMap := pubsub.NewCloudEventsEnvelope(msg.MsgId, msg.StoreHost, r.name, msg.GetProperty(primitive.PropertyKeys), msg.Topic, r.name, r.metadata.ContentType, msg.Body, "")
			dataBytes, err := json.Marshal(cloudEventsMap)
			if err != nil {
				r.logger.Warn("rocketmq fail to marshal cloudEventsMap message, topic:%s cloudEventsMap-length:%d err:%newMessage ", msg.Topic, len(msg.Body), err)
				success = false
				continue
			}
			metadata := map[string]string{
				metadataRocketmqType:          mqType,
				metadataRocketmqExpression:    mqExpr,
				metadataRocketmqConsumerGroup: consumerGroup,
			}
			if msg.Queue != nil {
				metadata[metadataRocketmqBrokerName] = msg.Queue.BrokerName
			}
			newMessage := pubsub.NewMessage{
				Topic:    topic,
				Data:     dataBytes,
				Metadata: metadata,
			}
			b := r.backOffConfig.NewBackOffWithContext(r.ctx)
			retError := retry.NotifyRecover(func() error {
				herr := handler(ctx, &newMessage)
				if herr != nil {
					r.logger.Errorf("rocketmq error: fail to send message to dapr application. topic:%s cloudEventsMap-length:%d err:%newMessage ", newMessage.Topic, len(msg.Body), herr)
					success = false
				}
				return herr
			}, b, func(err error, d time.Duration) {
				r.logger.Errorf("rocketmq error: fail to processing message. topic:%s cloudEventsMap-length:%d. Retrying...", newMessage.Topic, len(msg.Body))
			}, func() {
				r.logger.Infof("rocketmq successfully processed message after it previously failed. topic:%s cloudEventsMap-length:%d.", newMessage.Topic, len(msg.Body))
			})
			if retError != nil && !errors.Is(retError, context.Canceled) {
				r.logger.Errorf("rocketmq error: processing message and retries are exhausted. topic:%s cloudEventsMap-length:%d.", newMessage.Topic, len(msg.Body))
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
		req.Metadata = make(map[string]string)
	}
	consumerGroup := r.metadata.ConsumerGroup
	// get consumer group from request first
	if group, ok := req.Metadata[metadataRocketmqConsumerGroup]; ok {
		consumerGroup = group
	}
	var (
		mqExpr = req.Metadata[metadataRocketmqExpression]
		mqType = req.Metadata[metadataRocketmqType]
		err    error
	)
	if !r.validMqTypeParams(mqType) {
		return ErrRocketmqValidPublishMsgTyp
	}
	r.closeSubscriptionResources()
	if r.pushConsumer, err = r.setUpConsumer(); err != nil {
		return err
	}
	topics := r.addTopic(req.Topic, mqc.MessageSelector{
		Type:       mqc.ExpressionType(mqType),
		Expression: mqExpr,
	})
	r.subscribeAllTopics(topics, consumerGroup, handler)
	err = r.pushConsumer.Start()
	if err != nil {
		return fmt.Errorf("consumer start failed. %w", err)
	}
	return nil
}

func (r *rocketMQ) validMqTypeParams(mqType string) bool {
	if len(mqType) != 0 && (mqType != string(mqc.SQL92) && mqType != string(mqc.TAG)) {
		r.logger.Warnf("rocketmq subscribe failed because some illegal type(%s).", mqType)
		return false
	}
	return true
}

// Close down consumer group resources, refresh once
func (r *rocketMQ) closeSubscriptionResources() {
	if r.pushConsumer != nil {
		if len(r.topics) > 0 {
			_ = r.pushConsumer.Shutdown()
		}
	}
}

func (r *rocketMQ) subscribeAllTopics(topics []string, consumerGroup string, handler pubsub.Handler) {
	for _, topic := range topics {
		selector, ok := r.topics[topic]
		if !ok {
			r.logger.Errorf("no selector for topic:" + topic)
			continue
		}
		err := r.pushConsumer.Subscribe(topic, selector, r.adaptCallback(topic, consumerGroup, string(selector.Type), selector.Expression, handler))
		if err != nil {
			r.logger.Errorf("subscribe topic:%v failed,error:%v", topic, err)
			continue
		}
	}
}

func (r *rocketMQ) addTopic(topic string, selector mqc.MessageSelector) []string {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.topics[topic] = selector
	return r.getAllTopics()
}

func (r *rocketMQ) getAllTopics() []string {
	topics := make([]string, 0, len(r.topics))
	for topic, _ := range r.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (r *rocketMQ) Close() error {
	r.cancel()
	if r.pushConsumer != nil {
		_ = r.pushConsumer.Shutdown()
	}
	for _, p := range r.producerPool.Items() {
		if p.Object != nil {
			producer := p.Object.(mq.Producer)
			_ = producer.Shutdown()
		}
	}
	r.producerPool.Flush()
	return nil
}
