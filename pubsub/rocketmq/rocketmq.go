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

package rocketmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/rlog"

	mq "github.com/apache/rocketmq-client-go/v2"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqp "github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

type daprQueueSelector struct {
	hashQueueSelector       mqp.QueueSelector
	roundRobinQueueSelector mqp.QueueSelector
}

func NewDaprQueueSelector() *daprQueueSelector {
	return &daprQueueSelector{
		hashQueueSelector:       mqp.NewHashQueueSelector(),
		roundRobinQueueSelector: mqp.NewRoundRobinQueueSelector(),
	}
}

func (p *daprQueueSelector) Select(msg *primitive.Message, queues []*primitive.MessageQueue) *primitive.MessageQueue {
	if msg.Queue != nil {
		return msg.Queue
	}
	if queue := msg.GetProperty(metadataRocketmqQueue); queue != "" {
		for _, q := range queues {
			if strconv.Itoa(q.QueueId) == queue {
				return q
			}
		}
	}
	key := msg.GetShardingKey()
	if len(key) == 0 {
		return p.roundRobinQueueSelector.Select(msg, queues)
	}
	return p.hashQueueSelector.Select(msg, queues)
}

type rocketMQ struct {
	name          string
	metadata      *rocketMQMetaData
	producer      mq.Producer
	producerLock  sync.RWMutex
	consumer      mq.PushConsumer
	consumerLock  sync.RWMutex
	topics        map[string]mqc.MessageSelector
	msgProperties map[string]bool
	logger        logger.Logger
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewRocketMQ(l logger.Logger) pubsub.PubSub {
	return &rocketMQ{
		name:         "rocketmq",
		logger:       l,
		producerLock: sync.RWMutex{},
		consumerLock: sync.RWMutex{},
	}
}

func (r *rocketMQ) Init(metadata pubsub.Metadata) error {
	var err error
	r.metadata, err = parseRocketMQMetaData(metadata)
	if err != nil {
		return err
	}
	r.topics = make(map[string]mqc.MessageSelector)
	r.msgProperties = make(map[string]bool)
	rlog.SetLogLevel(r.metadata.LogLevel)
	if r.metadata.MsgProperties != "" {
		mps := strings.Split(r.metadata.MsgProperties, ",")
		for _, mp := range mps {
			r.msgProperties[mp] = true
		}
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return nil
}

func (r *rocketMQ) setUpConsumer() (mq.PushConsumer, error) {
	opts := make([]mqc.Option, 0)
	if r.metadata.InstanceName != "" {
		opts = append(opts, mqc.WithInstance(r.metadata.InstanceName))
	}
	if r.metadata.ConsumerGroupName != "" {
		opts = append(opts, mqc.WithGroupName(r.metadata.ConsumerGroupName))
	}
	if r.metadata.NameServer != "" {
		opts = append(opts, mqc.WithNameServer(strings.Split(r.metadata.NameServer, ",")))
	}
	if r.metadata.NameSpace != "" {
		opts = append(opts, mqc.WithNamespace(r.metadata.NameSpace))
	}
	if r.metadata.NameServerDomain != "" {
		opts = append(opts, mqc.WithNameServerDomain(r.metadata.NameServerDomain))
	}
	if r.metadata.AccessKey != "" && r.metadata.SecretKey != "" {
		opts = append(opts, mqc.WithCredentials(primitive.Credentials{
			AccessKey:     r.metadata.AccessKey,
			SecretKey:     r.metadata.SecretKey,
			SecurityToken: r.metadata.SecurityToken,
		}))
	}
	if r.metadata.Retries > 0 {
		opts = append(opts, mqc.WithRetry(r.metadata.Retries))
	}
	if r.metadata.ConsumerModel != "" {
		if strings.EqualFold(r.metadata.ConsumerModel, "BroadCasting") {
			opts = append(opts, mqc.WithConsumerModel(mqc.BroadCasting))
		} else if strings.EqualFold(r.metadata.ConsumerModel, "Clustering") {
			opts = append(opts, mqc.WithConsumerModel(mqc.Clustering))
		} else {
			r.metadata.ConsumerModel = "Clustering"
			opts = append(opts, mqc.WithConsumerModel(mqc.Clustering))
			r.logger.Warnf("%s Consumer Model[%s] is error, expected [BroadCasting, Clustering], "+
				"we will use default model [Clustering]", r.name, r.metadata.ConsumerModel)
		}
	}
	if r.metadata.FromWhere != "" {
		if strings.EqualFold(r.metadata.FromWhere, "ConsumeFromLastOffset") {
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromLastOffset))
		} else if strings.EqualFold(r.metadata.FromWhere, "ConsumeFromFirstOffset") {
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromFirstOffset))
		} else if strings.EqualFold(r.metadata.FromWhere, "ConsumeFromTimestamp") {
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromTimestamp))
		} else {
			r.metadata.FromWhere = "ConsumeFromLastOffset"
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromLastOffset))
			r.logger.Warnf("%s Consumer FromWhere[%s] is error, "+
				"expected [ConsumeFromLastOffset, ConsumeFromFirstOffset, ConsumeFromTimestamp], "+
				"we will use default value [ConsumeFromLastOffset]", r.name, r.metadata.FromWhere)
		}
	}
	if r.metadata.ConsumeOrderly != "" {
		if strings.EqualFold(r.metadata.ConsumeOrderly, "false") {
			opts = append(opts, mqc.WithConsumerOrder(false))
		} else if strings.EqualFold(r.metadata.ConsumeOrderly, "true") {
			opts = append(opts, mqc.WithConsumerOrder(true))
			// in orderly message mode, if no value is set of MessageBatchMaxSize, the recommended value [1] is used
			if r.metadata.ConsumeMessageBatchMaxSize <= 0 {
				r.metadata.ConsumeMessageBatchMaxSize = 1
			}
		} else {
			r.metadata.ConsumeOrderly = "false"
			opts = append(opts, mqc.WithConsumerOrder(false))
			r.logger.Warnf("%s Consumer Orderly[%s] is error, "+
				"expected [true, false], we will use default value [false]", r.name, r.metadata.FromWhere)
		}
	}
	if r.metadata.ConsumeMessageBatchMaxSize > 0 {
		opts = append(opts, mqc.WithConsumeMessageBatchMaxSize(r.metadata.ConsumeMessageBatchMaxSize))
	}
	if r.metadata.MaxReconsumeTimes > 0 {
		opts = append(opts, mqc.WithMaxReconsumeTimes(r.metadata.MaxReconsumeTimes))
	}
	if r.metadata.AutoCommit != "" {
		if strings.EqualFold(r.metadata.AutoCommit, "false") {
			opts = append(opts, mqc.WithAutoCommit(false))
		} else if strings.EqualFold(r.metadata.AutoCommit, "true") {
			opts = append(opts, mqc.WithAutoCommit(true))
		} else {
			r.metadata.AutoCommit = "true"
			opts = append(opts, mqc.WithAutoCommit(true))
			r.logger.Warnf("%s Consumer AutoCommit[%s] is error, "+
				"expected [true, false], we will use default value [true]", r.name, r.metadata.FromWhere)
		}
	}
	if r.metadata.PullInterval > 0 {
		opts = append(opts, mqc.WithPullInterval(time.Duration(r.metadata.PullInterval)*time.Millisecond))
	}
	if r.metadata.PullBatchSize > 0 {
		opts = append(opts, mqc.WithPullBatchSize(r.metadata.PullBatchSize))
	}
	c, e := mqc.NewPushConsumer(opts...)
	if e != nil {
		return nil, e
	}
	return c, e
}

func (r *rocketMQ) setUpProducer() (mq.Producer, error) {
	opts := make([]mqp.Option, 0)
	if r.metadata.InstanceName != "" {
		opts = append(opts, mqp.WithInstanceName(r.metadata.InstanceName))
	}
	if r.metadata.ProducerGroupName != "" {
		opts = append(opts, mqp.WithGroupName(r.metadata.ProducerGroupName))
	}
	if r.metadata.NameServer != "" {
		opts = append(opts, mqp.WithNameServer(strings.Split(r.metadata.NameServer, ",")))
	}
	if r.metadata.NameSpace != "" {
		opts = append(opts, mqp.WithNamespace(r.metadata.NameSpace))
	}
	if r.metadata.NameServerDomain != "" {
		opts = append(opts, mqp.WithNameServerDomain(r.metadata.NameServerDomain))
	}
	if r.metadata.AccessKey != "" && r.metadata.SecretKey != "" {
		opts = append(opts, mqp.WithCredentials(primitive.Credentials{
			AccessKey:     r.metadata.AccessKey,
			SecretKey:     r.metadata.SecretKey,
			SecurityToken: r.metadata.SecurityToken,
		}))
	}
	if r.metadata.Retries > 0 {
		opts = append(opts, mqp.WithRetry(r.metadata.Retries))
	}
	if r.metadata.SendMsgTimeout > 0 {
		opts = append(opts, mqp.WithSendMsgTimeout(time.Duration(r.metadata.SendMsgTimeout)*time.Second))
	}
	if r.metadata.ProducerQueueSelector == HashQueueSelector {
		opts = append(opts, mqp.WithQueueSelector(mqp.NewHashQueueSelector()))
	} else if r.metadata.ProducerQueueSelector == RandomQueueSelector {
		opts = append(opts, mqp.WithQueueSelector(mqp.NewRandomQueueSelector()))
	} else if r.metadata.ProducerQueueSelector == RoundRobinQueueSelector {
		opts = append(opts, mqp.WithQueueSelector(mqp.NewRandomQueueSelector()))
	} else if r.metadata.ProducerQueueSelector == ManualQueueSelector {
		opts = append(opts, mqp.WithQueueSelector(mqp.NewManualQueueSelector()))
	} else {
		opts = append(opts, mqp.WithQueueSelector(NewDaprQueueSelector()))
	}

	producer, err := mq.NewProducer(opts...)
	if err != nil {
		return nil, err
	}
	err = producer.Start()
	if err != nil {
		_ = producer.Shutdown()
		return nil, err
	}
	return producer, nil
}

func (r *rocketMQ) Features() []pubsub.Feature {
	return nil
}

func (r *rocketMQ) getProducer() (mq.Producer, error) {
	if r.producer != nil {
		return r.producer, nil
	}
	r.producerLock.Lock()
	defer r.producerLock.Unlock()
	if r.producer != nil {
		return r.producer, nil
	}
	producer, e := r.setUpProducer()
	if e != nil {
		return nil, e
	}
	r.producer = producer
	return r.producer, nil
}

func (r *rocketMQ) Publish(req *pubsub.PublishRequest) error {
	r.logger.Debugf("rocketmq publish topic:%s with data:%v", req.Topic, req.Data)
	msg := primitive.NewMessage(req.Topic, req.Data)
	for k, v := range req.Metadata {
		if strings.EqualFold(k, metadataRocketmqTag) {
			msg.WithTag(v)
		} else if strings.EqualFold(k, metadataRocketmqKey) {
			msg.WithKeys(strings.Split(v, ","))
		} else if strings.EqualFold(k, metadataRocketmqShardingKey) {
			msg.WithShardingKey(v)
		} else {
			msg.WithProperty(k, v)
		}
	}
	publishBo := backoff.NewExponentialBackOff()
	publishBo.InitialInterval = 100 * time.Millisecond
	bo := backoff.WithMaxRetries(publishBo, 3)
	bo = backoff.WithContext(bo, r.ctx)
	return retry.NotifyRecover(
		func() error {
			producer, e := r.getProducer()
			if e != nil {
				return fmt.Errorf("rocketmq message send fail because producer failed to initialize: %v", e)
			}
			ctx, cancel := context.WithTimeout(r.ctx, 3*time.Duration(r.metadata.SendMsgTimeout)*time.Second)
			defer cancel()
			result, e := producer.SendSync(ctx, msg)
			if e != nil {
				r.producerLock.Lock()
				r.producer = nil
				r.producerLock.Unlock()
				r.logger.Errorf("rocketmq message send fail, topic[%s]: %v", req.Topic, e)
				return fmt.Errorf("rocketmq message send fail, topic[%s]: %v", req.Topic, e)
			}
			r.logger.Debugf("rocketmq message send result: topic[%s], tag[%s], status[%v]", req.Topic, msg.GetTags(), result.Status)
			return nil
		},
		bo,
		func(err error, d time.Duration) {
			r.logger.Errorf("rocketmq message send fail, topic[%s], error[%v], Retrying...", err, msg.Topic)
		},
		func() {
			r.logger.Infof("rocketmq message sent successfully after it previously failed. topic:%s.", msg.Topic)
		},
	)
}

func (r *rocketMQ) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	selector, e := buildMessageSelector(req)
	if e != nil {
		r.logger.Warnf("rocketmq subscribe failed: ", e.Error())
		return e
	}

	var cb func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error)
	if r.metadata.ConsumeOrderly == "true" {
		cb = r.consumeMessageOrderly(req.Topic, selector, handler)
	} else {
		cb = r.consumeMessageCurrently(req.Topic, selector, handler)
	}

	r.consumerLock.Lock()
	defer r.consumerLock.Unlock()

	r.topics[req.Topic] = *selector

	if r.consumer == nil {
		// if consumer is not initialized, initialize it
		if r.consumer, e = r.setUpConsumer(); e != nil {
			return fmt.Errorf("consumer setup failed: %v", e)
		}
		// consumer will start after one second.
		// Consumers who complete the subscription within 1 second, will begin the subscription immediately upon launch.
		// Consumers who do not complete the subscription within 1 second, will start the subscription after 20 seconds.
		// The 20-second time is the interval for RocketMQ to refresh the topic route.
		go func() {
			time.Sleep(time.Second)
			if e = r.consumer.Start(); e == nil {
				r.logger.Infof("consumer start success: Group[%s], Topics[%v]", r.metadata.ConsumerGroupName, r.topics)
			} else {
				r.logger.Errorf("consumer start failed: %v", e)
			}
		}()
	}

	// subscribe topic
	if e = r.consumer.Subscribe(req.Topic, *selector, cb); e != nil {
		r.logger.Errorf("subscribe topic[%s] Group[%s] failed, error: %v", req.Topic, r.metadata.ConsumerGroupName, e)
		return e
	} else {
		r.logger.Infof("subscribe topic[%s] success, Group[%s], Topics[%v]", req.Topic, r.metadata.ConsumerGroupName, r.topics)
	}

	return nil
}

func buildMessageSelector(req pubsub.SubscribeRequest) (*mqc.MessageSelector, error) {
	if req.Metadata == nil {
		req.Metadata = make(map[string]string)
	}
	mqExpr := req.Metadata[metadataRocketmqExpression]
	mqType := req.Metadata[metadataRocketmqType]

	var ExpressionType mqc.ExpressionType
	if mqType == "" || strings.EqualFold(mqType, string(mqc.TAG)) {
		ExpressionType = mqc.TAG
	} else if strings.EqualFold(mqType, string(mqc.SQL92)) {
		ExpressionType = mqc.SQL92
	} else {
		return nil, fmt.Errorf("rocketmq msg type invalid: %s, expected value is 'tag' or 'sql92' or ''", mqType)
	}

	return &mqc.MessageSelector{
		Type:       ExpressionType,
		Expression: mqExpr,
	}, nil
}

func (r *rocketMQ) buildPubsubMessage(topic, mqType, mqExpr string, msg *primitive.MessageExt) (*pubsub.NewMessage, error) {
	cloudEventsMap := pubsub.NewCloudEventsEnvelope(msg.MsgId, msg.StoreHost, "", "", msg.Topic, r.name, r.metadata.ContentType, msg.Body, "", "")
	cloudEventsMap[primitive.PropertyKeys] = msg.GetKeys()
	cloudEventsMap[primitive.PropertyShardingKey] = msg.GetShardingKey()
	cloudEventsMap[primitive.PropertyTags] = msg.GetTags()
	cloudEventsMap[primitive.PropertyMsgRegion] = msg.GetRegionID()
	for k, v := range msg.GetProperties() {
		if _, ok := r.msgProperties[k]; ok {
			cloudEventsMap[k] = v
		}
		if strings.EqualFold(k, pubsub.TraceIDField) {
			cloudEventsMap[pubsub.TraceIDField] = v
		}
	}
	dataBytes, err := json.Marshal(cloudEventsMap)
	if err != nil {
		return nil, err
	}
	metadata := map[string]string{
		metadataRocketmqType:          mqType,
		metadataRocketmqExpression:    mqExpr,
		metadataRocketmqConsumerGroup: r.metadata.ProducerGroupName,
	}
	if msg.Queue != nil {
		metadata[metadataRocketmqBrokerName] = msg.Queue.BrokerName
		metadata[metadataRocketmqQueueID] = strconv.Itoa(msg.Queue.QueueId)
	}
	return &pubsub.NewMessage{
		Topic:    topic,
		Data:     dataBytes,
		Metadata: metadata,
	}, nil
}

func (r *rocketMQ) consumeMessageOrderly(topic string, selector *mqc.MessageSelector, handler pubsub.Handler) func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
		for _, msg := range msgs {
			newMessage, e := r.buildPubsubMessage(topic, string(selector.Type), selector.Expression, msg)
			if e != nil {
				r.logger.Errorf("rocketmq message consume fail, topic: %s, msgId: %s, error: %v", newMessage.Topic, msg.MsgId, e)
				return mqc.SuspendCurrentQueueAMoment, nil
			}
			e = handler(ctx, newMessage)
			if e != nil {
				r.logger.Errorf("rocketmq message consume fail, topic: %s, msgId: %s, error: %v", newMessage.Topic, msg.MsgId, e)
				return mqc.SuspendCurrentQueueAMoment, nil
			}
		}
		return mqc.ConsumeSuccess, nil
	}
}

func (r *rocketMQ) consumeMessageCurrently(topic string, selector *mqc.MessageSelector, handler pubsub.Handler) func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
		for _, msg := range msgs {
			newMessage, e := r.buildPubsubMessage(topic, string(selector.Type), selector.Expression, msg)
			if e != nil {
				r.logger.Errorf("rocketmq message consume fail, topic: %s, msgId: %s, error: %v", newMessage.Topic, msg.MsgId, e)
				return mqc.ConsumeRetryLater, nil
			}
			e = handler(ctx, newMessage)
			if e != nil {
				r.logger.Errorf("rocketmq message consume fail, topic: %s, msgId: %s, error: %v", newMessage.Topic, msg.MsgId, e)
				return mqc.ConsumeRetryLater, nil
			}
		}
		return mqc.ConsumeSuccess, nil
	}
}

func (r *rocketMQ) Close() error {
	r.producerLock.Lock()
	defer r.producerLock.Unlock()
	r.consumerLock.Lock()
	defer r.consumerLock.Unlock()

	r.cancel()

	r.producer = nil

	if r.consumer != nil {
		_ = r.consumer.Shutdown()
		r.consumer = nil
	}

	return nil
}
