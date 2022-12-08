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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mq "github.com/apache/rocketmq-client-go/v2"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqp "github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"

	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
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
	producerLock  sync.Mutex
	consumer      mq.PushConsumer
	consumerLock  sync.Mutex
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
		producerLock: sync.Mutex{},
		consumerLock: sync.Mutex{},
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

func parseNameServer(nameServer string) []string {
	if strings.Contains(nameServer, ",") {
		return strings.Split(nameServer, ",")
	} else if strings.Contains(nameServer, ";") {
		return strings.Split(nameServer, ";")
	} else {
		return []string{nameServer}
	}
}

func (r *rocketMQ) setUpConsumer() (mq.PushConsumer, error) {
	opts := make([]mqc.Option, 0)
	if r.metadata.InstanceName != "" {
		opts = append(opts, mqc.WithInstance(r.metadata.InstanceName))
	}
	if r.metadata.ConsumerGroup != "" {
		opts = append(opts, mqc.WithGroupName(r.metadata.ConsumerGroup))
	} else if r.metadata.GroupName != "" {
		r.metadata.ConsumerGroup = r.metadata.GroupName
		opts = append(opts, mqc.WithGroupName(r.metadata.ConsumerGroup))
		r.logger.Warnf("set the consumer group name, please use the keyword consumerGroup")
	}
	if r.metadata.NameServer != "" {
		opts = append(opts, mqc.WithNameServer(parseNameServer(r.metadata.NameServer)))
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
		switch strings.ToLower(r.metadata.ConsumerModel) {
		case "broadcasting":
			opts = append(opts, mqc.WithConsumerModel(mqc.BroadCasting))
		case "clustering":
			opts = append(opts, mqc.WithConsumerModel(mqc.Clustering))
		default:
			r.metadata.ConsumerModel = "Clustering"
			opts = append(opts, mqc.WithConsumerModel(mqc.Clustering))
			r.logger.Warnf("%s Consumer Model[%s] is invalid: expected [broadcasting, clustering]; "+
				"we will use default model [clustering]", r.name, r.metadata.ConsumerModel)
		}
	}
	if r.metadata.FromWhere != "" {
		switch strings.ToLower(r.metadata.FromWhere) {
		case "consumefromlastoffset":
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromLastOffset))
		case "consumefromfirstoffset":
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromFirstOffset))
		case "consumefromtimestamp":
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromTimestamp))
		default:
			r.metadata.FromWhere = "ConsumeFromLastOffset"
			opts = append(opts, mqc.WithConsumeFromWhere(mqc.ConsumeFromLastOffset))
			r.logger.Warnf("%s Consumer FromWhere[%s] is error, "+
				"expected [ConsumeFromLastOffset, ConsumeFromFirstOffset, ConsumeFromTimestamp], "+
				"we will use default value [ConsumeFromLastOffset]", r.name, r.metadata.FromWhere)
		}
	}
	if r.metadata.ConsumeOrderly != "" {
		if utils.IsTruthy(r.metadata.ConsumeOrderly) {
			opts = append(opts, mqc.WithConsumerOrder(true))
			// in orderly message mode, if no value is set of MessageBatchMaxSize, the recommended value [1] is used
			if r.metadata.ConsumeMessageBatchMaxSize <= 0 {
				r.metadata.ConsumeMessageBatchMaxSize = 1
			}
		} else {
			opts = append(opts, mqc.WithConsumerOrder(false))
		}
	}
	if r.metadata.ConsumeMessageBatchMaxSize > 0 {
		opts = append(opts, mqc.WithConsumeMessageBatchMaxSize(r.metadata.ConsumeMessageBatchMaxSize))
	}
	if r.metadata.MaxReconsumeTimes > 0 {
		opts = append(opts, mqc.WithMaxReconsumeTimes(r.metadata.MaxReconsumeTimes))
	}
	if r.metadata.AutoCommit != "" {
		opts = append(opts, mqc.WithAutoCommit(utils.IsTruthy(r.metadata.AutoCommit)))
	}
	if r.metadata.PullInterval > 0 {
		opts = append(opts, mqc.WithPullInterval(time.Duration(r.metadata.PullInterval)*time.Millisecond))
	}
	if r.metadata.PullBatchSize > 0 {
		opts = append(opts, mqc.WithPullBatchSize(r.metadata.PullBatchSize))
	} else if r.metadata.ConsumerBatchSize > 0 {
		r.metadata.PullBatchSize = int32(r.metadata.ConsumerBatchSize)
		opts = append(opts, mqc.WithPullBatchSize(r.metadata.PullBatchSize))
		r.logger.Warn("set the number of msg pulled from the broker at a time, " +
			"please use pullBatchSize instead of consumerBatchSize")
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
	if r.metadata.ProducerGroup != "" {
		opts = append(opts, mqp.WithGroupName(r.metadata.ProducerGroup))
	} else if r.metadata.GroupName != "" {
		r.metadata.ProducerGroup = r.metadata.GroupName
		opts = append(opts, mqp.WithGroupName(r.metadata.ProducerGroup))
		r.logger.Warnf("set the producer group name, please use the keyword producerGroup")
	}
	if r.metadata.NameServer != "" {
		opts = append(opts, mqp.WithNameServer(parseNameServer(r.metadata.NameServer)))
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
	if r.metadata.SendTimeOutSec > 0 {
		opts = append(opts, mqp.WithSendMsgTimeout(time.Duration(r.metadata.SendTimeOutSec)*time.Second))
	} else if r.metadata.SendTimeOut > 0 {
		r.metadata.SendTimeOutSec = r.metadata.SendTimeOut / int(time.Second.Nanoseconds())
		opts = append(opts, mqp.WithSendMsgTimeout(time.Duration(r.metadata.SendTimeOutSec)*time.Second))
		r.logger.Warn("set the timeout for send msg to rocketmq broker, please use the keyword sendTimeOutSec. " +
			"SendTimeOutSec is in seconds, SendTimeOut is in nanoseconds")
	} else {
		opts = append(opts, mqp.WithSendMsgTimeout(30*time.Second))
		r.logger.Warn("You have not set a timeout for send msg to rocketmq broker, " +
			"The default value of 30 seconds will be used. ")
	}
	switch r.metadata.ProducerQueueSelector {
	case HashQueueSelector:
		opts = append(opts, mqp.WithQueueSelector(mqp.NewHashQueueSelector()))
	case RandomQueueSelector:
		opts = append(opts, mqp.WithQueueSelector(mqp.NewRandomQueueSelector()))
	case RoundRobinQueueSelector:
		opts = append(opts, mqp.WithQueueSelector(mqp.NewRoundRobinQueueSelector()))
	case ManualQueueSelector:
		opts = append(opts, mqp.WithQueueSelector(mqp.NewManualQueueSelector()))
	case DaprQueueSelector:
		opts = append(opts, mqp.WithQueueSelector(NewDaprQueueSelector()))
	default:
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
	if nil != r.producer {
		return r.producer, nil
	}
	r.producerLock.Lock()
	defer r.producerLock.Unlock()
	if nil != r.producer {
		return r.producer, nil
	}
	producer, e := r.setUpProducer()
	if e != nil {
		return nil, e
	}
	r.producer = producer
	return r.producer, nil
}

func (r *rocketMQ) resetProducer() {
	r.producerLock.Lock()
	defer r.producerLock.Unlock()
	r.producer = nil
}

func (r *rocketMQ) Publish(req *pubsub.PublishRequest) error {
	r.logger.Debugf("rocketmq publish topic:%s with data:%v", req.Topic, req.Data)
	msg := primitive.NewMessage(req.Topic, req.Data)
	for k, v := range req.Metadata {
		switch strings.ToLower(k) {
		case metadataRocketmqTag:
			msg.WithTag(v)
		case metadataRocketmqKey:
			msg.WithKeys(strings.Split(v, ","))
		case metadataRocketmqShardingKey:
			msg.WithShardingKey(v)
		default:
			msg.WithProperty(k, v)
		}
	}
	producer, e := r.getProducer()
	if e != nil {
		return fmt.Errorf("rocketmq message send fail because producer failed to initialize: %v", e)
	}
	result, e := producer.SendSync(r.ctx, msg)
	if e != nil {
		r.resetProducer()
		m := fmt.Sprintf("rocketmq message send fail, topic[%s]: %v", req.Topic, e)
		r.logger.Error(m)
		return errors.New(m)
	}
	r.logger.Debugf("rocketmq message send result: topic[%s], tag[%s], status[%v]", req.Topic, msg.GetTags(), result.Status)
	return nil
}

func (r *rocketMQ) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	selector, e := buildMessageSelector(req)
	if e != nil {
		r.logger.Warnf("rocketmq subscribe failed: %v", e)
		return e
	}

	var cb func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error)
	if utils.IsTruthy(r.metadata.ConsumeOrderly) {
		cb = r.consumeMessageOrderly(req.Topic, selector, handler)
	} else {
		cb = r.consumeMessageConcurrently(req.Topic, selector, handler)
	}

	r.consumerLock.Lock()
	defer r.consumerLock.Unlock()

	r.topics[req.Topic] = *selector

	if nil == r.consumer {
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
				r.logger.Infof("consumer start success: Group[%s], Topics[%v]", r.metadata.ConsumerGroup, r.topics)
			} else {
				r.logger.Errorf("consumer start failed: %v", e)
			}
		}()
	}

	// subscribe topic
	if e = r.consumer.Subscribe(req.Topic, *selector, cb); e != nil {
		r.logger.Errorf("subscribe topic[%s] Group[%s] failed, error: %v", req.Topic, r.metadata.ConsumerGroup, e)
		return e
	}

	r.logger.Infof("subscribe topic[%s] success, Group[%s], Topics[%v]", req.Topic, r.metadata.ConsumerGroup, r.topics)
	return nil
}

func buildMessageSelector(req pubsub.SubscribeRequest) (*mqc.MessageSelector, error) {
	if req.Metadata == nil {
		req.Metadata = make(map[string]string)
	}
	mqExpr := req.Metadata[metadataRocketmqExpression]
	mqType := req.Metadata[metadataRocketmqType]

	var ExpressionType mqc.ExpressionType
	switch strings.ToUpper(mqType) {
	case "", string(mqc.TAG):
		ExpressionType = mqc.TAG
	case string(mqc.SQL92):
		ExpressionType = mqc.SQL92
	default:
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
		metadataRocketmqConsumerGroup: r.metadata.ProducerGroup,
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

func (r *rocketMQ) consumeMessageConcurrently(topic string, selector *mqc.MessageSelector, handler pubsub.Handler) func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
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

	if nil != r.consumer {
		_ = r.consumer.Shutdown()
		r.consumer = nil
	}

	return nil
}
