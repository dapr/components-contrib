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
	"sync"
	"time"

	mq "github.com/apache/rocketmq-client-go/v2"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqp "github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

type topicData struct {
	selector      mqc.MessageSelector
	handler       pubsub.Handler
	consumerGroup string
	mqType        string
	mqExpr        string
}

type rocketMQ struct {
	name     string
	metadata *rocketMQMetaData

	logger       logger.Logger
	topics       map[string]topicData
	producer     mq.Producer
	producerLock sync.RWMutex
	consumer     mq.PushConsumer
	consumerLock sync.RWMutex

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

func NewRocketMQ(l logger.Logger) pubsub.PubSub {
	return &rocketMQ{
		name:         "rocketmq",
		logger:       l,
		topics:       make(map[string]topicData),
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
	r.ctx, r.cancel = context.WithCancel(context.Background())
	// Default retry configuration is used if no
	// backOff properties are set.
	if err = retry.DecodeConfigWithPrefix(
		&r.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return fmt.Errorf("retry configuration error: %w", err)
	}
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

func (r *rocketMQ) Publish(req *pubsub.PublishRequest) error {
	r.logger.Debugf("rocketmq publish topic:%s with data:%v", req.Topic, req.Data)
	msg := newRocketMQMessage(req)

	publishBo := backoff.NewExponentialBackOff()
	publishBo.InitialInterval = 100 * time.Millisecond
	bo := backoff.WithMaxRetries(publishBo, 3)
	bo = backoff.WithContext(bo, r.ctx)
	return retry.NotifyRecover(
		func() (err error) {
			r.producerLock.RLock()
			producer := r.producer
			r.producerLock.RUnlock()

			if producer == nil {
				r.producerLock.Lock()
				r.producer, err = r.setUpProducer()
				if err != nil {
					r.producer = nil
				}
				producer = r.producer
				r.producerLock.Unlock()
				if err != nil {
					return err
				}
			}

			ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.metadata.SendTimeOut))
			defer cancel()
			result, err := producer.SendSync(ctx, msg)
			if err != nil {
				r.producerLock.Lock()
				r.producer = nil
				r.producerLock.Unlock()
				r.logger.Errorf("error send message topic:%s : %v", req.Topic, err)
				return ErrRocketmqPublishMsg
			}
			r.logger.Debugf("rocketmq send result topic:%s tag:%s status:%v", req.Topic, msg.GetTags(), result.Status)
			return nil
		},
		bo,
		func(err error, d time.Duration) {
			r.logger.Errorf("rocketmq error: fail to send message. topic:%s. Retrying...", msg.Topic)
		},
		func() {
			r.logger.Infof("rocketmq successfully sent message after it previously failed. topic:%s.", msg.Topic)
		},
	)
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
			cloudEventsMap := pubsub.NewCloudEventsEnvelope(msg.MsgId, msg.StoreHost, r.name, msg.GetProperty(primitive.PropertyKeys), msg.Topic, r.name, r.metadata.ContentType, msg.Body, "", "")
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
			b := r.backOffConfig.NewBackOffWithContext(ctx)
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

func (r *rocketMQ) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if req.Metadata == nil {
		req.Metadata = make(map[string]string)
	}
	var (
		mqExpr = req.Metadata[metadataRocketmqExpression]
		mqType = req.Metadata[metadataRocketmqType]
	)
	if !r.validMqTypeParams(mqType) {
		return ErrRocketmqValidPublishMsgTyp
	}
	consumerGroup := r.metadata.ConsumerGroup
	if group, ok := req.Metadata[metadataRocketmqConsumerGroup]; ok {
		consumerGroup = group
	}

	r.consumerLock.Lock()
	defer r.consumerLock.Unlock()

	// Start the subscription
	// When the connection is ready, add the topic
	// Use the global context here to maintain the connection
	r.startSubscription(ctx, func() {
		r.topics[req.Topic] = topicData{
			handler: handler,
			selector: mqc.MessageSelector{
				Type:       mqc.ExpressionType(mqType),
				Expression: mqExpr,
			},
			consumerGroup: consumerGroup,
			mqExpr:        mqExpr,
			mqType:        mqType,
		}
	})

	// Listen for context cancelation to remove the subscription
	go func() {
		select {
		case <-ctx.Done():
		case <-r.ctx.Done():
		}

		r.consumerLock.Lock()
		defer r.consumerLock.Unlock()

		// If this is the last subscription or if the global context is done, close the connection entirely
		if len(r.topics) <= 1 || r.ctx.Err() != nil {
			_ = r.consumer.Shutdown()
			r.consumer = nil
			delete(r.topics, req.Topic)
			return
		}

		// Reconnect with one less topic
		r.startSubscription(r.ctx, func() {
			delete(r.topics, req.Topic)
		})
	}()

	return nil
}

// Should be wrapped around r.consumerLock lock
func (r *rocketMQ) startSubscription(ctx context.Context, onConnRready func()) (err error) {
	// reset synchronization
	if r.consumer != nil {
		r.logger.Infof("re-initializing the consumer")
		_ = r.consumer.Shutdown()
		r.consumer = nil
	} else {
		r.logger.Infof("initializing the consumer")
	}

	r.consumer, err = r.setUpConsumer()
	if err != nil {
		r.consumer = nil
		return err
	}

	// Invoke onConnReady so changes to the topics can be made safely
	onConnRready()

	for topic, data := range r.topics {
		cb := r.adaptCallback(topic, r.metadata.ConsumerGroup, string(data.selector.Type), data.selector.Expression, data.handler)
		err = r.consumer.Subscribe(topic, data.selector, cb)
		if err != nil {
			r.logger.Errorf("subscribe topic:%v failed,error:%v", topic, err)
			continue
		}
	}

	err = r.consumer.Start()
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
