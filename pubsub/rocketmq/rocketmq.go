// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqw "github.com/cinience/go_rocketmq"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

type rocketMQ struct {
	name     string
	settings Settings
	producer mqw.Producer
	consumer mqw.PushConsumer
	logger   logger.Logger
	json     jsoniter.API
	topics   map[string]mqc.MessageSelector

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

// NewRocketMQ creates a new RocketMQ pub/sub.
func NewRocketMQ(logger logger.Logger) pubsub.PubSub {
	return &rocketMQ{ //nolint:exhaustivestruct
		name:     "rocketmq",
		consumer: nil,
		logger:   logger,
		json:     nil,
		topics:   nil,
	}
}

// Init does metadata parsing and connection creation.
func (r *rocketMQ) Init(md pubsub.Metadata) error {
	// Settings default values
	r.settings = Settings{ //nolint:exhaustivestruct
		ContentType: pubsub.DefaultCloudEventDataContentType,
	}
	err := r.settings.Decode(md.Properties)
	if err != nil {
		return fmt.Errorf("rocketmq configuration error: %w", err)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	// Default retry configuration is used if no
	// backOff properties are set.
	if err = retry.DecodeConfigWithPrefix(
		&r.backOffConfig,
		md.Properties,
		"backOff"); err != nil {
		return fmt.Errorf("retry configuration error: %w", err)
	}

	r.producer, err = r.setupPublisher()
	if err != nil {
		return fmt.Errorf("setupPublisher error: %w", err)
	}

	r.json = jsoniter.ConfigFastest
	r.topics = make(map[string]mqc.MessageSelector)

	r.consumer, err = r.setupConsumer()
	if err != nil {
		r.logger.Errorf("rocketmq init consumer failed: %v", err)

		return err
	}

	return nil
}

func (r *rocketMQ) setupPublisher() (mqw.Producer, error) {
	if producer, ok := mqw.Producers[r.settings.AccessProto]; ok {
		md := r.settings.ToRocketMQMetadata()
		if err := producer.Init(md); err != nil {
			r.logger.Debugf("rocketmq producer init failed: %v", err)

			return nil, fmt.Errorf("setupPublisher failed. %w", err)
		}
		r.logger.Infof("rocketmq proto: %s", r.settings.AccessProto)

		return producer, nil
	}

	return nil, errors.New("rocketmq error: cannot found rocketmq producer")
}

func (r *rocketMQ) setupConsumer() (mqw.PushConsumer, error) {
	if consumer, ok := mqw.Consumers[r.settings.AccessProto]; ok {
		md := r.settings.ToRocketMQMetadata()
		if err := consumer.Init(md); err != nil {
			r.logger.Errorf("rocketmq consumer init failed: %v", err)

			return nil, fmt.Errorf("setupConsumer failed. %w", err)
		}
		r.logger.Infof("rocketmq access proto: %s", r.settings.AccessProto)

		return consumer, nil
	}

	return nil, errors.New("rocketmq error: cannot found consumer")
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

func (r *rocketMQ) addTopic(newTopic string, selector mqc.MessageSelector) []string {
	// Add topic to our map of topics
	r.topics[newTopic] = selector

	topics := make([]string, len(r.topics))

	i := 0
	for topic := range r.topics {
		topics[i] = topic
		i++
	}

	return topics
}

// Close down consumer group resources, refresh once.
func (r *rocketMQ) closeSubscripionResources() {
	if r.consumer != nil {
		if len(r.topics) > 0 {
			_ = r.consumer.Shutdown()
		}
	}
}

func (r *rocketMQ) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if req.Metadata == nil {
		req.Metadata = make(map[string]string, 1)
	}

	consumerGroup := req.Metadata[metadataRocketmqConsumerGroup]
	if len(consumerGroup) == 0 {
		consumerGroup = r.settings.ConsumerGroup
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
	if r.consumer, err = r.setupConsumer(); err != nil {
		return err
	}

	topics := r.addTopic(req.Topic, mqc.MessageSelector{Type: mqc.ExpressionType(mqType), Expression: mqExpr})

	err = r.subscribeAllTopics(topics, consumerGroup, handler)
	if err != nil {
		return fmt.Errorf("rocketmq error: %w", err)
	}

	err = r.consumer.Start()
	if err != nil {
		return fmt.Errorf("consumer start failed. %w", err)
	}

	return nil
}

func (r *rocketMQ) subscribeAllTopics(topics []string, consumerGroup string, handler pubsub.Handler) error {
	for _, topic := range topics {
		selector, ok := r.topics[topic]
		if !ok {
			return fmt.Errorf("cannot found topic:%s selector", topic)
		}

		r.logger.Debugf("rocketmq start subscribe:%s group:%s type:%s expr:%s", topic, consumerGroup, string(selector.Type), selector.Expression)

		err := r.consumer.Subscribe(topic, selector, r.adaptCallback(topic, consumerGroup, string(selector.Type), selector.Expression, handler))
		if err != nil {
			r.logger.Errorf("rocketmq error to subscribe: %s", err)

			return fmt.Errorf("subscribe %s failed. %w", topic, err)
		}
	}

	return nil
}

func (r *rocketMQ) Features() []pubsub.Feature {
	return nil
}

func (r *rocketMQ) Close() error {
	r.cancel()

	if r.consumer != nil {
		_ = r.consumer.Shutdown()
	}

	if r.producer != nil {
		_ = r.producer.Shutdown()
	}

	return nil
}

type mqCallback func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error)

func (r *rocketMQ) adaptCallback(topic, consumerGroup, mqType, mqExpr string, handler pubsub.Handler) mqCallback {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
		success := true
		for _, v := range msgs {
			attributes := map[string]interface{}{
				pubsub.SubjectField: v.GetProperty(primitive.PropertyKeys),
			}

			attrBytes, _ := r.json.Marshal(attributes)

			data := pubsub.NewCloudEventsEnvelope(v.MsgId, v.StoreHost, r.name, v.GetProperty(primitive.PropertyKeys),
				v.Topic, r.name, r.settings.ContentType, v.Body, "", "", attrBytes)
			dataBytes, err := r.json.Marshal(data)
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
