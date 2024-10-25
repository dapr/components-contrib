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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/cenkalti/backoff/v4"
	mqw "github.com/cinience/go_rocketmq"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

type RocketMQ struct {
	logger   logger.Logger
	settings Settings
	producer mqw.Producer

	backOffConfig retry.Config
	closeCh       chan struct{}
	closed        atomic.Bool
	wg            sync.WaitGroup
}

func NewRocketMQ(l logger.Logger) *RocketMQ {
	return &RocketMQ{ //nolint:exhaustivestruct
		logger:   l,
		producer: nil,
		closeCh:  make(chan struct{}),
	}
}

// Init performs metadata parsing.
func (a *RocketMQ) Init(ctx context.Context, metadata bindings.Metadata) error {
	var err error
	if err = a.settings.Decode(metadata.Properties); err != nil {
		return err
	}

	// Default retry configuration is used if no
	// backOff properties are set.
	if err = retry.DecodeConfigWithPrefix(
		&a.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return fmt.Errorf("retry configuration error: %w", err)
	}

	a.producer, err = a.setupPublisher()
	if err != nil {
		return err
	}

	return nil
}

// Read triggers the rocketmq subscription.
func (a *RocketMQ) Read(ctx context.Context, handler bindings.Handler) error {
	if a.closed.Load() {
		return errors.New("error: binding is closed")
	}

	a.logger.Debugf("binding rocketmq: start read input binding")

	consumer, err := a.setupConsumer()
	if err != nil {
		return fmt.Errorf("binding-rocketmq error: %w", err)
	}

	if len(a.settings.Topics) == 0 {
		return errors.New("binding-rocketmq error: must configure topics")
	}

	for _, topicStr := range a.settings.Topics {
		if topicStr == "" {
			continue
		}

		var mqType, mqExpression, topic string
		if mqType, mqExpression, topic, err = parseTopic(topicStr); err != nil {
			return err
		}
		if err = consumer.Subscribe(
			topic,
			mqc.MessageSelector{
				Type:       mqc.ExpressionType(mqType),
				Expression: mqExpression,
			},
			a.adaptCallback(topic, a.settings.ConsumerGroup, mqType, mqExpression, handler),
		); err != nil {
			return fmt.Errorf("binding-rocketmq: subscribe %s failed. %w", topic, err)
		}
	}

	if err = consumer.Start(); err != nil {
		return fmt.Errorf("binding-rocketmq: consumer start failed. %w", err)
	}

	a.logger.Debugf("binding-rocketmq: consumer started")

	// Listen for context cancelation to stop the subscription
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		select {
		case <-ctx.Done():
		case <-a.closeCh:
		}

		innerErr := consumer.Shutdown()
		if innerErr != nil && !errors.Is(innerErr, context.Canceled) {
			a.logger.Warnf("binding-rocketmq: error while shutting down consumer: %v", innerErr)
		}
	}()

	return nil
}

// Close implements cancel all listeners, see https://github.com/dapr/components-contrib/issues/779
func (a *RocketMQ) Close() error {
	defer a.wg.Wait()
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	return nil
}

func parseTopic(key string) (mqType, mqExpression, topic string, err error) {
	mqType = ""
	mqExpression = ""
	array := strings.Split(key, topicSeparator)
	switch len(array) {
	case 0:
		break
	case 1:
		topic = array[0]
	case 2:
		topic = array[0]
		mqExpression = array[1]
	default:
		err = fmt.Errorf("binding-rocketmq config error: invalid format topic %s, must topicName||Expression(optional)", topic)
	}

	return
}

func (a *RocketMQ) setupConsumer() (mqw.PushConsumer, error) {
	if consumer, ok := mqw.Consumers[a.settings.AccessProto]; ok {
		md := a.settings.ToRocketMQMetadata()
		if err := consumer.Init(md); err != nil {
			a.logger.Errorf("rocketmq consumer init failed: %v", err)

			return nil, fmt.Errorf("setupConsumer %w", err)
		}

		a.logger.Infof("rocketmq access proto: %s", a.settings.AccessProto)

		return consumer, nil
	}

	return nil, errors.New("binding-rocketmq error: cannot found rocketmq consumer")
}

func (a *RocketMQ) setupPublisher() (mqw.Producer, error) {
	if producer, ok := mqw.Producers[a.settings.AccessProto]; ok {
		md := a.settings.ToRocketMQMetadata()
		if err := producer.Init(md); err != nil {
			a.logger.Debugf("rocketmq producer init failed: %v", err)

			return nil, fmt.Errorf("setupPublisher err:%w", err)
		}

		a.logger.Infof("rocketmq proto: %s", a.settings.AccessProto)
		if err := producer.Start(); err != nil {
			a.logger.Errorf("rocketmq producer start failed %v", err)

			return nil, fmt.Errorf("setupPublisher err:%w", err)
		}

		return producer, nil
	}

	return nil, errors.New("binding-rocketmq error: cannot found rocketmq producer")
}

// Operations returns list of operations supported by rocketmq binding.
func (a *RocketMQ) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *RocketMQ) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	rst := &bindings.InvokeResponse{Data: nil, Metadata: nil}

	if req.Operation != bindings.CreateOperation {
		return rst, fmt.Errorf("binding-rocketmq error: unsupported operation %s", req.Operation)
	}

	return rst, a.sendMessage(ctx, req)
}

func (a *RocketMQ) sendMessage(ctx context.Context, req *bindings.InvokeRequest) error {
	topic := req.Metadata[metadataRocketmqTopic]

	if topic != "" {
		_, err := a.send(ctx, topic, req.Metadata[metadataRocketmqTag], req.Metadata[metadataRocketmqKey], req.Data)
		if err != nil {
			return err
		}

		return nil
	}

	for _, topicStr := range a.settings.Topics {
		if topicStr == "" {
			continue
		}
		_, mqExpression, topic, err := parseTopic(topicStr)
		if err != nil {
			return err
		}
		_, err = a.send(ctx, topic, mqExpression, req.Metadata[metadataRocketmqKey], req.Data)
		if err != nil {
			return err
		}
		a.logger.Debugf("binding-rocketmq send msg done, topic:%s tag:%s data-length:%d ", topic, mqExpression, len(req.Data))
	}

	return nil
}

func (a *RocketMQ) send(ctx context.Context, topic, mqExpr, key string, data []byte) (bool, error) {
	msg := primitive.NewMessage(topic, data).WithTag(mqExpr).WithKeys([]string{key})
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rst, err := a.producer.SendSync(ctx, msg)
	if err != nil {
		return false, fmt.Errorf("binding-rocketmq: send failed err:%w", err)
	}
	if rst.Status == 0 {
		return true, nil
	}

	return false, fmt.Errorf("binding-rocketmq: unexpected status:%d", rst.Status)
}

type mqCallback func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error)

func (a *RocketMQ) adaptCallback(_, consumerGroup, mqType, mqExpr string, handler bindings.Handler) mqCallback {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
		success := true
		for _, v := range msgs {
			metadata := make(map[string]string, 4)
			metadata[metadataRocketmqType] = mqType
			metadata[metadataRocketmqExpression] = mqExpr
			metadata[metadataRocketmqConsumerGroup] = consumerGroup
			if v.Queue != nil {
				metadata[metadataRocketmqBrokerName] = v.Queue.BrokerName
			}
			a.logger.Debugf("binging-rocketmq handle msg, topic:%s msg-id:%s data-length:%d ", v.Topic, len(v.Body), v.MsgId)

			msg := &bindings.ReadResponse{
				Data:     v.Body,
				Metadata: metadata,
			}

			b := a.backOffConfig.NewBackOffWithContext(ctx)

			rerr := retry.NotifyRecover(func() error {
				herr := ctx.Err()
				if herr != nil {
					return backoff.Permanent(herr)
				}
				_, herr = handler(ctx, msg)
				if herr != nil {
					a.logger.Errorf("rocketmq error: fail to send message to dapr application. topic:%s data-length:%d err:%v ", v.Topic, len(v.Body), herr)
					success = false
				}

				return herr
			}, b, func(err error, d time.Duration) {
				a.logger.Errorf("rocketmq error: fail to processing message. topic:%s data-length:%d. Retrying...", v.Topic, len(v.Body))
			}, func() {
				a.logger.Infof("rocketmq successfully processed message after it previously failed. topic:%s data-length:%d.", v.Topic, len(v.Body))
			})
			if rerr != nil && !errors.Is(rerr, context.Canceled) {
				a.logger.Errorf("rocketmq error: processing message and retries are exhausted. topic:%s data-length:%d.", v.Topic, len(v.Body))
			}
		}
		if !success {
			return mqc.ConsumeRetryLater, nil
		}

		return mqc.ConsumeSuccess, nil
	}
}
