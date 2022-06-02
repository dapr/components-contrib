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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	mqw "github.com/cinience/go_rocketmq"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

type AliCloudRocketMQ struct {
	logger   logger.Logger
	settings Settings
	producer mqw.Producer
	consumer mqw.PushConsumer

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

func NewAliCloudRocketMQ(l logger.Logger) *AliCloudRocketMQ {
	return &AliCloudRocketMQ{ //nolint:exhaustivestruct
		logger:   l,
		producer: nil,
		consumer: nil,
	}
}

// Init performs metadata parsing.
func (a *AliCloudRocketMQ) Init(metadata bindings.Metadata) error {
	var err error
	if err = a.settings.Decode(metadata.Properties); err != nil {
		return err
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())

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
func (a *AliCloudRocketMQ) Read(handler bindings.Handler) error {
	a.logger.Debugf("binding rocketmq: start read input binding")

	var err error
	a.consumer, err = a.setupConsumer()
	if err != nil {
		return fmt.Errorf("binding-rocketmq error: %w", err)
	}

	if len(a.settings.Topics) == 0 {
		return fmt.Errorf("binding-rocketmq error: must configure topics")
	}

	for _, topicStr := range a.settings.Topics {
		if topicStr == "" {
			continue
		}
		mqType, mqExpression, topic, err := parseTopic(topicStr)
		if err != nil {
			return err
		}
		if err := a.consumer.Subscribe(
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

	if err := a.consumer.Start(); err != nil {
		return fmt.Errorf("binding-rocketmq: consumer start failed. %w", err)
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	<-exitChan
	a.logger.Info("binding-rocketmq: shutdown.")

	return nil
}

// Close implements cancel all listeners, see https://github.com/dapr/components-contrib/issues/779
func (a *AliCloudRocketMQ) Close() error {
	a.cancel()

	if a.consumer != nil {
		_ = a.consumer.Shutdown()
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

func (a *AliCloudRocketMQ) setupConsumer() (mqw.PushConsumer, error) {
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

func (a *AliCloudRocketMQ) setupPublisher() (mqw.Producer, error) {
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
func (a *AliCloudRocketMQ) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AliCloudRocketMQ) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	rst := &bindings.InvokeResponse{Data: nil, Metadata: nil}

	if req.Operation != bindings.CreateOperation {
		return rst, fmt.Errorf("binding-rocketmq error: unsupported operation %s", req.Operation)
	}

	return rst, a.sendMessage(req)
}

func (a *AliCloudRocketMQ) sendMessage(req *bindings.InvokeRequest) error {
	topic := req.Metadata[metadataRocketmqTopic]

	if topic != "" {
		_, err := a.send(topic, req.Metadata[metadataRocketmqTag], req.Metadata[metadataRocketmqKey], req.Data)
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
		_, err = a.send(topic, mqExpression, req.Metadata[metadataRocketmqKey], req.Data)
		if err != nil {
			return err
		}
		a.logger.Debugf("binding-rocketmq send msg done, topic:%s tag:%s data-length:%d ", topic, mqExpression, len(req.Data))
	}

	return nil
}

func (a *AliCloudRocketMQ) send(topic, mqExpr, key string, data []byte) (bool, error) {
	msg := primitive.NewMessage(topic, data).WithTag(mqExpr).WithKeys([]string{key})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

func (a *AliCloudRocketMQ) adaptCallback(_, consumerGroup, mqType, mqExpr string, handler bindings.Handler) mqCallback {
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

			b := a.backOffConfig.NewBackOffWithContext(a.ctx)

			rerr := retry.NotifyRecover(func() error {
				_, herr := handler(a.ctx, msg)
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
