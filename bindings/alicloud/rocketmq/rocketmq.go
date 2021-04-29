// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
)

type AliCloudRocketMQ struct {
	logger   logger.Logger
	metadata *metadata
	producer mqw.Producer
	consumer mqw.PushConsumer
}

func NewAliCloudRocketMQ(l logger.Logger) *AliCloudRocketMQ {
	return &AliCloudRocketMQ{
		logger:   l,
		metadata: nil,
		producer: nil,
		consumer: nil,
	}
}

// Init performs metadata parsing
func (t *AliCloudRocketMQ) Init(metadata bindings.Metadata) error {
	var err error
	t.metadata, err = parseMetadata(metadata)
	if err != nil {
		return err
	}

	t.producer, err = t.setupPublisher()
	if err != nil {
		return err
	}

	return nil
}

// Read triggers the rocketmq subscription
func (t *AliCloudRocketMQ) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	t.logger.Debugf("binding rocketmq: start read input binding")

	var err error
	t.consumer, err = t.setupConsumer()
	if err != nil {
		return fmt.Errorf("binding-rocketmq error: %w", err)
	}

	if t.metadata.Topics == "" {
		return fmt.Errorf("binding-rocketmq error: must config metadata.topics")
	}

	for _, topicStr := range strings.Split(t.metadata.Topics, topicSeparator) {
		if topicStr == "" {
			continue
		}
		mqType, mqExpression, topic, err := parseTopic(topicStr)
		if err != nil {
			return err
		}
		if err := t.consumer.Subscribe(
			topic,
			mqc.MessageSelector{
				Type:       mqc.ExpressionType(mqType),
				Expression: mqExpression,
			},
			t.adaptCallback(topic, t.metadata.ConsumerGroup, mqType, mqExpression, handler),
		); err != nil {
			return fmt.Errorf("binding-rocketmq: subscribe %s failed. %w", topic, err)
		}
	}

	if err := t.consumer.Start(); err != nil {
		return fmt.Errorf("binding-rocketmq: consumer start failed. %w", err)
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	<-exitChan
	t.logger.Info("binding-rocketmq: shutdown.")

	return nil
}

// Close implements cancel all listeners, see https://github.com/dapr/components-contrib/issues/779
func (t *AliCloudRocketMQ) Close() error {
	if t.consumer != nil {
		_ = t.consumer.Shutdown()
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

func (t *AliCloudRocketMQ) setupConsumer() (mqw.PushConsumer, error) {
	if consumer, ok := mqw.Consumers[t.metadata.AccessProto]; ok {
		md := parseCommonMetadata(t.metadata)
		if err := consumer.Init(md); err != nil {
			t.logger.Errorf("rocketmq consumer init failed: %v", err)

			return nil, fmt.Errorf("setupConsumer %w", err)
		}

		t.logger.Infof("rocketmq access proto: %s", t.metadata.AccessProto)

		return consumer, nil
	}

	return nil, errors.New("binding-rocketmq error: cannot found rocketmq consumer")
}

func (t *AliCloudRocketMQ) setupPublisher() (mqw.Producer, error) {
	if producer, ok := mqw.Producers[t.metadata.AccessProto]; ok {
		md := parseCommonMetadata(t.metadata)
		if err := producer.Init(md); err != nil {
			t.logger.Debugf("rocketmq producer init failed: %v", err)

			return nil, fmt.Errorf("setupPublisher err:%w", err)
		}

		t.logger.Infof("rocketmq proto: %s", t.metadata.AccessProto)
		if err := producer.Start(); err != nil {
			t.logger.Errorf("rocketmq producer start failed %v", err)

			return nil, fmt.Errorf("setupPublisher err:%w", err)
		}

		return producer, nil
	}

	return nil, errors.New("binding-rocketmq error: cannot found rocketmq producer")
}

// Operations returns list of operations supported by rocketmq binding
func (t *AliCloudRocketMQ) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (t *AliCloudRocketMQ) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	rst := &bindings.InvokeResponse{Data: nil, Metadata: nil}

	if req.Operation != bindings.CreateOperation {
		return rst, fmt.Errorf("binding-rocketmq error: unsupported operation %s", req.Operation)
	}

	return rst, t.sendMessage(req)
}

func (t *AliCloudRocketMQ) sendMessage(req *bindings.InvokeRequest) error {
	topic := req.Metadata[metadataRocketmqTopic]

	if topic != "" {
		_, err := t.send(topic, req.Metadata[metadataRocketmqTag], req.Metadata[metadataRocketmqKey], req.Data)
		if err != nil {
			return err
		}

		return nil
	}

	for _, topicStr := range strings.Split(t.metadata.Topics, topicSeparator) {
		if topicStr == "" {
			continue
		}
		_, mqExpression, topic, err := parseTopic(topicStr)
		if err != nil {
			return err
		}
		_, err = t.send(topic, mqExpression, req.Metadata[metadataRocketmqKey], req.Data)
		if err != nil {
			return err
		}
		t.logger.Debugf("binding-rocketmq send msg done, topic:%s tag:%s data-length:%d ", topic, mqExpression, len(req.Data))
	}

	return nil
}

func (t *AliCloudRocketMQ) send(topic, mqExpr, key string, data []byte) (bool, error) {
	msg := primitive.NewMessage(topic, data).WithTag(mqExpr).WithKeys([]string{key})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rst, err := t.producer.SendSync(ctx, msg)
	if err != nil {
		return false, fmt.Errorf("binding-rocketmq: send failed err:%w", err)
	}
	if rst.Status == 0 {
		return true, nil
	}

	return false, fmt.Errorf("binding-rocketmq: unexpected status:%d", rst.Status)
}

type mqCallback func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error)

func (t *AliCloudRocketMQ) adaptCallback(_, consumerGroup, mqType, mqExpr string, handler func(*bindings.ReadResponse) ([]byte, error)) mqCallback {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (mqc.ConsumeResult, error) {
		var success = true
		for _, v := range msgs {
			metadata := make(map[string]string, 4)
			metadata[metadataRocketmqType] = mqType
			metadata[metadataRocketmqExpression] = mqExpr
			metadata[metadataRocketmqConsumerGroup] = consumerGroup
			if v.Queue != nil {
				metadata[metadataRocketmqBrokerName] = v.Queue.BrokerName
			}
			t.logger.Debugf("binging-rocketmq handle msg, topic:%s msg-id:%s data-length:%d ", v.Topic, len(v.Body), v.MsgId)

			msg := &bindings.ReadResponse{Data: v.Body,
				Metadata: metadata}
			if _, err := handler(msg); err != nil {
				t.logger.Errorf("binging-rocketmq fail to send message to dapr application. topic:%s data-length:%d err:%v ", v.Topic, len(v.Body), err)
				success = false
			}
		}
		if !success {
			return mqc.ConsumeRetryLater, nil
		}

		return mqc.ConsumeSuccess, nil
	}
}
