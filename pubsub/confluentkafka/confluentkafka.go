// +build !skip_confluent_kafka

package confluentkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	ProducerFlushTimeoutMs = 15 * 1000
)

type contextWithCancelFunc struct {
	Ctx        context.Context
	CancelFunc context.CancelFunc
}

type Kafka struct {
	ConfigMap *kafka.ConfigMap
	logger    logger.Logger
	producer  *kafka.Producer
	consumer  *kafka.Consumer
	topics    []string

	consumerContext contextWithCancelFunc
}

func NewConfluentKafka(logger logger.Logger) pubsub.PubSub {
	return &Kafka{ //nolint:exhaustivestruct
		logger: logger,
	}
}

func (k *Kafka) parseMetadata(props map[string]string) error {
	k.ConfigMap = &kafka.ConfigMap{}

	var metadata map[string]interface{}
	if configJSON, ok := props["configJson"]; ok {
		err := json.Unmarshal([]byte(configJSON), &metadata)
		if err != nil {
			return err
		}

		for key, val := range metadata {
			if v, ok := val.(float64); ok && v == math.Trunc(v) {
				val = int(v)
			}
			err = k.ConfigMap.SetKey(key, val)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return fmt.Errorf("needs key configJson in metadata for initialization and check https://github.com/confluentinc/confluent-kafka-go for format of the config")
}

func (k *Kafka) Init(md pubsub.Metadata) error {
	err := k.parseMetadata(md.Properties)
	if err != nil {
		return err
	}

	k.producer, err = kafka.NewProducer(k.ConfigMap)
	if err != nil {
		return err
	}

	k.consumer, err = kafka.NewConsumer(k.ConfigMap)
	if err != nil {
		return err
	}

	k.topics = make([]string, 0)

	return nil
}

func parsePublishMetadata(md map[string]string, msg *kafka.Message) error {
	if partitionStr, ok := md["partition"]; ok {
		partition, err := strconv.ParseInt(partitionStr, 10, 32)
		if err != nil {
			return err
		}
		msg.TopicPartition.Partition = int32(partition)
	} else {
		msg.TopicPartition.Partition = kafka.PartitionAny
	}

	if keyStr, ok := md["key"]; ok {
		msg.Key = []byte(keyStr)
	}

	for k, v := range md {
		if strings.HasPrefix(k, "headers.") {
			headerComponents := strings.Split(k, ".")
			if len(headerComponents) != 2 {
				continue
			}
			headerKey := headerComponents[1]
			msg.Headers = append(msg.Headers, kafka.Header{Key: headerKey, Value: []byte(v)})
		}
	}

	return nil
}

func (k *Kafka) Publish(req *pubsub.PublishRequest) error {
	msg := new(kafka.Message)
	err := parsePublishMetadata(req.Metadata, msg)
	if err != nil {
		return err
	}

	msg.TopicPartition.Topic = &req.Topic
	msg.Value = req.Data

	err = k.producer.Produce(msg, nil)
	if err != nil {
		return err
	}

	return nil
}

func loadMsg(kafkaMsg *kafka.Message, msg *pubsub.NewMessage) {
	msg.Topic = *kafkaMsg.TopicPartition.Topic
	msg.Data = kafkaMsg.Value
	msg.Metadata = make(map[string]string)

	msg.Metadata["partition"] = string(kafkaMsg.TopicPartition.Partition)
	msg.Metadata["offset"] = fmt.Sprint(kafkaMsg.TopicPartition.Offset)

	if msgMetaData := kafkaMsg.TopicPartition.Metadata; msgMetaData != nil {
		msg.Metadata["partitionMetadata"] = *msgMetaData
	}
	if msgErr := kafkaMsg.TopicPartition.Error; msgErr != nil {
		msg.Metadata["partitionError"] = msgErr.Error()
	}

	msg.Metadata["key"] = string(kafkaMsg.Key)

	msg.Metadata["timestamp"] = kafkaMsg.Timestamp.String()
	msg.Metadata["timestampType"] = kafkaMsg.TimestampType.String()

	headers := make(map[string]string)
	for _, h := range kafkaMsg.Headers {
		headers[h.Key] = string(h.Value)
	}
	jsonBytes, err := json.Marshal(headers)
	if err == nil {
		msg.Metadata["headers"] = string(jsonBytes)
	}
}

func (k *Kafka) processMessageWorker(handler pubsub.Handler) {
	for {
		select {
		case <-k.consumerContext.Ctx.Done():

			return
		default:
			kafkaMsg, err := k.consumer.ReadMessage(-1)
			if err == nil {
				msg := new(pubsub.NewMessage)
				loadMsg(kafkaMsg, msg)

				err = handler(k.consumerContext.Ctx, msg)
				if err != nil {
					k.logger.Error(err)
				}
			} else {
				k.logger.Error(err)
			}
		}
	}
}

func (k *Kafka) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	isNewTopic := true
	for _, t := range k.topics {
		if t == req.Topic {
			isNewTopic = false

			break
		}
	}
	if isNewTopic {
		k.topics = append(k.topics, req.Topic)
	}

	if k.consumerContext.Ctx != nil {
		k.consumerContext.CancelFunc()
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	k.consumerContext = contextWithCancelFunc{Ctx: ctx, CancelFunc: cancelFunc}

	go k.processMessageWorker(handler)

	err := k.consumer.SubscribeTopics(k.topics, nil)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) Features() []pubsub.Feature {
	return nil
}

func (k *Kafka) Close() error {
	k.producer.Flush(ProducerFlushTimeoutMs)
	err := k.consumer.Close()
	k.producer.Close()
	if err != nil {
		return err
	}

	return nil
}
