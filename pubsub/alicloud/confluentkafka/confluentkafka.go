package confluentkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

var (
	ProducerFlushTimeoutMs = 50 * 1000
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
	if configJson, ok := props["configJson"]; ok {
		err := json.Unmarshal([]byte(configJson), &metadata)
		if err != nil {
			return err
		}

		for key, val := range metadata{
			if reflect.ValueOf(val).Kind() == reflect.Float64{
				err = k.ConfigMap.SetKey(key, int(val.(float64)))
				if err != nil {
					return err
				}
			} else {
				err = k.ConfigMap.SetKey(key, val)
				if err != nil {
					return err
				}
			}
		}

		return nil
	} else {
		return fmt.Errorf("needs key configJson in metadata for initialization and check https://github.com/confluentinc/confluent-kafka-go for format of the config")
	}
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

	if offsetStr, ok := md["offset"]; ok {
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return err
		}
		msg.TopicPartition.Offset = kafka.Offset(offset)
	}

	if keyStr, ok := md["key"]; ok {
		msg.Key = []byte(keyStr)
	}

	// if timestampStr, ok := md["timestamp"]; ok {
	// 	if timeLayout, ok := md["timeLayout"]; ok {
	// 		var err error
	// 		msg.Timestamp, err = time.Parse(timeLayout, timestampStr)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	} else {
	// 		return fmt.Errorf("a value associated with key timeLayout must be provided for parsing timestamp, for more info: https://golang.org/src/time/format.go")
	// 	}

	// 	if timestampTypeStr, ok := md["timestampType"]; ok {
	// 		timestampType, err := strconv.Atoi(timestampTypeStr)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		msg.TimestampType = kafka.TimestampType(timestampType)
	// 	} else {
	// 		return fmt.Errorf("a value associated with key timestampType must be provided if timestamp is provided")
	// 	}
	// }

	if headerJson, ok := md["headers"]; ok {
		var headers map[string]string
		err := json.Unmarshal([]byte(headerJson), &headers)
		if err != nil {
			return err
		}
		for k, v := range headers {
			msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
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

	msg.Metadata["partition"] = string((*kafkaMsg).TopicPartition.Partition)
	msg.Metadata["offset"] = fmt.Sprint((*kafkaMsg).TopicPartition.Offset)

	if msgMetaData :=  (*kafkaMsg).TopicPartition.Metadata; msgMetaData != nil {
		msg.Metadata["partitionMetadata"] = *msgMetaData
	}
	if msgErr := (*kafkaMsg).TopicPartition.Error; msgErr != nil {
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

				err := handler(k.consumerContext.Ctx, msg)
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
