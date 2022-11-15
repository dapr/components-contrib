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
	"fmt"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	metadataRocketmqTag           = "rocketmq-tag"
	metadataRocketmqKey           = "rocketmq-key"
	metadataRocketmqShardingKey   = "rocketmq-shardingkey"
	metadataRocketmqQueue         = "rocketmq-queue"
	metadataRocketmqConsumerGroup = "rocketmq-consumerGroup"
	metadataRocketmqType          = "rocketmq-sub-type"
	metadataRocketmqExpression    = "rocketmq-sub-expression"
	metadataRocketmqBrokerName    = "rocketmq-broker-name"
	metadataRocketmqQueueID       = "rocketmq-queue-id"
)

type QueueSelectorType string

const (
	HashQueueSelector       QueueSelectorType = "hash"
	RandomQueueSelector     QueueSelectorType = "random"
	ManualQueueSelector     QueueSelectorType = "manual"
	RoundRobinQueueSelector QueueSelectorType = "roundRobin"
	DaprQueueSelector       QueueSelectorType = "dapr"
)

// RocketMQ Go Client Options
type rocketMQMetaData struct {
	// rocketmq instance name, it will be registered to the broker
	InstanceName string `mapstructure:"instanceName"`
	// Deprecated: consumer group name
	GroupName     string `mapstructure:"groupName"`
	ConsumerGroup string `mapstructure:"consumerGroup"`
	// producer group name
	ProducerGroup string `mapstructure:"producerGroup"`
	// rocketmq namespace
	NameSpace string `mapstructure:"nameSpace"`
	// rocketmq's name server domain
	NameServerDomain string `mapstructure:"nameServerDomain"`
	// rocketmq's name server
	NameServer string `mapstructure:"nameServer"`
	// rocketmq Credentials
	AccessKey     string `mapstructure:"accessKey"`
	SecretKey     string `mapstructure:"secretKey"`
	SecurityToken string `mapstructure:"securityToken"`
	// retry times to send msg to broker
	Retries int `mapstructure:"retries"`

	// Producer Queue selector
	// There are five implementations of queue selector，Hash, Random, Manual, RoundRobin, Dapr，respectively
	//
	// Dapr Queue selector is design by dapr developers
	ProducerQueueSelector QueueSelectorType `mapstructure:"producerQueueSelector"`

	// Message model defines the way how messages are delivered to each consumer clients
	// 	RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
	// 	the same {@link #ConsumerGroup} would only consume shards of the messages subscribed, which achieves load
	// 	balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
	// 	separately.
	//
	// This field defaults to clustering.
	ConsumerModel string `mapstructure:"consumerModel"`

	// Consuming point on consumer booting.
	// There are three consuming points:
	//   - CONSUME_FROM_LAST_OFFSET: consumer clients pick up where it stopped previously. If it were a newly booting up
	//   consumer client, according aging of the consumer group, there are two cases.
	//     cases1:
	//       if the consumer group is created so recently that the earliest message being subscribed has yet
	//       expired, which means the consumer group represents a lately launched business, consuming will
	//       start from the very beginning.
	//     case2:
	//       if the earliest message being subscribed has expired, consuming will start from the latest messages,
	//       meaning messages born prior to the booting timestamp would be ignored.
	//   - CONSUME_FROM_FIRST_OFFSET: Consumer client will start from earliest messages available.
	//   - CONSUME_FROM_TIMESTAMP: Consumer client will start from specified timestamp, which means messages born
	//   prior to {@link #consumeTimestamp} will be ignored
	FromWhere string `mapstructure:"fromWhere"`

	/**
	 * Backtracking consumption time with second precision. Time format is
	 * 20131223171201<br>
	 * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
	 * Default backtracking consumption time Half an hour ago.
	 *
	 * RocketMQ Go Client does not support configuration in github.com/apache/rocketmq-client-go/v2 v2.1.1-rc2
	 */
	ConsumeTimestamp string `mapstructure:"consumeTimestamp"`

	// Whether it is an ordered message using FIFO order
	//
	// This field defaults to false.
	ConsumeOrderly string `mapstructure:"consumeOrderly"`

	// Batch consumption size
	ConsumeMessageBatchMaxSize int `mapstructure:"consumeMessageBatchMaxSize"`

	// Concurrently max span offset.it has no effect on sequential consumption
	ConsumeConcurrentlyMaxSpan int `mapstructure:"consumeConcurrentlyMaxSpan"`

	// Max re-consume times. -1 means 16 times.
	//
	// If messages are re-consumed more than {@link #maxReconsumeTimes} before Success, it's be directed to a deletion
	// queue waiting.
	MaxReconsumeTimes int32  `mapstructure:"maxReconsumeTimes"`
	AutoCommit        string `mapstructure:"autoCommit"`

	// Maximum amount of time a message may block the consuming thread.
	//
	// RocketMQ Go Client does not support configuration in github.com/apache/rocketmq-client-go/v2 v2.1.1-rc2
	ConsumeTimeout int `mapstructure:"consumeTimeout"`

	// The socket timeout in milliseconds
	ConsumerPullTimeout int `mapstructure:"consumerPullTimeout"`

	// Message pull Interval
	PullInterval int `mapstructure:"pullInterval"`

	// Deprecated: The number of messages pulled from the broker at a time
	ConsumerBatchSize int `mapstructure:"consumerBatchSize"`
	// The number of messages pulled from the broker at a time
	PullBatchSize int32 `mapstructure:"pullBatchSize"`

	// Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
	// Consider the {PullBatchSize}, the instantaneous value may exceed the limit
	//
	// RocketMQ Go Client does not support configuration in github.com/apache/rocketmq-client-go/v2 v2.1.1-rc2
	PullThresholdForQueue int64 `mapstructure:"pullThresholdForQueue"`

	// Flow control threshold on topic level, default value is -1(Unlimited)
	//
	// The value of {@code pullThresholdForQueue} will be overwritten and calculated based on
	// {@code pullThresholdForTopic} if it isn't unlimited
	//
	// For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
	// then pullThresholdForQueue will be set to 100
	//
	// RocketMQ Go Client does not support configuration in github.com/apache/rocketmq-client-go/v2 v2.1.1-rc2
	PullThresholdForTopic int64 `mapstructure:"pullThresholdForTopic"`

	// RocketMQ Go Client does not support configuration in github.com/apache/rocketmq-client-go/v2 v2.1.1-rc2
	PullThresholdSizeForQueue int `mapstructure:"pullThresholdSizeForQueue"`

	// Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
	//
	// The value of {@code pullThresholdSizeForQueue} will be overwritten and calculated based on
	// {@code pullThresholdSizeForTopic} if it isn't unlimited
	//
	// For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message queues are
	// assigned to this consumer, then pullThresholdSizeForQueue will be set to 100 MiB
	//
	// RocketMQ Go Client does not support configuration in github.com/apache/rocketmq-client-go/v2 v2.1.1-rc2
	PullThresholdSizeForTopic int    `mapstructure:"pullThresholdSizeForTopic"`
	ContentType               string `mapstructure:"content-type"` // msg's content-type
	// Deprecated: send msg timeout to connect rocketmq's broker, nanoseconds
	SendTimeOut int `mapstructure:"sendTimeOut"`
	// timeout for send msg to rocketmq broker, in seconds
	SendTimeOutSec int    `mapstructure:"sendTimeOutSec"`
	LogLevel       string `mapstructure:"logLevel"`

	// The RocketMQ message properties in this collection are passed to the APP in Data
	// Separate multiple properties with ","
	MsgProperties string `mapstructure:"mspProperties"`
}

func (s *rocketMQMetaData) Decode(in interface{}) error {
	if err := metadata.DecodeMetadata(in, &s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}
	return nil
}

const (
	KeyConsumeFromWhere string = "consumeFromWhere"
	KeyQueueSelector    string = "queueSelector"
)

func parseRocketMQMetaData(metadata pubsub.Metadata) (*rocketMQMetaData, error) {
	rMetaData := &rocketMQMetaData{
		Retries:             3,
		LogLevel:            "warn",
		PullInterval:        100,
		ConsumerPullTimeout: 30,
	}
	if metadata.Properties != nil {
		err := rMetaData.Decode(metadata.Properties)
		if err != nil {
			return nil, fmt.Errorf("rocketmq configuration error: %w", err)
		}
	}

	if rMetaData.ProducerGroup == "" {
		rMetaData.ProducerGroup = metadata.Properties[pubsub.RuntimeConsumerIDKey]
	}

	if rMetaData.FromWhere == "" {
		rMetaData.FromWhere = metadata.Properties[KeyConsumeFromWhere]
	}

	if rMetaData.ProducerQueueSelector == "" {
		rMetaData.ProducerQueueSelector = QueueSelectorType(metadata.Properties[KeyQueueSelector])
	}

	return rMetaData, nil
}
