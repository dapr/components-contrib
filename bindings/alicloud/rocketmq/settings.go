// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"fmt"
	"strings"

	rocketmq "github.com/cinience/go_rocketmq"
	"github.com/dapr/kit/config"
)

// rocketmq
const (
	metadataRocketmqTopic         = "rocketmq-topic"
	metadataRocketmqTag           = "rocketmq-tag"
	metadataRocketmqKey           = "rocketmq-key"
	metadataRocketmqConsumerGroup = "rocketmq-consumerGroup"
	metadataRocketmqType          = "rocketmq-sub-type"
	metadataRocketmqExpression    = "rocketmq-sub-expression"
	metadataRocketmqBrokerName    = "rocketmq-broker-name"
	multiTopicsSeparator          = ","
	topicSeparator                = "||"
)

type Settings struct {
	// sdk proto (tcp, tcp-cgo，http)
	AccessProto string `mapstructure:"accessProto"`
	// rocketmq Credentials
	AccessKey string `mapstructure:"accessKey"`
	// rocketmq Credentials
	SecretKey string `mapstructure:"secretKey"`
	// rocketmq's name server, optional
	NameServer string `mapstructure:"nameServer"`
	// rocketmq's endpoint, optional, just for http proto
	Endpoint string `mapstructure:"endpoint"`
	// consumer group for rocketmq's subscribers, suggested to provide
	ConsumerGroup string `mapstructure:"consumerGroup"`
	// consumer group for rocketmq's subscribers, suggested to provide
	ConsumerBatchSize int `mapstructure:"consumerBatchSize,string"`
	// consumer group for rocketmq's subscribers, suggested to provide, just for tcp-cgo proto
	ConsumerThreadNums int `mapstructure:"consumerThreadNums,string"`
	// rocketmq's namespace, optional
	InstanceID string `mapstructure:"instanceId"`
	// rocketmq's name server domain, optional
	NameServerDomain string `mapstructure:"nameServerDomain"`
	// retry times to connect rocketmq's broker, optional
	Retries int `mapstructure:"retries,string"`
	// topics to subscribe, use delimiter ',' to separate if more than one topics are configured, optional
	Topics TopicsDelimited `mapstructure:"topics"`
}

func (s *Settings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode error: %w", err)
	}

	return nil
}

func (s *Settings) ToRocketMQMetadata() *rocketmq.Metadata {
	return &rocketmq.Metadata{
		AccessProto:        s.AccessProto,
		AccessKey:          s.AccessKey,
		SecretKey:          s.SecretKey,
		NameServer:         s.NameServer,
		Endpoint:           s.Endpoint,
		InstanceId:         s.InstanceID,
		ConsumerGroup:      s.ConsumerGroup,
		ConsumerBatchSize:  s.ConsumerBatchSize,
		ConsumerThreadNums: s.ConsumerThreadNums,
		NameServerDomain:   s.NameServerDomain,
		Retries:            s.Retries,
	}
}

type TopicsDelimited []string

func (t *TopicsDelimited) DecodeString(value string) error {
	*t = strings.Split(value, multiTopicsSeparator)

	return nil
}

func (t *TopicsDelimited) ToString() string {
	return strings.Join(*t, multiTopicsSeparator)
}
