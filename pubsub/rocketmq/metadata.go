package rocketmq

import (
	"fmt"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/config"
)

const (
	metadataRocketmqTag           = "rocketmq-tag"
	metadataRocketmqKey           = "rocketmq-key"
	metadataRocketmqConsumerGroup = "rocketmq-consumerGroup"
	metadataRocketmqType          = "rocketmq-sub-type"
	metadataRocketmqExpression    = "rocketmq-sub-expression"
	metadataRocketmqBrokerName    = "rocketmq-broker-name"
)

type rocketMQMetaData struct {
	// rocketmq Credentials
	AccessKey  string `mapstructure:"accessKey"`
	SecretKey  string `mapstructure:"secretKey"`
	NameServer string `mapstructure:"nameServer"`
	GroupName  string `mapstructure:"groupName"`
	NameSpace  string `mapstructure:"nameSpace"`
	// consumer group rocketmq's subscribers
	ConsumerGroup     string `mapstructure:"consumerGroup"`
	ConsumerBatchSize int    `mapstructure:"consumerBatchSize"`
	// rocketmq's name server domain
	NameServerDomain string `mapstructure:"nameServerDomain"`
	// msg's content-type
	ContentType string `mapstructure:"content-type"`
	// retry times to connect rocketmq's broker
	Retries int `mapstructure:"retries"`
}

func (s *rocketMQMetaData) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}
	return nil
}

func parseRocketMQMetaData(metadata pubsub.Metadata) (*rocketMQMetaData, error) {
	rMetaData := &rocketMQMetaData{
		ContentType: pubsub.DefaultCloudEventDataContentType,
	}
	err := rMetaData.Decode(metadata)
	if err != nil {
		return nil, fmt.Errorf("rocketmq configuration error: %w", err)
	}
	return rMetaData, nil
}
