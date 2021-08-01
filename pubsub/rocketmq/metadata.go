package rocketmq

import "github.com/dapr/components-contrib/pubsub"

const (
	metadataRocketmqTag           = "rocketmq-tag"
	metadataRocketmqKey           = "rocketmq-key"
	metadataRocketmqConsumerGroup = "rocketmq-consumerGroup"
	metadataRocketmqType          = "rocketmq-sub-type"
	metadataRocketmqExpression    = "rocketmq-sub-expression"
	metadataRocketmqBrokerName    = "rocketmq-broker-name"
)

type rocketMQMetaData struct {
	RetryTimes int
	AccessKey  string
	SecretKey  string
	NameServer string
}

func parseRocketMQMetaData(metadata pubsub.Metadata)*rocketMQMetaData{
	rmq := &rocketMQMetaData{
		RetryTimes: 0,
		AccessKey:  "",
		SecretKey:  "",
	}


	return rmq
}