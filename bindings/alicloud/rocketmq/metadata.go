// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"fmt"

	mqw "github.com/cinience/go_rocketmq"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/config"
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
	topicSeparator                = "||"
)

type metadata struct {
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
	Topics string `mapstructure:"topics"`
}

func parseMetadata(md bindings.Metadata) (*metadata, error) {
	var result metadata
	err := config.Decode(md.Properties, &result)
	if err != nil {
		return nil, fmt.Errorf("parse error:%w", err)
	}

	return &result, nil
}

func parseCommonMetadata(md *metadata) *mqw.Metadata {
	m := mqw.Metadata{
		AccessProto: md.AccessProto, AccessKey: md.AccessKey, SecretKey: md.SecretKey,
		NameServer: md.NameServer, Endpoint: md.Endpoint, InstanceId: md.InstanceID,
		ConsumerGroup: md.ConsumerGroup, ConsumerBatchSize: md.ConsumerBatchSize,
		ConsumerThreadNums: md.ConsumerThreadNums, NameServerDomain: md.NameServerDomain,
		Retries: md.Retries, Topics: md.Topics,
	}

	return &m
}
