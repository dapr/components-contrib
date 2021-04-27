// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"fmt"

	mqw "github.com/cinience/go_rocketmq"
	"github.com/dapr/components-contrib/internal/config"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	metadataRocketmqTag           = "rocketmq-tag"
	metadataRocketmqKey           = "rocketmq-key"
	metadataRocketmqConsumerGroup = "rocketmq-consumerGroup"
	metadataRocketmqType          = "rocketmq-sub-type"
	metadataRocketmqExpression    = "rocketmq-sub-expression"
	metadataRocketmqBrokerName    = "rocketmq-broker-name"
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
	// rocketmq's instanceId, optional
	InstanceID string `mapstructure:"instanceId"`
	// consumer group for rocketmq's subscribers, suggested to provide
	ConsumerGroup string `mapstructure:"consumerGroup"`
	// consumer group for rocketmq's subscribers, suggested to provide
	ConsumerBatchSize int `mapstructure:"consumerBatchSize"`
	// consumer group for rocketmq's subscribers, suggested to provide, just for tcp-cgo proto
	ConsumerThreadNums int `mapstructure:"consumerThreadNums"`
	// rocketmq's name server domain, optional
	NameServerDomain string `mapstructure:"nameServerDomain"`
	// retry times to connect rocketmq's broker, optional
	Retries int `mapstructure:"retries,string"`
	// topics to subscribe, use delimiter ',' to separate if more than one topics are configured, optional
	Topics string `mapstructure:"topics"`
	// msg's content-type eg:"application/cloudevents+json; charset=utf-8", application/octet-stream
	ContentType string `mapstructure:"content-type"`
}

func parseMetadata(md pubsub.Metadata) (*metadata, error) {
	var result metadata
	err := config.Decode(md.Properties, &result)
	if err != nil {
		return nil, fmt.Errorf("parse error:%w", err)
	}

	if result.ContentType == "" {
		result.ContentType = pubsub.DefaultCloudEventDataContentType
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
