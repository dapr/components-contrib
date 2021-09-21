// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"fmt"

	mqw "github.com/cinience/go_rocketmq"

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
	Retries int `mapstructure:"retries"`
	// msg's content-type eg:"application/cloudevents+json; charset=utf-8", application/octet-stream
	ContentType string `mapstructure:"content-type"`
}

func (s *Settings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	return nil
}

func (s *Settings) ToRocketMQMetadata() *mqw.Metadata {
	return &mqw.Metadata{
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
