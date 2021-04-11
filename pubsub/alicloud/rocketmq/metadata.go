// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"encoding/json"

	"github.com/dapr/components-contrib/pubsub"
	mqw "github.com/cinience/go_rocketmq"
)

//rocketmq
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
	AccessProto string `json:"accessProto"`

	// rocketmq Credentials
	AccessKey string `json:"accessKey"`

	// rocketmq Credentials
	SecretKey string `json:"secretKey"`

	// rocketmq's name server, optional
	NameServer string `json:"nameServer"`

	// rocketmq's endpoint, optional, just for http proto
	Endpoint string `json:"endpoint"`

	// rocketmq's instanceId, optional
	InstanceId string `json:"instanceId"`

	// consumer group for rocketmq's subscribers, suggested to provide
	ConsumerGroup string `json:"consumerGroup"`

	// consumer group for rocketmq's subscribers, suggested to provide
	ConsumerBatchSize int `json:"consumerBatchSize,string"`

	// consumer group for rocketmq's subscribers, suggested to provide, just for tcp-cgo proto
	ConsumerThreadNums int `json:"consumerThreadNums,string"`

	// rocketmq's name server domain, optional
	NameServerDomain string `json:"nameServerDomain"`

	// retry times to connect rocketmq's broker, optional
	Retries int `json:"retries,string"`

	// topics to subscribe, use delimiter ',' to separate if more than one topics are configured, optional
	Topics string `json:"topics"`

	// msg's content-type eg:"application/cloudevents+json; charset=utf-8", application/octet-stream
	ContentType string `json:"content-type"`

	concurrency pubsub.ConcurrencyMode
}

func parseMetadata(md pubsub.Metadata) (*metadata, error) {
	b, err := json.Marshal(md.Properties)
	if err != nil {
		return nil, err
	}

	var m metadata
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	if m.ContentType == "" {
		m.ContentType = pubsub.DefaultCloudEventDataContentType
	}

	c, err := pubsub.Concurrency(md.Properties)
	if err != nil {
		return &m, err
	}
	m.concurrency = c
	return &m, nil
}

func parseCommonMetadata(md *metadata) (*mqw.Metadata, error) {
	str, err := json.Marshal(md)
	if err != nil {
		return nil, err
	}

	var m mqw.Metadata
	if err = json.Unmarshal(str, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
