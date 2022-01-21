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
	"errors"
	"fmt"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/config"
)

var (
	ErrRocketmqPublishMsg         = errors.New("rocketmq publish msg error")
	ErrRocketmqValidPublishMsgTyp = errors.New("rocketmq publish msg error, invalid msg type")
)

const (
	metadataRocketmqTag           = "rocketmq-tag"
	metadataRocketmqKey           = "rocketmq-key"
	metadataRocketmqShardingKey   = "rocketmq-shardingkey"
	metadataRocketmqConsumerGroup = "rocketmq-consumerGroup"
	metadataRocketmqType          = "rocketmq-sub-type"
	metadataRocketmqExpression    = "rocketmq-sub-expression"
	metadataRocketmqBrokerName    = "rocketmq-broker-name"
)

type rocketMQMetaData struct {
	AccessProto string `mapstructure:"accessProto"`
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
	Retries     int `mapstructure:"retries"`
	SendTimeOut int `mapstructure:"sendTimeOut"`
}

func getDefaultRocketMQMetaData() *rocketMQMetaData {
	return &rocketMQMetaData{
		AccessProto:       "",
		AccessKey:         "",
		SecretKey:         "",
		NameServer:        "",
		GroupName:         "",
		NameSpace:         "",
		ConsumerGroup:     "",
		ConsumerBatchSize: 0,
		NameServerDomain:  "",
		ContentType:       pubsub.DefaultCloudEventDataContentType,
		Retries:           3,
		SendTimeOut:       10,
	}
}

func (s *rocketMQMetaData) Decode(in interface{}) error {
	if err := config.Decode(in, &s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}
	return nil
}

func parseRocketMQMetaData(metadata pubsub.Metadata) (*rocketMQMetaData, error) {
	rMetaData := getDefaultRocketMQMetaData()
	if metadata.Properties != nil {
		err := rMetaData.Decode(metadata.Properties)
		if err != nil {
			return nil, fmt.Errorf("rocketmq configuration error: %w", err)
		}
	}
	return rMetaData, nil
}
