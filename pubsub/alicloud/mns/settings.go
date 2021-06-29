// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mns

import (
	"fmt"

	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/creasty/defaults"
	"github.com/dapr/components-contrib/internal/config"
)

var (
	// for more info: https://www.alibabacloud.com/help/doc-detail/27414.htm?spm=a2c63.p38356.879954.3.61943078qdiiEF#section-ghy-14s-7xd
	// use one-to-one mode for messaging
	MNSModeQueue = "queue"
	// use one-to-many mode for messaging
	MNSModeTopic = "topic"
)

// MNS settings
type Settings struct {
	// url for mns service
	URL string `mapstructure:"url"`
	// mns access key id
	AccessKeyID string `mapstructure:"accessKeyId"`
	// mns access key secret
	AccessKeySecret string `mapstructure:"accessKeySecret"`
	// mns token, optional
	Token string `mapstructure:"token"`
	// timeout in seconds, default 35
	TimeoutSecond int64 `mapstructure:"timeoutSecond" default:"35"`
	// mns mode (queue or topic)
	MNSMode string `mapstructure:"mnsMode"`
	// msg's content-type eg:"application/cloudevents+json; charset=utf-8", application/octet-stream
	ContentType string `mapstructure:"contentType"`
}

// request meta data
type RequestMetaData struct {
	// queue argument: Queue name
	QueueName string `mapstructure:"queueName" default:"defaultMnsQueue"`
	// queue argument: mns delay seconds for the queue, default 0
	QueueDelaySeconds int32 `mapstructure:"queueDelaySeconds"`
	// queue argument: max message size in bytes for the queue, default 65536
	QueueMaxMessageSize int32 `mapstructure:"queueMaxMessageSize" default:"65536"`
	// queue argument: message retention period, default 345600
	QueueMessageRetentionPeriod int32 `mapstructure:"queueMessageRetentionPeriod" default:"345600"`
	// queue argument: visible time out, default 30
	QueueVisibilityTimeout int32 `mapstructure:"queueVisibilityTimeout" default:"30"`
	// queue argument: polling wait time in seconds, default 0
	QueuePollingWaitSeconds int32 `mapstructure:"queuePollingWaitSeconds"`
	// queue argument: slices, default 2
	QueueSlices int32 `mapstructure:"queueSlices" default:"2"`
	// topic argument: max message size in bytes for the queue, default 65536
	TopicMaxMessageSize int32 `mapstructure:"TopicMaxMessageSize" default:"65536"`
	// topic argument: whether enable logging
	TopicLoggingEnabled bool `mapstructure:"TopicLoggingEnabled"`
	// subscription argument: subscription name
	SubscriptionName string `mapstructure:"subscriptionName"`
	// subscription argument: notify content type, could be XML or SIMPLIFIED, default SIMPLIFIED
	SubscriptionNotifyContentFormat ali_mns.NotifyContentFormatType `mapstructure:"subscriptionNotifyContentFormat" default:"SIMPLIFIED"`
}

func (s *Settings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	defaults.Set(s)

	return nil
}

func (s *RequestMetaData) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	defaults.Set(s)

	return nil
}
