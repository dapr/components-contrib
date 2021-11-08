// nolint: godot
// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
package mns

// nolint: goimports
import (
	"fmt"

	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/creasty/defaults"
	"github.com/dapr/kit/config"
)

const (
	// for more info: https://www.alibabacloud.com/help/doc-detail/27414.htm?spm=a2c63.p38356.879954.3.61943078qdiiEF#section-ghy-14s-7xd.
	// use one-to-one mode for messaging.
	MNSModeQueue = "queue"
	// use one-to-many mode for messaging.
	MNSModeTopic = "topic"
)

// MNS settings.
type Settings struct {
	// url for mns service.
	URL string `mapstructure:"url"`
	// mns access key id.
	AccessKeyID string `mapstructure:"accessKeyId"`
	// mns access key secret.
	AccessKeySecret string `mapstructure:"accessKeySecret"`
	// mns token, optional.
	Token string `mapstructure:"token"`
	// timeout in seconds, default 35.
	TimeoutSecond int64 `mapstructure:"timeoutSecond" default:"35"`
	// mns mode (queue or topic).
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

func (r *RequestMetaData) Decode(in interface{}) error {
	if err := config.Decode(in, r); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	defaults.Set(r)

	return nil
}

func (s *Settings) Validate() error {
	if s.TimeoutSecond < 0 {
		return fmt.Errorf("invalid timeout %v", s.TimeoutSecond)
	}

	return nil
}

// nolint: cyclop
func (r *RequestMetaData) Validate() error {
	if r.QueueDelaySeconds < 0 || r.QueueDelaySeconds > 604800 {
		return fmt.Errorf("delay seconds must be between 0 ~ 604800")
	}

	if r.QueueMaxMessageSize < 1024 || r.QueueMaxMessageSize > 65536 {
		return fmt.Errorf("max message size must be between 1024 and 65536")
	}

	if r.QueueMessageRetentionPeriod < 60 || r.QueueMessageRetentionPeriod > 604800 {
		return fmt.Errorf("message retention period must be between 60 ~ 604800")
	}

	if r.QueuePollingWaitSeconds < 0 || r.QueuePollingWaitSeconds > 30 {
		return fmt.Errorf("polling wait seconds must be between 0 ~ 30")
	}

	if r.QueueVisibilityTimeout < 1 || r.QueueVisibilityTimeout > 43200 {
		return fmt.Errorf("visibility timeout must be between 1 ~ 43200")
	}

	if r.TopicMaxMessageSize < 1024 || r.TopicMaxMessageSize > 65536 {
		return fmt.Errorf("max message size must be between 1024 and 65536")
	}

	if r.QueueSlices < 0 {
		return fmt.Errorf("slices must be larger than 0")
	}

	return nil
}
