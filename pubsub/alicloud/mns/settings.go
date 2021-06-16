// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mns

import (
	"fmt"

	"github.com/creasty/defaults"
	"github.com/dapr/components-contrib/internal/config"
)

var (
	// for more info: https://www.alibabacloud.com/help/doc-detail/27414.htm?spm=a2c63.p38356.879954.3.61943078qdiiEF#section-ghy-14s-7xd
	// use one-to-one mode for messaging
	MnsModeQueue = "queue"
	// use one-to-many mode for messaging
	MnsModeTopic = "topic"
)

// MNS settings
type Settings struct {
	// url for mns service
	Url string `mapstructure:"url"`
	// mns access key id
	AccessKeyId string `mapstructure:"accessKeyId"`
	// mns access key secret
	AccessKeySecret string `mapstructure:"accessKeySecret"`
	// mns mode (queue or topic)
	MnsMode string `mapstructure:"mnsMode"`
	// queue argument: mns queue name
	QueueName string `mapstructure:"queueName"`
	// queue argument: mns delay seconds for the queue, default 0
	DelaySeconds int32 `mapstructure:"DelaySeconds" default:"0"`
	// queue/topic argument: max message size in bytes for the queue, default 65536
	MaxMessageSize int32 `mapstructure:"maxMessageSize" default:"65536"`
	// queue argument: message retention period, default 345600
	MessageRetentionPeriod int32 `mapstructure:"messageRetentionPeriod" default:"345600"`
	// queue argument: visible time out, default 30
	VisibilityTimeout int32 `mapstructure:"visibilityTimeout" default:"30"`
	// queue argument: polling wait time in seconds, default 0
	PollingWaitSeconds int32 `mapstructure:"pollingWaitSeconds" default:"0"`
	// queue argument: slices, default 2
	Slices int32 `mapstructure:"slices" default:"2"`
	// topic argument: whether enable logging
	LoggingEnabled bool `mapstructure:"loggingEnabled"`
	// msg's content-type eg:"application/cloudevents+json; charset=utf-8", application/octet-stream
	ContentType string `mapstructure:"contentType"`
}

func (s *Settings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	defaults.Set(s)

	return nil
}
