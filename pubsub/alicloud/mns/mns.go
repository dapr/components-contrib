// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mns

import (
	"context"

	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"

	"github.com/dapr/components-contrib/internal/retry"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type mns struct {
	name         string
	settings     Settings
	logger       logger.Logger
	client       ali_mns.MNSClient
	queueManager ali_mns.AliQueueManager
	topicManager ali_mns.AliTopicManager
	topics       map[string]bool

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

func (m *mns) Init(md pubsub.Metadata) error {
	var settings Settings
	settings.Decode(md.Properties)

	return nil
}
