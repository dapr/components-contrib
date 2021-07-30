// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mns

import (
	"context"
	"fmt"
	"strconv"
	"time"

	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"gopkg.in/square/go-jose.v2/json"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	receivingMessageWaitTime          = 1000 * time.Microsecond
	receivingMessagePeriod            = 30
	receivingMessageVisibilityTimeout = 5
)

type contextWithCancelFunc struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type mns struct {
	name         string
	settings     Settings
	logger       logger.Logger
	client       ali_mns.MNSClient
	queueManager ali_mns.AliQueueManager
	topicManager ali_mns.AliTopicManager

	ctx        context.Context
	cancelFunc context.CancelFunc

	queueContexts        map[string]contextWithCancelFunc
	subscriptionContexts map[string]contextWithCancelFunc
}

// NewMNS creates a new MNS pub/sub
func NewMNS(logger logger.Logger) pubsub.PubSub {
	ctx, cancelFunc := context.WithCancel(context.Background())

	return &mns{ //nolint:exhaustivestruct
		name:       "MNS",
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

// Init creates an mns client and its queue manager and topic manager
func (m *mns) Init(md pubsub.Metadata) error {
	var settings Settings
	settings.Decode(md.Properties)
	err := settings.Validate()
	if err != nil {
		return err
	}

	m.settings = settings

	m.client = ali_mns.NewAliMNSClientWithConfig(
		ali_mns.AliMNSClientConfig{
			EndPoint:        settings.URL,
			AccessKeyId:     settings.AccessKeyID,
			AccessKeySecret: settings.AccessKeySecret,
			Token:           settings.Token,
			TimeoutSecond:   settings.TimeoutSecond,
		},
	)

	m.queueManager = ali_mns.NewMNSQueueManager(m.client)

	m.topicManager = ali_mns.NewMNSTopicManager(m.client)

	m.queueContexts = make(map[string]contextWithCancelFunc)

	m.subscriptionContexts = make(map[string]contextWithCancelFunc)

	return nil
}

// Publish
func (m *mns) Publish(req *pubsub.PublishRequest) error {
	var metaData RequestMetaData
	metaData.Decode(req.Metadata)
	err := metaData.Validate()
	if err != nil {
		return err
	}

	jsonBody, err := json.Marshal(req.Metadata)
	if err != nil {
		return fmt.Errorf("publishing from MNS: %w", err)
	}

	var msgSendReq ali_mns.MessageSendRequest
	var msgPublishReq ali_mns.MessagePublishRequest
	var mailAttr ali_mns.MailAttributes

	json.Unmarshal(jsonBody, &msgSendReq)
	json.Unmarshal(jsonBody, &msgPublishReq)
	json.Unmarshal(jsonBody, &mailAttr)

	delaySeconds, _ := strconv.ParseInt(req.Metadata["delay_seconds"], 10, 64)
	msgSendReq.DelaySeconds = delaySeconds

	priority, _ := strconv.ParseInt(req.Metadata["priority"], 10, 64)
	msgSendReq.Priority = priority
	if priority < 1 || priority > 16 {
		return fmt.Errorf("priority must be between 1 ~ 16")
	}

	addressType, _ := strconv.ParseInt(req.Metadata["AddressType"], 10, 32)
	mailAttr.AddressType = int32(addressType)

	replyToAddress, _ := strconv.ParseInt(req.Metadata["ReplyToAddress"], 10, 32)
	mailAttr.ReplyToAddress = int32(replyToAddress)

	isHTML, _ := strconv.ParseBool(req.Metadata["IsHtml"])
	mailAttr.IsHtml = isHTML

	msgSendReq.MessageBody = string(req.Data)
	msgPublishReq.MessageBody = string(req.Data)

	msgPublishReq.MessageAttributes = &ali_mns.MessageAttributes{MailAttributes: &mailAttr} //nolint:exhaustivestruct

	switch m.settings.MNSMode {
	case MNSModeTopic:
		// create/fetch topic
		err = m.topicManager.CreateTopic(
			req.Topic,
			metaData.TopicMaxMessageSize,
			metaData.TopicLoggingEnabled,
		)
		if err != nil && !ali_mns.ERR_MNS_TOPIC_ALREADY_EXIST_AND_HAVE_SAME_ATTR.IsEqual(err) {
			m.logger.Error(err)

			return fmt.Errorf("publishing from MNS: %w", err)
		}

		topic := ali_mns.NewMNSTopic(req.Topic, m.client)
		topic.PublishMessage(msgPublishReq)

	case MNSModeQueue:
		queue := ali_mns.NewMNSQueue(metaData.QueueName, m.client)
		_, err := queue.SendMessage(msgSendReq)
		if err != nil {
			return fmt.Errorf("publishing from MNS: %w", err)
		}

	default:
		return fmt.Errorf("unsupported MNS mode: %v, should be queue or topic", m.settings.MNSMode)
	}

	return nil
}

// Subscribe
func (m *mns) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error { //nolint:cyclop
	var metaData RequestMetaData
	metaData.Decode(req.Metadata)
	err := metaData.Validate()
	if err != nil {
		return err
	}

	// create/fetch queue
	err = m.queueManager.CreateQueue(
		metaData.QueueName,
		metaData.QueueDelaySeconds,
		metaData.QueueMaxMessageSize,
		metaData.QueueMessageRetentionPeriod,
		metaData.QueueVisibilityTimeout,
		metaData.QueuePollingWaitSeconds,
		metaData.QueueSlices,
	)
	if err != nil && !ali_mns.ERR_MNS_QUEUE_ALREADY_EXIST_AND_HAVE_SAME_ATTR.IsEqual(err) {
		m.logger.Error(err)

		return fmt.Errorf("subscribing from MNS: %w", err)
	}

	switch m.settings.MNSMode {
	case MNSModeTopic:
		// create/fetch topic
		err = m.topicManager.CreateTopic(
			req.Topic,
			metaData.TopicMaxMessageSize,
			metaData.TopicLoggingEnabled,
		)
		if err != nil && !ali_mns.ERR_MNS_TOPIC_ALREADY_EXIST_AND_HAVE_SAME_ATTR.IsEqual(err) {
			m.logger.Error(err)

			return fmt.Errorf("subscribing from MNS: %w", err)
		}

		// subscribe topic
		topic := ali_mns.NewMNSTopic(req.Topic, m.client)
		sub := ali_mns.MessageSubsribeRequest{ // nolint: exhaustivestruct
			Endpoint:            topic.GenerateQueueEndpoint(metaData.QueueName),
			NotifyContentFormat: metaData.SubscriptionNotifyContentFormat,
		}

		err = topic.Subscribe(metaData.SubscriptionName, sub)
		if err != nil && !ali_mns.ERR_MNS_SUBSCRIPTION_ALREADY_EXIST_AND_HAVE_SAME_ATTR.IsEqual(err) {
			m.logger.Error(err)

			return fmt.Errorf("subscribing from MNS: %w", err)
		}

		time.Sleep(time.Duration(2) * time.Second)
	case MNSModeQueue:
		// do nothing here because queue is already created or fetched
	default:
		return fmt.Errorf("unsupported MNS mode: %v, should be queue or topic", m.settings.MNSMode)
	}

	ctx, cancelFunc := context.WithCancel(m.ctx) //nolint: govet

	queue := ali_mns.NewMNSQueue(metaData.QueueName, m.client)

	respChan := make(chan ali_mns.MessageReceiveResponse)
	errChan := make(chan error)

	go m.processMessageLoop(ctx, req.Topic, queue, handler, respChan, errChan)
	go m.receiveMessageLoop(ctx, queue, respChan, errChan)

	switch m.settings.MNSMode {
	case MNSModeQueue:
		if c, ok := m.queueContexts[metaData.QueueName]; ok {
			c.cancel()
		}
		m.queueContexts[metaData.QueueName] = contextWithCancelFunc{ctx, cancelFunc}
	case MNSModeTopic:
		if c, ok := m.subscriptionContexts[metaData.SubscriptionName]; ok {
			c.cancel()
		}
		m.subscriptionContexts[metaData.SubscriptionName] = contextWithCancelFunc{ctx, cancelFunc}
	}

	return nil // nolint: govet
}

func wrapMNSMessage(resp ali_mns.MessageReceiveResponse, topic string) (msg pubsub.NewMessage, err error) {
	msg.Topic = topic
	msg.Data = []byte(resp.MessageBody)

	jsonBody, err := json.Marshal(msg)
	if err != nil {
		return msg, fmt.Errorf("subscribing from MNS: %w", err)
	}

	err = json.Unmarshal(jsonBody, &msg.Metadata)
	if err != nil {
		return msg, fmt.Errorf("subscribing from MNS: %w", err)
	}

	return msg, nil
}

func (m *mns) processMessageLoop(ctx context.Context, topic string, queue ali_mns.AliMNSQueue, handler pubsub.Handler, respChan chan ali_mns.MessageReceiveResponse, errChan chan error) {
	for {
		select {
		case <-ctx.Done():

			return
		case resp := <-respChan:
			{
				m.logger.Infof("response: %+v", resp)

				msg, err := wrapMNSMessage(resp, topic)
				if err != nil {
					m.logger.Error(err)

					continue
				}

				handler(ctx, &msg)

				m.logger.Debugf("change the visibility: %+v", resp.ReceiptHandle)
				if ret, e := queue.ChangeMessageVisibility(resp.ReceiptHandle, int64(receivingMessageVisibilityTimeout)); e != nil {
					m.logger.Error(e)
				} else {
					m.logger.Infof("visibility changed: %+v", ret)
					m.logger.Debugf("delete it now: %+v", ret.ReceiptHandle)
					if e := queue.DeleteMessage(ret.ReceiptHandle); e != nil {
						m.logger.Error(e)
					}

					continue
				}
			}
		case err := <-errChan:
			{
				if !ali_mns.ERR_MNS_MESSAGE_NOT_EXIST.IsEqual(err) {
					m.logger.Error(err)
				}

				continue
			}
		default:
			time.Sleep(receivingMessageWaitTime)
		}
	}
}

func (m *mns) receiveMessageLoop(ctx context.Context, queue ali_mns.AliMNSQueue, respChan chan ali_mns.MessageReceiveResponse, errChan chan error) {
	for {
		select {
		case <-ctx.Done():

			return
		default:
			queue.ReceiveMessage(respChan, errChan, int64(receivingMessagePeriod))
		}
		time.Sleep(receivingMessageWaitTime)
	}
}

// Features does nothing here
func (m *mns) Features() []pubsub.Feature {
	return nil
}

// Close unsubscribes all topics/queues and closes this service gracefully
func (m *mns) Close() error {
	m.cancelFunc()

	return nil
}
