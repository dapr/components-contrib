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

package servicebus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/cenkalti/backoff/v4"

	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	"github.com/dapr/components-contrib/internal/utils"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	errorMessagePrefix            = "azure service bus error:"
	defaultMaxBulkSubCount        = 100
	defaultMaxBulkPubBytes uint64 = 1024 * 128 // 128 KiB
)

type azureServiceBus struct {
	metadata   *impl.Metadata
	client     *impl.Client
	logger     logger.Logger
	features   []pubsub.Feature
	topics     map[string]*servicebus.Sender
	topicsLock *sync.RWMutex

	publishCtx    context.Context
	publishCancel context.CancelFunc
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation.
func NewAzureServiceBus(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger:     logger,
		features:   []pubsub.Feature{pubsub.FeatureMessageTTL},
		topics:     map[string]*servicebus.Sender{},
		topicsLock: &sync.RWMutex{},
	}
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) (err error) {
	a.metadata, err = impl.ParseMetadata(metadata.Properties, a.logger, impl.MetadataModeTopics)
	if err != nil {
		return err
	}

	a.client, err = impl.NewClient(a.metadata, metadata.Properties)
	if err != nil {
		return err
	}

	a.publishCtx, a.publishCancel = context.WithCancel(context.Background())

	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	msg, err := impl.NewASBMessageFromPubsubRequest(req)
	if err != nil {
		return err
	}

	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Duration(a.metadata.PublishInitialRetryIntervalInMs) * time.Millisecond
	bo := backoff.WithMaxRetries(ebo, uint64(a.metadata.PublishMaxRetries))
	bo = backoff.WithContext(bo, a.publishCtx)

	msgID := "nil"
	if msg.MessageID != nil {
		msgID = *msg.MessageID
	}
	return retry.NotifyRecover(
		func() (err error) {
			// Get the sender
			var sender *servicebus.Sender
			sender, err = a.senderForTopic(a.publishCtx, req.Topic)
			if err != nil {
				return err
			}

			// Try sending the message
			ctx, cancel := context.WithTimeout(a.publishCtx, time.Second*time.Duration(a.metadata.TimeoutInSec))
			defer cancel()
			err = sender.SendMessage(ctx, msg, nil)
			if err != nil {
				if impl.IsNetworkError(err) {
					// Retry after reconnecting
					a.deleteSenderForTopic(req.Topic)
					return err
				}

				if impl.IsRetriableAMQPError(err) {
					// Retry (no need to reconnect)
					return err
				}

				// Do not retry on other errors
				return backoff.Permanent(err)
			}
			return nil
		},
		bo,
		func(err error, _ time.Duration) {
			a.logger.Warnf("Could not publish service bus message (%s). Retrying...: %v", msgID, err)
		},
		func() {
			a.logger.Infof("Successfully published service bus message (%s) after it previously failed", msgID)
		},
	)
}

func (a *azureServiceBus) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	// If the request is empty, sender.SendMessageBatch will panic later.
	// Return an empty response to avoid this.
	if len(req.Entries) == 0 {
		a.logger.Warnf("Empty bulk publish request, skipping")
		return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishSucceeded, nil), nil
	}

	sender, err := a.senderForTopic(ctx, req.Topic)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishFailed, err), err
	}

	// Create a new batch of messages with batch options.
	batchOpts := &servicebus.MessageBatchOptions{
		MaxBytes: utils.GetElemOrDefaultFromMap(req.Metadata, contribMetadata.MaxBulkPubBytesKey, defaultMaxBulkPubBytes),
	}

	batchMsg, err := sender.NewMessageBatch(ctx, batchOpts)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishFailed, err), err
	}

	// Add messages from the bulk publish request to the batch.
	err = UpdateASBBatchMessageWithBulkPublishRequest(batchMsg, req)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishFailed, err), err
	}

	// Azure Service Bus does not return individual status for each message in the request.
	err = sender.SendMessageBatch(ctx, batchMsg, nil)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishFailed, err), err
	}

	return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishSucceeded, nil), nil
}

func (a *azureServiceBus) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	sub := impl.NewSubscription(
		subscribeCtx,
		a.metadata.MaxActiveMessages,
		a.metadata.TimeoutInSec,
		nil,
		a.metadata.MaxRetriableErrorsPerSec,
		a.metadata.MaxConcurrentHandlers,
		"topic "+req.Topic,
		a.logger,
	)

	receiveAndBlockFn := func(onFirstSuccess func()) error {
		return sub.ReceiveAndBlock(
			a.getHandlerFunc(req.Topic, handler),
			a.metadata.LockRenewalInSec,
			false, // Bulk is not supported in regular Subscribe.
			onFirstSuccess,
		)
	}

	return a.doSubscribe(subscribeCtx, req, sub, receiveAndBlockFn)
}

func (a *azureServiceBus) BulkSubscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.BulkHandler) error {
	maxBulkSubCount := utils.GetElemOrDefaultFromMap(req.Metadata, contribMetadata.MaxBulkSubCountKey, defaultMaxBulkSubCount)
	sub := impl.NewSubscription(
		subscribeCtx,
		a.metadata.MaxActiveMessages,
		a.metadata.TimeoutInSec,
		&maxBulkSubCount,
		a.metadata.MaxRetriableErrorsPerSec,
		a.metadata.MaxConcurrentHandlers,
		"topic "+req.Topic,
		a.logger,
	)

	receiveAndBlockFn := func(onFirstSuccess func()) error {
		return sub.ReceiveAndBlock(
			a.getBulkHandlerFunc(req.Topic, handler),
			a.metadata.LockRenewalInSec,
			true, // Bulk is supported in BulkSubscribe.
			onFirstSuccess,
		)
	}

	return a.doSubscribe(subscribeCtx, req, sub, receiveAndBlockFn)
}

// doSubscribe is a helper function that handles the common logic for both Subscribe and BulkSubscribe.
// The receiveAndBlockFn is a function should invoke a blocking call to receive messages from the topic.
func (a *azureServiceBus) doSubscribe(subscribeCtx context.Context,
	req pubsub.SubscribeRequest, sub *impl.Subscription, receiveAndBlockFn func(func()) error,
) error {
	// Does nothing if DisableEntityManagement is true
	err := a.client.EnsureSubscription(subscribeCtx, a.metadata.ConsumerID, req.Topic)
	if err != nil {
		return err
	}

	// Reconnection backoff policy
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.InitialInterval = time.Duration(a.metadata.MinConnectionRecoveryInSec) * time.Second
	bo.MaxInterval = time.Duration(a.metadata.MaxConnectionRecoveryInSec) * time.Second

	onFirstSuccess := func() {
		// Reset the backoff when the subscription is successful and we have received the first message
		bo.Reset()
	}

	go func() {
		// Reconnect loop.
		for {
			// Blocks until a successful connection (or until context is canceled)
			err := sub.Connect(func() (*servicebus.Receiver, error) {
				return a.client.GetClient().NewReceiverForSubscription(req.Topic, a.metadata.ConsumerID, nil)
			})
			if err != nil {
				// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
				if errors.Is(err, context.Canceled) {
					a.logger.Errorf("%s could not instantiate subscription %s for topic %s", errorMessagePrefix, a.metadata.ConsumerID, req.Topic)
				}
				return
			}

			// receiveAndBlockFn will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
			// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
			err = receiveAndBlockFn(onFirstSuccess)
			if err != nil && !errors.Is(err, context.Canceled) {
				a.logger.Error(err)
			}

			// Gracefully close the connection (in case it's not closed already)
			// Use a background context here (with timeout) because ctx may be closed already
			closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
			sub.Close(closeCtx)
			closeCancel()

			// If context was canceled, do not attempt to reconnect
			if subscribeCtx.Err() != nil {
				a.logger.Debug("Context canceled; will not reconnect")
				return
			}

			wait := bo.NextBackOff()
			a.logger.Warnf("Subscription to topic %s lost connection, attempting to reconnect in %s...", req.Topic, wait)
			time.Sleep(wait)
		}
	}()

	return nil
}

func (a *azureServiceBus) getHandlerFunc(topic string, handler pubsub.Handler) impl.HandlerFunc {
	emptyResponseItems := []impl.HandlerResponseItem{}
	// Only the first ASB message is used in the actual handler invocation.
	return func(ctx context.Context, asbMsgs []*servicebus.ReceivedMessage) ([]impl.HandlerResponseItem, error) {
		if len(asbMsgs) != 1 {
			return nil, fmt.Errorf("expected 1 message, got %d", len(asbMsgs))
		}

		pubsubMsg, err := NewPubsubMessageFromASBMessage(asbMsgs[0], topic)
		if err != nil {
			return emptyResponseItems, fmt.Errorf("failed to get pubsub message from azure service bus message: %+v", err)
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second)
		defer handleCancel()
		a.logger.Debugf("Calling app's handler for message %s on topic %s", asbMsgs[0].MessageID, topic)
		return emptyResponseItems, handler(handleCtx, pubsubMsg)
	}
}

func (a *azureServiceBus) getBulkHandlerFunc(topic string, handler pubsub.BulkHandler) impl.HandlerFunc {
	return func(ctx context.Context, asbMsgs []*servicebus.ReceivedMessage) ([]impl.HandlerResponseItem, error) {
		pubsubMsgs := make([]pubsub.BulkMessageEntry, len(asbMsgs))
		for i, asbMsg := range asbMsgs {
			pubsubMsg, err := NewBulkMessageEntryFromASBMessage(asbMsg)
			if err != nil {
				return nil, fmt.Errorf("failed to get pubsub message from azure service bus message: %+v", err)
			}
			pubsubMsgs[i] = pubsubMsg
		}

		// Note, no metadata is currently supported here.
		// In the future, we could add propagate metadata to the handler if required.
		bulkMessage := &pubsub.BulkMessage{
			Entries:  pubsubMsgs,
			Metadata: map[string]string{},
			Topic:    topic,
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second)
		defer handleCancel()
		a.logger.Debugf("Calling app's handler for %d messages on topic %s", len(asbMsgs), topic)
		resps, err := handler(handleCtx, bulkMessage)

		implResps := make([]impl.HandlerResponseItem, len(resps))
		for i, resp := range resps {
			implResps[i] = impl.HandlerResponseItem{
				EntryId: resp.EntryId,
				Error:   resp.Error,
			}
		}

		return implResps, err
	}
}

// senderForTopic returns the sender for a topic, or creates a new one if it doesn't exist
func (a *azureServiceBus) senderForTopic(ctx context.Context, topic string) (*servicebus.Sender, error) {
	a.topicsLock.RLock()
	sender, ok := a.topics[topic]
	a.topicsLock.RUnlock()
	if ok && sender != nil {
		return sender, nil
	}

	a.topicsLock.Lock()
	defer a.topicsLock.Unlock()

	// Check again after acquiring a write lock in case another goroutine created the sender
	sender, ok = a.topics[topic]
	if ok && sender != nil {
		return sender, nil
	}

	// Ensure the topic exists the first time it is referenced
	// This does nothing if DisableEntityManagement is true
	err := a.client.EnsureTopic(ctx, topic)
	if err != nil {
		return nil, err
	}

	// Create the sender
	sender, err = a.client.GetClient().NewSender(topic, nil)
	if err != nil {
		return nil, err
	}
	a.topics[topic] = sender

	return sender, nil
}

// deleteSenderForTopic deletes a sender for a topic, closing the connection
func (a *azureServiceBus) deleteSenderForTopic(topic string) {
	a.topicsLock.Lock()
	defer a.topicsLock.Unlock()

	sender, ok := a.topics[topic]
	if ok && sender != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		_ = sender.Close(closeCtx)
		closeCancel()
	}
	delete(a.topics, topic)
}

func (a *azureServiceBus) Close() (err error) {
	a.topicsLock.Lock()
	defer a.topicsLock.Unlock()

	a.publishCancel()

	// Close all topics, up to 3 in parallel
	workersCh := make(chan bool, 3)
	for k, t := range a.topics {
		// Blocks if we have too many goroutines
		workersCh <- true
		go func(k string, t *servicebus.Sender) {
			a.logger.Debugf("Closing topic %s", k)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(a.metadata.TimeoutInSec)*time.Second)
			err = t.Close(ctx)
			cancel()
			if err != nil {
				// Log only
				a.logger.Warnf("%s closing topic %s: %+v", errorMessagePrefix, k, err)
			}
			<-workersCh
		}(k, t)
	}
	for i := 0; i < cap(workersCh); i++ {
		// Wait for all workers to be done
		workersCh <- true
	}
	close(workersCh)

	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return a.features
}
