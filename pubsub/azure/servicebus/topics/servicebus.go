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

package topics

import (
	"context"
	"errors"
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
	defaultMaxBulkSubCount        = 100
	defaultMaxBulkPubBytes uint64 = 1024 * 128 // 128 KiB
)

type azureServiceBus struct {
	metadata      *impl.Metadata
	client        *impl.Client
	logger        logger.Logger
	features      []pubsub.Feature
	publishCtx    context.Context
	publishCancel context.CancelFunc
}

// NewAzureServiceBusTopics returns a new pub-sub implementation.
func NewAzureServiceBusTopics(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger:   logger,
		features: []pubsub.Feature{pubsub.FeatureMessageTTL},
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
			// Ensure the queue or topic exists the first time it is referenced
			// This does nothing if DisableEntityManagement is true
			err = a.client.EnsureTopic(a.publishCtx, req.Topic)
			if err != nil {
				return err
			}

			// Get the sender
			var sender *servicebus.Sender
			sender, err = a.client.GetSender(a.publishCtx, req.Topic)
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
					a.client.CloseSender(req.Topic)
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

	// Ensure the queue or topic exists the first time it is referenced
	// This does nothing if DisableEntityManagement is true
	err := a.client.EnsureTopic(a.publishCtx, req.Topic)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishFailed, err), err
	}

	// Get the sender
	sender, err := a.client.GetSender(ctx, req.Topic)
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
	err = impl.UpdateASBBatchMessageWithBulkPublishRequest(batchMsg, req)
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
			impl.GetPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second),
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
			impl.GetBulkPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second),
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
					a.logger.Errorf("Could not instantiate subscription %s for topic %s", a.metadata.ConsumerID, req.Topic)
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

func (a *azureServiceBus) Close() (err error) {
	a.publishCancel()
	a.client.CloseAllSenders(a.logger)
	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return a.features
}
