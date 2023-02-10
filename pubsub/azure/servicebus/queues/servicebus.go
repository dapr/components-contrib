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

package queues

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"

	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	defaultMaxBulkSubCount        = 100
	defaultMaxBulkPubBytes uint64 = 1024 * 128 // 128 KiB
)

type azureServiceBus struct {
	metadata *impl.Metadata
	client   *impl.Client
	logger   logger.Logger
}

// NewAzureServiceBusQueues returns a new implementation.
func NewAzureServiceBusQueues(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger: logger,
	}
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) (err error) {
	a.metadata, err = impl.ParseMetadata(metadata.Properties, a.logger, impl.MetadataModeQueues)
	if err != nil {
		return err
	}

	a.client, err = impl.NewClient(a.metadata, metadata.Properties)
	if err != nil {
		return err
	}

	return nil
}

func (a *azureServiceBus) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	return a.client.PublishPubSub(ctx, req, a.client.EnsureQueue, a.logger)
}

func (a *azureServiceBus) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	return a.client.PublishPubSubBulk(ctx, req, a.client.EnsureQueue, a.logger)
}

func (a *azureServiceBus) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	sub := impl.NewSubscription(
		subscribeCtx,
		impl.SubscriptionOptions{
			MaxActiveMessages:     a.metadata.MaxActiveMessages,
			TimeoutInSec:          a.metadata.TimeoutInSec,
			MaxBulkSubCount:       nil,
			MaxRetriableEPS:       a.metadata.MaxRetriableErrorsPerSec,
			MaxConcurrentHandlers: a.metadata.MaxConcurrentHandlers,
			Entity:                "queue " + req.Topic,
			LockRenewalInSec:      a.metadata.LockRenewalInSec,
			RequireSessions:       false,
		},
		a.logger,
	)

	return a.doSubscribe(subscribeCtx, req, sub, func(receiver impl.Receiver, onFirstSuccess func()) error {
		return sub.ReceiveBlocking(
			impl.GetPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second),
			receiver,
			onFirstSuccess,
		)
	})
}

func (a *azureServiceBus) BulkSubscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.BulkHandler) error {
	maxBulkSubCount := utils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxMessagesCount, defaultMaxBulkSubCount)
	sub := impl.NewSubscription(
		subscribeCtx,
		impl.SubscriptionOptions{
			MaxActiveMessages:     a.metadata.MaxActiveMessages,
			TimeoutInSec:          a.metadata.TimeoutInSec,
			MaxBulkSubCount:       &maxBulkSubCount,
			MaxRetriableEPS:       a.metadata.MaxRetriableErrorsPerSec,
			MaxConcurrentHandlers: a.metadata.MaxConcurrentHandlers,
			Entity:                "queue " + req.Topic,
			LockRenewalInSec:      a.metadata.LockRenewalInSec,
			RequireSessions:       false,
		},
		a.logger,
	)

	return a.doSubscribe(subscribeCtx, req, sub, func(receiver impl.Receiver, onFirstSuccess func()) error {
		return sub.ReceiveBlocking(
			impl.GetBulkPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second),
			receiver,
			onFirstSuccess,
		)
	})
}

// doSubscribe is a helper function that handles the common logic for both Subscribe and BulkSubscribe.
// The receiveAndBlockFn is a function should invoke a blocking call to receive messages from the topic.
func (a *azureServiceBus) doSubscribe(
	subscribeCtx context.Context,
	req pubsub.SubscribeRequest,
	sub *impl.Subscription,
	receiveAndBlockFn func(receiver impl.Receiver, onFirstSuccess func()) error,
) error {
	// Does nothing if DisableEntityManagement is true
	err := a.client.EnsureQueue(subscribeCtx, req.Topic)
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
			receiver, err := sub.Connect(func() (impl.Receiver, error) {
				receiver, err := a.client.GetClient().NewReceiverForQueue(req.Topic, nil)
				return &impl.MessageReceiver{Receiver: receiver}, err
			})
			if err != nil {
				// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
				if errors.Is(err, context.Canceled) {
					a.logger.Errorf("Could not instantiate subscription to queue %s", req.Topic)
				}
				return
			}

			// receiveAndBlockFn will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
			// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
			err = receiveAndBlockFn(receiver, onFirstSuccess)
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

			// Check for context canceled again, after sleeping
			if subscribeCtx.Err() != nil {
				a.logger.Debug("Context canceled; will not reconnect")
				return
			}
		}
	}()

	return nil
}

func (a *azureServiceBus) Close() (err error) {
	a.client.CloseAllSenders(a.logger)
	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return []pubsub.Feature{
		pubsub.FeatureMessageTTL,
	}
}
