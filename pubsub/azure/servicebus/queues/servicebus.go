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
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/components-contrib/metadata"
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
	closed   atomic.Bool
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// NewAzureServiceBusQueues returns a new implementation.
func NewAzureServiceBusQueues(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

func (a *azureServiceBus) Init(_ context.Context, metadata pubsub.Metadata) (err error) {
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
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	return a.client.PublishPubSub(ctx, req, a.client.EnsureQueue, a.logger)
}

func (a *azureServiceBus) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	if a.closed.Load() {
		return pubsub.BulkPublishResponse{}, errors.New("component is closed")
	}

	return a.client.PublishPubSubBulk(ctx, req, a.client.EnsureQueue, a.logger)
}

func (a *azureServiceBus) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	sub := impl.NewSubscription(
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

	return a.doSubscribe(ctx, req, sub, impl.GetPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second))
}

func (a *azureServiceBus) BulkSubscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.BulkHandler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	maxBulkSubCount := utils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxMessagesCount, defaultMaxBulkSubCount)
	sub := impl.NewSubscription(
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

	return a.doSubscribe(ctx, req, sub, impl.GetBulkPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second))
}

// doSubscribe is a helper function that handles the common logic for both Subscribe and BulkSubscribe.
// The receiveAndBlockFn is a function should invoke a blocking call to receive messages from the topic.
func (a *azureServiceBus) doSubscribe(
	parentCtx context.Context,
	req pubsub.SubscribeRequest,
	sub *impl.Subscription,
	handlerFn impl.HandlerFn,
) error {
	subscribeCtx, cancel := context.WithCancel(parentCtx)
	a.wg.Add(1)
	go func() {
		select {
		case <-parentCtx.Done():
		case <-a.closeCh:
		}
		a.wg.Done()
		cancel()
	}()

	// Does nothing if DisableEntityManagement is true
	err := a.client.EnsureQueue(subscribeCtx, req.Topic)
	if err != nil {
		return err
	}

	// Reconnection backoff policy
	bo := a.client.ReconnectionBackoff()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		logMsg := fmt.Sprintf("subscription %s to queue %s", a.metadata.ConsumerID, req.Topic)

		// Reconnect loop.
		for {
			// Blocks until a successful connection (or until context is canceled)
			receiver, err := sub.Connect(subscribeCtx, func() (impl.Receiver, error) {
				a.logger.Debug("Connecting to " + logMsg)
				r, rErr := a.client.GetClient().NewReceiverForQueue(req.Topic, nil)
				if rErr != nil {
					return nil, rErr
				}
				return impl.NewMessageReceiver(r), nil
			})
			if err != nil {
				// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
				if errors.Is(err, context.Canceled) {
					a.logger.Error("Could not instantiate subscription " + logMsg)
				}
				return
			}

			// ReceiveBlocking will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
			// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
			// Reset the backoff when the subscription is successful and we have received the first message
			err = sub.ReceiveBlocking(subscribeCtx, handlerFn, receiver, bo.Reset, logMsg)
			if err != nil && !errors.Is(err, context.Canceled) {
				a.logger.Error(err)
			}

			// If context was canceled, do not attempt to reconnect
			if subscribeCtx.Err() != nil {
				a.logger.Debug("Context canceled; will not reconnect")
				return
			}

			wait := bo.NextBackOff()
			a.logger.Warnf("Subscription to queue %s lost connection, attempting to reconnect in %s...", req.Topic, wait)
			select {
			case <-time.After(wait):
			case <-subscribeCtx.Done():
				a.logger.Debug("Context canceled; will not reconnect")
				return
			}
		}
	}()

	return nil
}

func (a *azureServiceBus) Close() (err error) {
	defer a.wg.Wait()

	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}

	a.client.CloseAllSenders(a.logger)

	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return []pubsub.Feature{
		pubsub.FeatureMessageTTL,
		pubsub.FeatureBulkPublish,
	}
}

// GetComponentMetadata returns the metadata of the component.
func (a *azureServiceBus) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := impl.Metadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	delete(metadataInfo, "consumerID") // only applies to topics, not queues
	return
}
