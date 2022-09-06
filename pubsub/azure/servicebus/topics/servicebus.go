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
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	impl "github.com/dapr/components-contrib/common/component/azure/servicebus"
	commonutils "github.com/dapr/components-contrib/common/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/utils"
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

// NewAzureServiceBusTopics returns a new pub-sub implementation.
func NewAzureServiceBusTopics(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

func (a *azureServiceBus) Init(_ context.Context, metadata pubsub.Metadata) (err error) {
	a.metadata, err = impl.ParseMetadata(metadata.Properties, a.logger, impl.MetadataModeTopics)
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
	return a.client.PublishPubSub(ctx, req, a.client.EnsureTopic, a.logger)
}

func (a *azureServiceBus) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	if a.closed.Load() {
		return pubsub.BulkPublishResponse{}, errors.New("component is closed")
	}
	return a.client.PublishPubSubBulk(ctx, req, a.client.EnsureTopic, a.logger)
}

func (a *azureServiceBus) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	requireSessions := utils.IsTruthy(req.Metadata[impl.RequireSessionsMetadataKey])
	sessionIdleTimeout := time.Duration(commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.SessionIdleTimeoutMetadataKey, impl.DefaultSesssionIdleTimeoutInSec)) * time.Second
	maxConcurrentSessions := commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.MaxConcurrentSessionsMetadataKey, impl.DefaultMaxConcurrentSessions)

	sub := impl.NewSubscription(
		impl.SubscriptionOptions{
			MaxActiveMessages:     a.metadata.MaxActiveMessages,
			TimeoutInSec:          a.metadata.TimeoutInSec,
			MaxBulkSubCount:       nil,
			MaxRetriableEPS:       a.metadata.MaxRetriableErrorsPerSec,
			MaxConcurrentHandlers: a.metadata.MaxConcurrentHandlers,
			Entity:                "topic " + req.Topic,
			LockRenewalInSec:      a.metadata.LockRenewalInSec,
			RequireSessions:       requireSessions,
			SessionIdleTimeout:    sessionIdleTimeout,
		},
		a.logger,
	)

	handlerFn := impl.GetPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second)
	return a.doSubscribe(subscribeCtx, req, sub, handlerFn, impl.SubscribeOptions{
		RequireSessions:      requireSessions,
		MaxConcurrentSesions: maxConcurrentSessions,
	})
}

func (a *azureServiceBus) BulkSubscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.BulkHandler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	requireSessions := utils.IsTruthy(req.Metadata[impl.RequireSessionsMetadataKey])
	sessionIdleTimeout := time.Duration(commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.SessionIdleTimeoutMetadataKey, impl.DefaultSesssionIdleTimeoutInSec)) * time.Second
	maxConcurrentSessions := commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.MaxConcurrentSessionsMetadataKey, impl.DefaultMaxConcurrentSessions)

	maxBulkSubCount := commonutils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxMessagesCount, defaultMaxBulkSubCount)
	sub := impl.NewSubscription(
		impl.SubscriptionOptions{
			MaxActiveMessages:     a.metadata.MaxActiveMessages,
			TimeoutInSec:          a.metadata.TimeoutInSec,
			MaxBulkSubCount:       &maxBulkSubCount,
			MaxRetriableEPS:       a.metadata.MaxRetriableErrorsPerSec,
			MaxConcurrentHandlers: a.metadata.MaxConcurrentHandlers,
			Entity:                "topic " + req.Topic,
			LockRenewalInSec:      a.metadata.LockRenewalInSec,
			RequireSessions:       requireSessions,
			SessionIdleTimeout:    sessionIdleTimeout,
		},
		a.logger,
	)

	handlerFn := impl.GetBulkPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second)
	return a.doSubscribe(subscribeCtx, req, sub, handlerFn, impl.SubscribeOptions{
		RequireSessions:      requireSessions,
		MaxConcurrentSesions: maxConcurrentSessions,
	})
}

// doSubscribe is a helper function that handles the common logic for both Subscribe and BulkSubscribe.
// The receiveAndBlockFn is a function should invoke a blocking call to receive messages from the topic.
func (a *azureServiceBus) doSubscribe(
	parentCtx context.Context,
	req pubsub.SubscribeRequest,
	sub *impl.Subscription,
	handlerFn impl.HandlerFn,
	opts impl.SubscribeOptions,
) error {
	subscribeCtx, cancel := context.WithCancel(parentCtx)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer cancel()
		select {
		case <-parentCtx.Done():
		case <-a.closeCh:
		}
	}()

	// Does nothing if DisableEntityManagement is true
	err := a.client.EnsureSubscription(subscribeCtx, a.metadata.ConsumerID, req.Topic, opts)
	if err != nil {
		return err
	}

	// Reconnection backoff policy
	bo := a.client.ReconnectionBackoff()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		// Reconnect loop.
		for {
			// Reset the backoff when the subscription is successful and we have received the first message
			if opts.RequireSessions {
				a.connectAndReceiveWithSessions(subscribeCtx, req, sub, handlerFn, bo.Reset, opts.MaxConcurrentSesions)
			} else {
				a.connectAndReceive(subscribeCtx, req, sub, handlerFn, bo.Reset)
			}

			// If context was canceled, do not attempt to reconnect
			if subscribeCtx.Err() != nil {
				a.logger.Debug("Context canceled; will not reconnect")
				return
			}

			wait := bo.NextBackOff()
			a.logger.Warnf("Subscription to topic %s lost connection, attempting to reconnect in %s...", req.Topic, wait)
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
	if !a.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(a.closeCh)

	a.client.Close(a.logger)
	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return []pubsub.Feature{
		pubsub.FeatureMessageTTL,
		pubsub.FeatureBulkPublish,
	}
}

func (a *azureServiceBus) connectAndReceive(ctx context.Context, req pubsub.SubscribeRequest, sub *impl.Subscription, handlerFn impl.HandlerFn, onFirstSuccess func()) {
	logMsg := fmt.Sprintf("subscription %s to topic %s", a.metadata.ConsumerID, req.Topic)

	// Blocks until a successful connection (or until context is canceled)
	receiver, err := sub.Connect(ctx, func() (impl.Receiver, error) {
		a.logger.Debug("Connecting to " + logMsg)
		r, rErr := a.client.GetClient().NewReceiverForSubscription(req.Topic, a.metadata.ConsumerID, nil)
		if rErr != nil {
			return nil, rErr
		}
		return impl.NewMessageReceiver(r), nil
	})
	if err != nil {
		// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
		if !errors.Is(err, context.Canceled) {
			a.logger.Error("Could not instantiate " + logMsg)
		}
		return
	}

	a.logger.Debug("Receiving messages for " + logMsg)

	// ReceiveBlocking will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
	// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
	err = sub.ReceiveBlocking(ctx, handlerFn, receiver, onFirstSuccess, logMsg)
	if err != nil && !errors.Is(err, context.Canceled) {
		a.logger.Error(err)
	}
}

func (a *azureServiceBus) connectAndReceiveWithSessions(ctx context.Context, req pubsub.SubscribeRequest, sub *impl.Subscription, handlerFn impl.HandlerFn, onFirstSuccess func(), maxConcurrentSessions int) {
	sessionsChan := make(chan struct{}, maxConcurrentSessions)
	for range maxConcurrentSessions {
		sessionsChan <- struct{}{}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-sessionsChan:
			// nop - continue
		}

		// Check again if the context was canceled
		if ctx.Err() != nil {
			return
		}

		acceptCtx, acceptCancel := context.WithCancel(ctx)

		// Blocks until a successful connection (or until context is canceled)
		receiver, err := sub.Connect(ctx, func() (impl.Receiver, error) {
			a.logger.Debugf("Accepting next available session subscription %s to topic %s", a.metadata.ConsumerID, req.Topic)
			r, rErr := a.client.GetClient().AcceptNextSessionForSubscription(acceptCtx, req.Topic, a.metadata.ConsumerID, nil)
			if rErr != nil {
				return nil, rErr
			}
			return impl.NewSessionReceiver(r), nil
		})
		acceptCancel()
		if err != nil {
			// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
			if !errors.Is(err, context.Canceled) {
				a.logger.Errorf("Could not instantiate session subscription %s to topic %s", a.metadata.ConsumerID, req.Topic)
			}
			return
		}

		// Receive messages for the session in a goroutine
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()

			logMsg := fmt.Sprintf("session %s for subscription %s to topic %s", receiver.(*impl.SessionReceiver).SessionID(), a.metadata.ConsumerID, req.Topic)

			defer func() {
				// Return the session to the pool
				sessionsChan <- struct{}{}
			}()

			a.logger.Debug("Receiving messages for " + logMsg)

			// ReceiveBlocking will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
			// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
			err = sub.ReceiveBlocking(ctx, handlerFn, receiver, onFirstSuccess, logMsg)
			if err != nil && !errors.Is(err, context.Canceled) {
				a.logger.Error(err)
			}
		}()
	}
}

// GetComponentMetadata returns the metadata of the component.
func (a *azureServiceBus) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := impl.Metadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return
}
