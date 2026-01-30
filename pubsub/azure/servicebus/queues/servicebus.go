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
	stdstrings "strings"
	"sync"
	"sync/atomic"
	"time"

	impl "github.com/dapr/components-contrib/common/component/azure/servicebus"
	commonutils "github.com/dapr/components-contrib/common/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/strings"
)

const (
	defaultMaxBulkSubCount        = 100
	defaultMaxBulkPubBytes uint64 = 1024 * 128 // 128 KiB
)

// subscribeOptions contains options for subscribing to a queue with sessions support.
type subscribeOptions struct {
	requireSessions       bool
	maxConcurrentSessions int
	// assignedSessionIDs contains explicit session IDs this subscriber should handle.
	// If set, the subscriber will only accept these specific sessions.
	assignedSessionIDs []string
}

// partitionedSessionConfig holds configuration for partitioned session mode.
// This mode allows explicit control over which sessions are handled by which instance.
type partitionedSessionConfig struct {
	enabled    bool
	sessionIDs []string // List of session IDs defined in the component
}

type azureServiceBus struct {
	metadata           *impl.Metadata
	client             *impl.Client
	logger             logger.Logger
	closed             atomic.Bool
	closeCh            chan struct{}
	wg                 sync.WaitGroup
	partitionedSession partitionedSessionConfig
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

	a.client, err = impl.NewClient(a.metadata, metadata.Properties, a.logger)
	if err != nil {
		return err
	}

	// Parse partitioned session mode configuration
	if sessionIDsStr := metadata.Properties[impl.SessionIDsMetadataKey]; sessionIDsStr != "" {
		a.partitionedSession.enabled = true
		a.partitionedSession.sessionIDs = stdstrings.Split(sessionIDsStr, ",")
		// Trim whitespace from each session ID
		for i, id := range a.partitionedSession.sessionIDs {
			a.partitionedSession.sessionIDs[i] = stdstrings.TrimSpace(id)
		}
		a.logger.Infof("Partitioned session mode enabled with %d session IDs: %v",
			len(a.partitionedSession.sessionIDs), a.partitionedSession.sessionIDs)
		a.logger.Warnf("IMPORTANT: Scale up to %d replicas maximum (one per session ID)", len(a.partitionedSession.sessionIDs))
	}

	return nil
}

func (a *azureServiceBus) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	// In partitioned session mode, validate that the SessionId is one of the configured ones
	if a.partitionedSession.enabled {
		sessionID := ""
		if req.Metadata != nil {
			sessionID = req.Metadata[impl.MessageKeySessionID]
		}
		if sessionID == "" {
			return fmt.Errorf("partitioned session mode requires SessionId metadata; valid session IDs are: %v", a.partitionedSession.sessionIDs)
		}
		if !a.isValidSessionID(sessionID) {
			return fmt.Errorf("invalid SessionId '%s'; must be one of: %v", sessionID, a.partitionedSession.sessionIDs)
		}
	}

	// If message has a SessionId, we need to ensure the queue is created with sessions enabled
	ensureFn := a.client.EnsureQueue
	if req.Metadata != nil && req.Metadata[impl.MessageKeySessionID] != "" {
		ensureFn = func(ctx context.Context, queue string) error {
			return a.client.EnsureQueueWithSessions(ctx, queue, true)
		}
	}

	return a.client.PublishPubSub(ctx, req, ensureFn, a.logger)
}

// isValidSessionID checks if a session ID is in the list of configured session IDs.
func (a *azureServiceBus) isValidSessionID(sessionID string) bool {
	for _, id := range a.partitionedSession.sessionIDs {
		if id == sessionID {
			return true
		}
	}
	return false
}

func (a *azureServiceBus) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	if a.closed.Load() {
		return pubsub.BulkPublishResponse{}, errors.New("component is closed")
	}

	// In partitioned session mode, validate all messages have valid SessionIds
	if a.partitionedSession.enabled {
		for i, entry := range req.Entries {
			sessionID := ""
			if entry.Metadata != nil {
				sessionID = entry.Metadata[impl.MessageKeySessionID]
			}
			if sessionID == "" {
				return pubsub.BulkPublishResponse{}, fmt.Errorf("message %d: partitioned session mode requires SessionId metadata; valid session IDs are: %v", i, a.partitionedSession.sessionIDs)
			}
			if !a.isValidSessionID(sessionID) {
				return pubsub.BulkPublishResponse{}, fmt.Errorf("message %d: invalid SessionId '%s'; must be one of: %v", i, sessionID, a.partitionedSession.sessionIDs)
			}
		}
	}

	// Check if any message has a SessionId to determine if queue needs sessions
	requireSessions := false
	for _, entry := range req.Entries {
		if entry.Metadata != nil && entry.Metadata[impl.MessageKeySessionID] != "" {
			requireSessions = true
			break
		}
	}

	ensureFn := a.client.EnsureQueue
	if requireSessions {
		ensureFn = func(ctx context.Context, queue string) error {
			return a.client.EnsureQueueWithSessions(ctx, queue, true)
		}
	}

	return a.client.PublishPubSubBulk(ctx, req, ensureFn, a.logger)
}

func (a *azureServiceBus) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	requireSessions := strings.IsTruthy(req.Metadata[impl.RequireSessionsMetadataKey])
	sessionIdleTimeout := time.Duration(commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.SessionIdleTimeoutMetadataKey, impl.DefaultSesssionIdleTimeoutInSec)) * time.Second
	maxConcurrentSessions := commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.MaxConcurrentSessionsMetadataKey, impl.DefaultMaxConcurrentSessions)

	// Assigned session IDs for this subscriber (empty means accept any session)
	var assignedSessionIDs []string

	// In partitioned session mode, force sessions and use the configured session IDs
	if a.partitionedSession.enabled {
		requireSessions = true
		assignedSessionIDs = a.partitionedSession.sessionIDs
		maxConcurrentSessions = len(assignedSessionIDs)
		a.logger.Infof("Partitioned session mode: subscriber will handle sessions %v for queue %s", assignedSessionIDs, req.Topic)
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
			RequireSessions:       requireSessions,
			SessionIdleTimeout:    sessionIdleTimeout,
		},
		a.logger,
	)

	handlerFn := impl.GetPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second)
	return a.doSubscribe(ctx, req, sub, handlerFn, subscribeOptions{
		requireSessions:       requireSessions,
		maxConcurrentSessions: maxConcurrentSessions,
		assignedSessionIDs:    assignedSessionIDs,
	})
}

func (a *azureServiceBus) BulkSubscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.BulkHandler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	requireSessions := strings.IsTruthy(req.Metadata[impl.RequireSessionsMetadataKey])
	sessionIdleTimeout := time.Duration(commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.SessionIdleTimeoutMetadataKey, impl.DefaultSesssionIdleTimeoutInSec)) * time.Second
	maxConcurrentSessions := commonutils.GetElemOrDefaultFromMap(req.Metadata, impl.MaxConcurrentSessionsMetadataKey, impl.DefaultMaxConcurrentSessions)

	// Assigned session IDs for this subscriber (empty means accept any session)
	var assignedSessionIDs []string

	// In partitioned session mode, force sessions and use the configured session IDs
	if a.partitionedSession.enabled {
		requireSessions = true
		assignedSessionIDs = a.partitionedSession.sessionIDs
		maxConcurrentSessions = len(assignedSessionIDs)
		a.logger.Infof("Partitioned session mode: bulk subscriber will handle sessions %v for queue %s", assignedSessionIDs, req.Topic)
	}

	maxBulkSubCount := commonutils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxMessagesCount, defaultMaxBulkSubCount)
	sub := impl.NewSubscription(
		impl.SubscriptionOptions{
			MaxActiveMessages:     a.metadata.MaxActiveMessages,
			TimeoutInSec:          a.metadata.TimeoutInSec,
			MaxBulkSubCount:       &maxBulkSubCount,
			MaxRetriableEPS:       a.metadata.MaxRetriableErrorsPerSec,
			MaxConcurrentHandlers: a.metadata.MaxConcurrentHandlers,
			Entity:                "queue " + req.Topic,
			LockRenewalInSec:      a.metadata.LockRenewalInSec,
			RequireSessions:       requireSessions,
			SessionIdleTimeout:    sessionIdleTimeout,
		},
		a.logger,
	)

	handlerFn := impl.GetBulkPubSubHandlerFunc(req.Topic, handler, a.logger, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second)
	return a.doSubscribe(ctx, req, sub, handlerFn, subscribeOptions{
		requireSessions:       requireSessions,
		maxConcurrentSessions: maxConcurrentSessions,
		assignedSessionIDs:    assignedSessionIDs,
	})
}

// doSubscribe is a helper function that handles the common logic for both Subscribe and BulkSubscribe.
// The receiveAndBlockFn is a function should invoke a blocking call to receive messages from the topic.
func (a *azureServiceBus) doSubscribe(
	parentCtx context.Context,
	req pubsub.SubscribeRequest,
	sub *impl.Subscription,
	handlerFn impl.HandlerFn,
	opts subscribeOptions,
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
	err := a.client.EnsureQueueWithSessions(subscribeCtx, req.Topic, opts.requireSessions)
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
			if opts.requireSessions {
				if len(opts.assignedSessionIDs) > 0 {
					// Partitioned mode: connect to specific session IDs
					a.connectAndReceiveWithAssignedSessions(subscribeCtx, req, sub, handlerFn, bo.Reset, opts.assignedSessionIDs)
				} else {
					// Dynamic mode: accept any available session
					a.connectAndReceiveWithSessions(subscribeCtx, req, sub, handlerFn, bo.Reset, opts.maxConcurrentSessions)
				}
			} else {
				a.connectAndReceive(subscribeCtx, req, sub, handlerFn, bo.Reset)
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

// connectAndReceive connects to the queue and receives messages without sessions.
func (a *azureServiceBus) connectAndReceive(ctx context.Context, req pubsub.SubscribeRequest, sub *impl.Subscription, handlerFn impl.HandlerFn, onFirstSuccess func()) {
	logMsg := fmt.Sprintf("subscription to queue %s", req.Topic)

	// Blocks until a successful connection (or until context is canceled)
	receiver, err := sub.Connect(ctx, func() (impl.Receiver, error) {
		a.logger.Debug("Connecting to " + logMsg)
		r, rErr := a.client.GetClient().NewReceiverForQueue(req.Topic, nil)
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

// connectAndReceiveWithSessions connects to the queue and receives messages using sessions for ordered processing.
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
			a.logger.Debugf("Accepting next available session for queue %s", req.Topic)
			r, rErr := a.client.GetClient().AcceptNextSessionForQueue(acceptCtx, req.Topic, nil)
			if rErr != nil {
				return nil, rErr
			}
			return impl.NewSessionReceiver(r), nil
		})
		acceptCancel()
		if err != nil {
			// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
			if !errors.Is(err, context.Canceled) {
				a.logger.Errorf("Could not instantiate session subscription to queue %s", req.Topic)
			}
			return
		}

		// Receive messages for the session in a goroutine
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()

			logMsg := fmt.Sprintf("session %s for queue %s", receiver.(*impl.SessionReceiver).SessionID(), req.Topic)

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

// connectAndReceiveWithAssignedSessions connects to specific session IDs for partitioned session mode.
// This ensures this subscriber only handles the explicitly assigned sessions.
func (a *azureServiceBus) connectAndReceiveWithAssignedSessions(ctx context.Context, req pubsub.SubscribeRequest, sub *impl.Subscription, handlerFn impl.HandlerFn, onFirstSuccess func(), sessionIDs []string) {
	var wg sync.WaitGroup

	for _, sessionID := range sessionIDs {
		wg.Add(1)
		go func(sid string) {
			defer wg.Done()
			a.receiveFromSpecificSession(ctx, req, sub, handlerFn, onFirstSuccess, sid)
		}(sessionID)
	}

	wg.Wait()
}

// receiveFromSpecificSession connects to a specific session ID and receives messages.
func (a *azureServiceBus) receiveFromSpecificSession(ctx context.Context, req pubsub.SubscribeRequest, sub *impl.Subscription, handlerFn impl.HandlerFn, onFirstSuccess func(), sessionID string) {
	logMsg := fmt.Sprintf("assigned session %s for queue %s", sessionID, req.Topic)

	// Blocks until a successful connection (or until context is canceled)
	receiver, err := sub.Connect(ctx, func() (impl.Receiver, error) {
		a.logger.Debugf("Connecting to specific session %s for queue %s", sessionID, req.Topic)
		r, rErr := a.client.GetClient().AcceptSessionForQueue(ctx, req.Topic, sessionID, nil)
		if rErr != nil {
			return nil, rErr
		}
		return impl.NewSessionReceiver(r), nil
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			a.logger.Errorf("Could not connect to %s: %v", logMsg, err)
		}
		return
	}

	a.logger.Infof("Connected to %s - this subscriber will exclusively handle this session", logMsg)

	// ReceiveBlocking will only return with an error that it cannot handle internally.
	err = sub.ReceiveBlocking(ctx, handlerFn, receiver, onFirstSuccess, logMsg)
	if err != nil && !errors.Is(err, context.Canceled) {
		a.logger.Error(err)
	}
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
