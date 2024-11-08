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
	"sync/atomic"
	"time"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.uber.org/multierr"
	"go.uber.org/ratelimit"

	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
	"github.com/dapr/kit/retry"
)

const (
	RequireSessionsMetadataKey       = "requireSessions"
	SessionIdleTimeoutMetadataKey    = "sessionIdleTimeoutInSec"
	MaxConcurrentSessionsMetadataKey = "maxConcurrentSessions"

	DefaultSesssionIdleTimeoutInSec = 60
	DefaultMaxConcurrentSessions    = 8

	// Maximum number of concurrent operations such as lock renewals or message completion/abandonment
	maxConcurrentOps = 20
)

// HandlerResponseItem represents a response from the handler for each message.
type HandlerResponseItem struct {
	EntryId string //nolint:stylecheck
	Error   error
}

// HandlerFn is the type for handlers that receive messages
type HandlerFn func(ctx context.Context, msgs []*azservicebus.ReceivedMessage) ([]HandlerResponseItem, error)

// Subscription is an object that manages a subscription to an Azure Service Bus receiver, for a topic or queue.
type Subscription struct {
	entity               string
	mu                   sync.RWMutex
	activeMessages       map[int64]*azservicebus.ReceivedMessage
	activeOperationsChan chan struct{}
	requireSessions      bool
	sessionIdleTimeout   time.Duration
	timeout              time.Duration
	lockRenewalInterval  time.Duration
	maxBulkSubCount      int
	retriableErrLimiter  ratelimit.Limiter
	handleChan           chan struct{}
	logger               logger.Logger
}

type SubscriptionOptions struct {
	MaxActiveMessages     int
	TimeoutInSec          int
	MaxBulkSubCount       *int
	MaxRetriableEPS       int
	MaxConcurrentHandlers int
	Entity                string
	LockRenewalInSec      int
	RequireSessions       bool
	SessionIdleTimeout    time.Duration
}

// NewBulkSubscription returns a new Subscription object.
// Parameter "entity" is usually in the format "topic <topicname>" or "queue <queuename>" and it's only used for logging.
func NewSubscription(opts SubscriptionOptions, logger logger.Logger) *Subscription {
	if opts.MaxBulkSubCount != nil {
		if *opts.MaxBulkSubCount < 1 {
			logger.Warnf("maxBulkSubCount must be greater than 0, setting it to 1")
			opts.MaxBulkSubCount = ptr.Of(1)
		}
	} else {
		// for non-bulk subscriptions, we only get one message at a time
		opts.MaxBulkSubCount = ptr.Of(1)
	}

	if *opts.MaxBulkSubCount > opts.MaxActiveMessages {
		logger.Warnf("maxBulkSubCount must not be greater than maxActiveMessages, setting it to %d", opts.MaxActiveMessages)
		opts.MaxBulkSubCount = &opts.MaxActiveMessages
	}

	s := &Subscription{
		entity:              opts.Entity,
		activeMessages:      make(map[int64]*azservicebus.ReceivedMessage),
		timeout:             time.Duration(opts.TimeoutInSec) * time.Second,
		lockRenewalInterval: time.Duration(opts.LockRenewalInSec) * time.Second,
		sessionIdleTimeout:  opts.SessionIdleTimeout,
		maxBulkSubCount:     *opts.MaxBulkSubCount,
		requireSessions:     opts.RequireSessions,
		logger:              logger,
		// This is a pessimistic estimate of the number of total operations that can be active at any given time.
		// In case of a non-bulk subscription, one operation is one message.
		activeOperationsChan: make(chan struct{}, opts.MaxActiveMessages/(*opts.MaxBulkSubCount)),
	}

	if opts.MaxRetriableEPS > 0 {
		s.retriableErrLimiter = ratelimit.New(opts.MaxRetriableEPS)
	} else {
		s.retriableErrLimiter = ratelimit.NewUnlimited()
	}

	if opts.MaxConcurrentHandlers > 0 {
		s.logger.Debugf("Subscription to %s is limited to %d message handler(s)", opts.Entity, opts.MaxConcurrentHandlers)
		s.handleChan = make(chan struct{}, opts.MaxConcurrentHandlers)
	}

	return s
}

// Connect to a Service Bus topic or queue, blocking until it succeeds; it can retry forever (until the context is canceled).
func (s *Subscription) Connect(ctx context.Context, newReceiverFunc func() (Receiver, error)) (Receiver, error) {
	// Connections need to retry forever with a maximum backoff of 5 minutes and exponential scaling.
	config := retry.DefaultConfig()
	config.Policy = retry.PolicyExponential
	config.MaxInterval = 5 * time.Minute
	config.MaxElapsedTime = 0
	backoff := config.NewBackOffWithContext(ctx)

	return retry.NotifyRecoverWithData(
		func() (Receiver, error) {
			receiver, innerErr := newReceiverFunc()
			if innerErr != nil {
				if s.requireSessions {
					var sbErr *azservicebus.Error
					if errors.As(innerErr, &sbErr) && sbErr.Code == azservicebus.CodeTimeout {
						return nil, errors.New("no sessions available")
					}
				}
				return nil, innerErr
			}
			if _, ok := receiver.(*SessionReceiver); !ok && s.requireSessions {
				return nil, fmt.Errorf("expected a session receiver, got %T", receiver)
			} else if _, ok := receiver.(*MessageReceiver); !ok && !s.requireSessions {
				return nil, fmt.Errorf("expected a message receiver, got %T", receiver)
			}
			return receiver, nil
		},
		backoff,
		func(err error, d time.Duration) {
			s.logger.Warnf("Failed to connect to Azure Service Bus %s; will retry in %s. Error: %s", s.entity, d, err.Error())
		},
		func() {
			s.logger.Infof("Successfully reconnected to Azure Service Bus %s", s.entity)
		},
	)
}

// ReceiveBlocking is a blocking call to receive messages on an Azure Service Bus subscription from a topic or queue.
func (s *Subscription) ReceiveBlocking(parentCtx context.Context, handler HandlerFn, receiver Receiver, onFirstSuccess func(), logMsg string) error {
	ctx, cancel := context.WithCancel(parentCtx)

	defer func() {
		// Cancel the context which also stops the lock renewal loop
		cancel()

		// Close the receiver when we're done
		s.logger.Debug("Closing message receiver for " + logMsg)
		closeReceiverCtx, closeReceiverCancel := context.WithTimeout(context.Background(), s.timeout)
		err := receiver.Close(closeReceiverCtx)
		closeReceiverCancel()
		if err != nil {
			// Log errors only
			s.logger.Warn("Error while closing receiver for " + logMsg)
		}
	}()

	// Lock renewal loop
	go func() {
		s.logger.Debug("Starting lock renewal loop for " + logMsg)
		lockErr := s.renewLocksBlocking(ctx, receiver)
		if lockErr != nil {
			if !errors.Is(lockErr, context.Canceled) {
				s.logger.Errorf("Error from lock renewal for %s: %v", logMsg, lockErr)
			}
		}
		s.logger.Debug("Exiting lock renewal loop for " + logMsg)
	}()

	// Receiver loop
	for {
		// This blocks if there are too many active operations already
		// This is released by the handler, but if the loop ends before it reaches the handler, make sure to release it with `<-s.activeOperationsChan`
		select {
		case s.activeOperationsChan <- struct{}{}:
			// No-op
		case <-ctx.Done():
			// Context is canceled or expired; return
			s.logger.Debugf("Receive context for %s done", s.entity)
			return ctx.Err()
		}

		// If we require sessions then we must have a timeout to allow
		// us to try and process any other sessions that have available
		// messages. If we do not require sessions then we will block
		// on the receiver until a message is available or the context
		// is canceled.
		var (
			receiverCtx    context.Context
			receiverCancel context.CancelFunc
		)
		if s.requireSessions && s.sessionIdleTimeout > 0 {
			// Canceled below after the context is used (we can't defer a cancelation because we're in a loop)
			receiverCtx, receiverCancel = context.WithTimeout(ctx, s.sessionIdleTimeout)
		} else {
			receiverCtx = ctx
		}

		// This method blocks until we get a message or the context is canceled
		msgs, err := receiver.ReceiveMessages(receiverCtx, s.maxBulkSubCount, nil)
		if receiverCancel != nil {
			receiverCancel()
		}
		if err != nil {
			if err != context.Canceled {
				s.logger.Errorf("Error reading from %s. %s", s.entity, err.Error())
			}
			<-s.activeOperationsChan
			// Return the error. This will cause the Service Bus component to try and reconnect.
			return err
		}

		if len(msgs) == 0 {
			// We got no message, which is unusual too
			// Treat this as error
			s.logger.Warn("Received 0 messages from Service Bus")
			<-s.activeOperationsChan
			// Return an error to force the Service Bus component to try and reconnect.
			return errors.New("received 0 messages from Service Bus")
		}

		// Invoke only once
		if onFirstSuccess != nil {
			onFirstSuccess()
			onFirstSuccess = nil
		}

		s.logger.Debugf("Received messages: %d; current active operations usage: %d/%d", len(msgs), len(s.activeOperationsChan), cap(s.activeOperationsChan))

		skipProcessing := false
		for _, msg := range msgs {
			err = s.addActiveMessage(msg)
			if err != nil {
				// If we cannot add the message then sequence number is not set, this must
				// be a bug in the Azure Service Bus SDK so we will log the error and not
				// handle the message. The message will eventually be retried until fixed.
				s.logger.Errorf("Error adding message: %s", err.Error())
				skipProcessing = true
				break
			}
			s.logger.Debugf("Processing received message: %s", msg.MessageID)
		}

		if skipProcessing {
			<-s.activeOperationsChan
			continue
		}

		// Handle the messages in background
		go s.handleAsync(ctx, msgs, handler, receiver)
	}
}

func (s *Subscription) renewLocksBlocking(ctx context.Context, receiver Receiver) error {
	if receiver == nil {
		return nil
	}

	if s.lockRenewalInterval <= 0 {
		s.logger.Info("Lock renewal for %s disabled", s.entity)
		return nil
	}

	t := time.NewTicker(s.lockRenewalInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if s.requireSessions {
				s.doRenewLocksSession(ctx, receiver.(*SessionReceiver))
			} else {
				s.doRenewLocks(ctx, receiver.(*MessageReceiver))
			}
		}
	}
}

func (s *Subscription) doRenewLocks(ctx context.Context, receiver *MessageReceiver) {
	s.logger.Debugf("Renewing message locks for %s", s.entity)

	// Snapshot the messages to try to renew locks for.
	s.mu.RLock()
	msgs := make([]*azservicebus.ReceivedMessage, len(s.activeMessages))
	var i int
	for _, m := range s.activeMessages {
		msgs[i] = m
		i++
	}
	s.mu.RUnlock()

	if len(msgs) == 0 {
		s.logger.Debugf("No active messages require lock renewal for %s", s.entity)
		return
	}

	// Collect errors
	count := atomic.Int64{}
	count.Store(int64(len(msgs)))
	errCh := make(chan error)
	go func() {
		var (
			err     error
			errored int
		)
		for range len(msgs) {
			// This is a nop if the received error is nil
			if multierr.AppendInto(&err, <-errCh) {
				errored++
			}
		}
		close(errCh)

		if err != nil {
			s.logger.Warnf("Error renewing message locks for %s (failed: %d/%d): %v", s.entity, errored, count.Load(), err)
		} else {
			s.logger.Debugf("Renewed message locks for %s for %d messages", s.entity, count.Load())
		}
	}()

	// Renew the locks for each message, with a limit of maxConcurrentOps in parallel
	limitCh := make(chan struct{}, maxConcurrentOps)
	for _, msg := range msgs {
		// Limit parllel executions
		limitCh <- struct{}{}
		go func(rMsg *azservicebus.ReceivedMessage) {
			// Check again if the message is active, in case it was already completed in the meanwhile
			s.mu.RLock()
			_, ok := s.activeMessages[*rMsg.SequenceNumber]
			s.mu.RUnlock()
			if !ok {
				count.Add(-1)
				// Release the limitCh lock and return no error
				<-limitCh
				errCh <- nil
				return
			}

			// Renew the lock for the message.
			lockCtx, lockCancel := context.WithTimeout(ctx, s.timeout)
			defer lockCancel()
			rErr := receiver.RenewMessageLock(lockCtx, rMsg, nil)

			// Since errChan is unbuffered, release the limitCh before we try to put the error back in errChan
			<-limitCh
			switch {
			case IsLockLostError(rErr):
				errCh <- errors.New("couldn't renew active message lock for message " + rMsg.MessageID + ": lock has been lost (this often happens if the message has already been completed or abandoned)")
			case rErr != nil:
				errCh <- fmt.Errorf("couldn't renew active message lock for message %s: %w", rMsg.MessageID, rErr)
			default:
				errCh <- nil
			}
		}(msg)
	}
	close(limitCh)
}

func (s *Subscription) doRenewLocksSession(ctx context.Context, sessionReceiver *SessionReceiver) {
	err := sessionReceiver.RenewSessionLocks(ctx, s.timeout)
	if err != nil {
		s.logger.Warnf("Error renewing session locks for %s: %v", s.entity, err)
	} else {
		s.logger.Debugf("Renewed session %s locks for %s", sessionReceiver.SessionID(), s.entity)
	}
}

// handleAsync handles messages from azure service bus and is meant to be called in a goroutine (go s.handleAsync).
func (s *Subscription) handleAsync(ctx context.Context, msgs []*azservicebus.ReceivedMessage, handler HandlerFn, receiver Receiver) {
	var (
		consumeToken           bool
		takenConcurrentHandler bool
	)

	// Invoke this at the end of the execution to release all taken tokens
	defer func() {
		// Release a handler if needed
		if takenConcurrentHandler {
			<-s.handleChan
			s.logger.Debugf("Released message handle for %s on %s", msgs[0].MessageID, s.entity)
		}

		// Remove the messages from the map of active ones
		s.removeActiveMessages(msgs)

		// If we got a retriable error (app handler returned a retriable error, or a network error while connecting to the app, etc) consume a retriable error token
		// We do it here, after the handler has been released but before releasing the active operation (which would allow us to retrieve more messages)
		if consumeToken {
			if s.logger.IsOutputLevelEnabled(logger.DebugLevel) {
				s.logger.Debugf("Taking a retriable error token")
				before := time.Now()
				_ = s.retriableErrLimiter.Take()
				s.logger.Debugf("Resumed after pausing for %v", time.Since(before))
			} else {
				_ = s.retriableErrLimiter.Take()
			}
		}

		// Remove an entry from activeOperationsChan to allow processing more messages
		<-s.activeOperationsChan
	}()

	// If handleChan is non-nil, we have a limit on how many handler we can process
	// Note that in log messages we only log the message ID of the first one in a batch, if there are more than one
	if cap(s.handleChan) > 0 {
		s.logger.Debugf("Taking message handle for %s on %s", msgs[0].MessageID, s.entity)
		select {
		// Context is done, so we will stop waiting
		case <-ctx.Done():
			s.logger.Debugf("Message context done for %s on %s", msgs[0].MessageID, s.entity)
			return
		// Blocks until we have a handler available
		case s.handleChan <- struct{}{}:
			takenConcurrentHandler = true
			s.logger.Debugf("Taken message handle for %s on %s", msgs[0].MessageID, s.entity)
		}
	}

	// Invoke the handler to process the message.
	resps, err := handler(ctx, msgs)
	if err != nil {
		// Errors here are from the app, so consume a retriable error token
		consumeToken = true

		// If we have a response with 0 items (or a nil response), it means the handler was a non-bulk one
		if len(resps) == 0 {
			// Log the error only, as we're running asynchronously
			s.logger.Errorf("App handler returned an error for message %s on %s: %s", msgs[0].MessageID, s.entity, err)
			finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), s.timeout)
			s.AbandonMessage(finalizeCtx, receiver, msgs[0])
			finalizeCancel()
			return
		}

		// Handle the errors on bulk messages and mark messages accordingly.
		// Note, the order of the responses match the order of the messages.
		// Perform the operations in a background goroutine with a limit of maxConcurrentOps concurrent
		limitCh := make(chan struct{}, maxConcurrentOps)
		wg := sync.WaitGroup{}
		for i := range resps {
			// Limit parllel executions
			limitCh <- struct{}{}
			wg.Add(1)

			go func(i int) {
				defer func() {
					wg.Done()
					<-limitCh
				}()

				// This context is used for the calls to service bus to finalize (i.e. complete/abandon) the message.
				// If we fail to finalize the message, this message will eventually be reprocessed (at-least once delivery).
				// This uses a background context in case ctx has been canceled already.
				finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), s.timeout)
				if resps[i].Error != nil {
					// Log the error only, as we're running asynchronously.
					s.logger.Errorf("App handler returned an error for message %s on %s: %s", msgs[i].MessageID, s.entity, resps[i].Error)
					s.AbandonMessage(finalizeCtx, receiver, msgs[i])
				} else {
					s.CompleteMessage(finalizeCtx, receiver, msgs[i])
				}
				finalizeCancel()
			}(i)
		}
		return
	}

	// No error, so we can complete all messages.
	if len(msgs) == 1 {
		// Avoid spawning goroutines for 1 message
		finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), s.timeout)
		s.CompleteMessage(finalizeCtx, receiver, msgs[0])
		finalizeCancel()
	} else {
		// Perform the operations in a background goroutine with a limit of maxConcurrentOps concurrent
		limitCh := make(chan struct{}, maxConcurrentOps)
		wg := sync.WaitGroup{}
		for _, msg := range msgs {
			// Limit parllel executions
			limitCh <- struct{}{}
			wg.Add(1)

			go func(msg *azservicebus.ReceivedMessage) {
				defer func() {
					wg.Done()
					<-limitCh
				}()

				finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), s.timeout)
				s.CompleteMessage(finalizeCtx, receiver, msg)
				finalizeCancel()
			}(msg)
		}
	}
}

// AbandonMessage marks a messsage as abandoned.
func (s *Subscription) AbandonMessage(ctx context.Context, receiver Receiver, m *azservicebus.ReceivedMessage) {
	s.logger.Debugf("Abandoning message %s on %s", m.MessageID, s.entity)

	// Use a background context in case a.ctx has been canceled already
	err := receiver.AbandonMessage(ctx, m, nil)
	if err != nil {
		// Log only
		s.logger.Warnf("Error abandoning message %s on %s: %s", m.MessageID, s.entity, err.Error())
	}
}

// CompleteMessage marks a message as complete.
func (s *Subscription) CompleteMessage(ctx context.Context, receiver Receiver, m *azservicebus.ReceivedMessage) {
	s.logger.Debugf("Completing message %s on %s", m.MessageID, s.entity)

	// Use a background context in case a.ctx has been canceled already
	err := receiver.CompleteMessage(ctx, m, nil)
	if err != nil {
		// Log only
		s.logger.Warnf("Error completing message %s on %s: %s", m.MessageID, s.entity, err.Error())
	}
}

func (s *Subscription) addActiveMessage(m *azservicebus.ReceivedMessage) error {
	if m.SequenceNumber == nil {
		return errors.New("message sequence number is nil")
	}

	var logSuffix string
	if m.SessionID != nil {
		if !s.requireSessions {
			s.logger.Warnf("Message %s with sequence number %d has a session ID but the subscription is not configured to require sessions", m.MessageID, *m.SequenceNumber)
		}
		logSuffix = " with session id " + *m.SessionID
	}
	s.logger.Debugf("Adding message %s with sequence number %d to active messages on %s%s", m.MessageID, *m.SequenceNumber, s.entity, logSuffix)
	s.mu.Lock()
	s.activeMessages[*m.SequenceNumber] = m
	s.mu.Unlock()
	return nil
}

func (s *Subscription) removeActiveMessages(msgs []*azservicebus.ReceivedMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, msg := range msgs {
		s.logger.Debugf("Removing message %s with sequence number %d from active messages on %s", msg.MessageID, *msg.SequenceNumber, s.entity)
		delete(s.activeMessages, *msg.SequenceNumber)
	}
}
