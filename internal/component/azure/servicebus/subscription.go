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

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.uber.org/ratelimit"

	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
	"github.com/dapr/kit/retry"
)

// HandlerResponseItem represents a response from the handler for each message.
type HandlerResponseItem struct {
	EntryId string //nolint:stylecheck
	Error   error
}

// HandlerFunc is the type for handlers that receive messages
type HandlerFunc func(ctx context.Context, msgs []*azservicebus.ReceivedMessage) ([]HandlerResponseItem, error)

type Receiver interface {
	ID() string
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, m *azservicebus.ReceivedMessage, opts *azservicebus.CompleteMessageOptions) error
	AbandonMessage(ctx context.Context, m *azservicebus.ReceivedMessage, opts *azservicebus.AbandonMessageOptions) error
	RenewLocks(ctx context.Context, m *azservicebus.ReceivedMessage) error
	Close(ctx context.Context) error
}

var (
	_ Receiver = (*SessionReceiver)(nil)
	_ Receiver = (*MessageReceiver)(nil)
)

type SessionReceiver struct {
	*azservicebus.SessionReceiver
}

func (s *SessionReceiver) ID() string {
	return s.SessionID()
}

func (s *SessionReceiver) RenewLocks(ctx context.Context, msg *azservicebus.ReceivedMessage) error {
	return s.RenewSessionLock(ctx, nil)
}

type MessageReceiver struct {
	*azservicebus.Receiver
}

func (m *MessageReceiver) ID() string {
	return ""
}

func (m *MessageReceiver) RenewLocks(ctx context.Context, msg *azservicebus.ReceivedMessage) error {
	return m.RenewMessageLock(ctx, msg, nil)
}

type MessageRenewFunc func(ctx context.Context, message *azservicebus.ReceivedMessage) error

// Subscription is an object that manages a subscription to an Azure Service Bus receiver, for a topic or queue.
type Subscription struct {
	entity               string
	mu                   sync.RWMutex
	activeMessages       map[int64]*azservicebus.ReceivedMessage
	activeOperationsChan chan struct{}
	receiver             *MessageReceiver
	sessionMu            sync.RWMutex
	sessionReceivers     map[string]*SessionReceiver
	sessionsEnabled      bool // read-only once set
	timeout              time.Duration
	maxBulkSubCount      int
	retriableErrLimit    ratelimit.Limiter
	handleChan           chan struct{}
	logger               logger.Logger
	ctx                  context.Context
	cancel               context.CancelFunc
}

// NewBulkSubscription returns a new Subscription object.
// Parameter "entity" is usually in the format "topic <topicname>" or "queue <queuename>" and it's only used for logging.
func NewSubscription(
	parentCtx context.Context,
	maxActiveMessages int,
	timeoutInSec int,
	maxBulkSubCount *int,
	maxRetriableEPS int,
	maxConcurrentHandlers int,
	entity string,
	lockRenewalInSec int,
	sessionsEnabled bool,
	logger logger.Logger,
) *Subscription {
	ctx, cancel := context.WithCancel(parentCtx)

	if maxBulkSubCount != nil {
		if *maxBulkSubCount < 1 {
			logger.Warnf("maxBulkSubCount must be greater than 0, setting it to 1")
			maxBulkSubCount = ptr.Of(1)
		}
	} else {
		// for non-bulk subscriptions, we only get one message at a time
		maxBulkSubCount = ptr.Of(1)
	}

	if *maxBulkSubCount > maxActiveMessages {
		logger.Warnf("maxBulkSubCount must not be greater than maxActiveMessages, setting it to %d", maxActiveMessages)
		maxBulkSubCount = &maxActiveMessages
	}

	s := &Subscription{
		entity:           entity,
		activeMessages:   make(map[int64]*azservicebus.ReceivedMessage),
		timeout:          time.Duration(timeoutInSec) * time.Second,
		maxBulkSubCount:  *maxBulkSubCount,
		sessionReceivers: make(map[string]*SessionReceiver),
		sessionsEnabled:  sessionsEnabled,
		logger:           logger,
		ctx:              ctx,
		cancel:           cancel,
		// This is a pessimistic estimate of the number of total operations that can be active at any given time.
		// In case of a non-bulk subscription, one operation is one message.
		activeOperationsChan: make(chan struct{}, maxActiveMessages/(*maxBulkSubCount)),
	}

	if maxRetriableEPS > 0 {
		s.retriableErrLimit = ratelimit.New(maxRetriableEPS)
	} else {
		s.retriableErrLimit = ratelimit.NewUnlimited()
	}

	if maxConcurrentHandlers > 0 {
		s.logger.Debugf("Subscription to %s is limited to %d message handler(s)", entity, maxConcurrentHandlers)
		s.handleChan = make(chan struct{}, maxConcurrentHandlers)
	}

	// Lock renewal loop.
	go func() {
		shouldRenewLocks := lockRenewalInSec > 0
		if !shouldRenewLocks {
			s.logger.Debugf("Lock renewal for %s disabled", s.entity)
			return
		}
		t := time.NewTicker(time.Second * time.Duration(lockRenewalInSec))
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Debugf("Lock renewal context for %s done", s.entity)
				return
			case <-t.C:
				s.tryRenewLocks()
			}
		}
	}()

	return s
}

// Connect to a Service Bus topic or queue, blocking until it succeeds; it can retry forever (until the context is canceled).
func (s *Subscription) Connect(newReceiverFunc func() (Receiver, error)) error {
	// Connections need to retry forever with a maximum backoff of 5 minutes and exponential scaling.
	config := retry.DefaultConfig()
	config.Policy = retry.PolicyExponential
	config.MaxInterval = 5 * time.Minute
	config.MaxElapsedTime = 0
	backoff := config.NewBackOffWithContext(s.ctx)

	err := retry.NotifyRecover(
		func() error {
			clientAttempt, innerErr := newReceiverFunc()
			if innerErr != nil {
				return innerErr
			}
			if s.sessionsEnabled {
				sessionReceiver, ok := clientAttempt.(*SessionReceiver)
				if !ok {
					return fmt.Errorf("expected a session receiver, got %T", clientAttempt)
				}
				s.sessionMu.Lock()
				_, exists := s.sessionReceivers[sessionReceiver.ID()]
				if exists {
					s.sessionMu.Unlock()
					return fmt.Errorf("session receiver %s already exists", sessionReceiver.ID())
				}
				s.sessionReceivers[sessionReceiver.ID()] = sessionReceiver
				s.sessionMu.Unlock()
			} else {
				msgReciever, ok := clientAttempt.(*MessageReceiver)
				if !ok {
					return fmt.Errorf("expected a message receiver, got %T", clientAttempt)
				}
				s.receiver = msgReciever
			}
			return nil
		},
		backoff,
		func(err error, d time.Duration) {
			s.logger.Warnf("Failed to connect to Azure Service Bus %s; will retry in %s. Error: %s", s.entity, d, err.Error())
		},
		func() {
			s.logger.Infof("Successfully reconnected to Azure Service Bus %s", s.entity)
		},
	)

	return err
}

type ReceiveOptions struct {
	BulkEnabled bool
}

// ReceiveAndBlock is a blocking call to receive messages on an Azure Service Bus subscription from a topic or queue.
func (s *Subscription) ReceiveAndBlock(handler HandlerFunc, onFirstSuccess func(), opts ReceiveOptions) error {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// Receiver loop
	for {
		select {
		case s.activeOperationsChan <- struct{}{}:
			// No-op
			// This blocks if there are too many active operations already
			// This is released by the handler, but if the loop ends before it reaches the handler, make sure to release it with `<-s.activeOperationsChan`
		case <-ctx.Done():
			// Return if context is canceled
			s.logger.Debugf("Receive context for %s done", s.entity)
			return ctx.Err()
		}

		recvCtx, recvCancel := context.WithTimeout(ctx, s.timeout)
		defer recvCancel()

		// This method blocks until we get a message or the context is canceled
		msgs, err := s.receiver.ReceiveMessages(recvCtx, s.maxBulkSubCount, nil)
		if err != nil {
			if err != context.Canceled {
				s.logger.Errorf("Error reading from %s. %s", s.entity, err.Error())
			}
			<-s.activeOperationsChan
			// Return the error. This will cause the Service Bus component to try and reconnect.
			return err
		}

		l := len(msgs)
		if l == 0 {
			// We got no message, which is unusual too
			s.logger.Warn("Received 0 messages from Service Bus")
			<-s.activeOperationsChan
			// Return an error to force the Service Bus component to try and reconnect.
			return errors.New("received 0 messages from Service Bus")
		} else if l > 1 {
			// We are requesting one message only; this should never happen
			s.logger.Errorf("Expected one message from Service Bus, but received %d", l)
		}

		// Invoke only once
		if onFirstSuccess != nil {
			onFirstSuccess()
			onFirstSuccess = nil
		}

		s.logger.Debugf("Received messages: %d; current active operations usage: %d/%d", l, len(s.activeOperationsChan), cap(s.activeOperationsChan))

		skipProcessing := false
		for _, msg := range msgs {
			if err = s.addActiveMessage(msg); err != nil {
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

		runHandlerFn := func(hctx context.Context) {
			msg := msgs[0]

			// Invoke the handler to process the message
			_, err = handler(hctx, msgs)

			// This context is used for the calls to service bus to finalize (i.e. complete/abandon) the message.
			// If we fail to finalize the message, this message will eventually be reprocessed (at-least once delivery).
			// This uses a background context in case ctx has been canceled already.
			finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), s.timeout)
			defer finalizeCancel()

			if err != nil {
				// Log the error only, as we're running asynchronously
				s.logger.Errorf("App handler returned an error for message %s on %s: %s", msg.MessageID, s.entity, err)
				s.AbandonMessage(finalizeCtx, msg)
				return
			}

			s.CompleteMessage(finalizeCtx, msg)
		}

		bulkRunHandlerFunc := func(hctx context.Context) {
			resps, err := handler(hctx, msgs)

			// This context is used for the calls to service bus to finalize (i.e. complete/abandon) the message.
			// If we fail to finalize the message, this message will eventually be reprocessed (at-least once delivery).
			// This uses a background context in case ctx has been canceled already.
			finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), s.timeout)
			defer finalizeCancel()

			if err != nil {
				// Handle the error and mark messages accordingly.
				// Note, the order of the responses match the order of the messages.
				for i, resp := range resps {
					if resp.Error != nil {
						// Log the error only, as we're running asynchronously.
						s.logger.Errorf("App handler returned an error for message %s on %s: %s", msgs[i].MessageID, s.entity, resp.Error)
						s.AbandonMessage(finalizeCtx, msgs[i])
					} else {
						s.CompleteMessage(finalizeCtx, msgs[i])
					}
				}
				return
			}

			// No error, so we can complete all messages.
			for _, msg := range msgs {
				s.CompleteMessage(finalizeCtx, msg)
			}
		}

		if opts.BulkEnabled {
			s.handleAsync(s.ctx, msgs, bulkRunHandlerFunc)
		} else {
			s.handleAsync(s.ctx, msgs, runHandlerFn)
		}
	}
}

// Close the receiver and stops watching for new messages.
func (s *Subscription) Close(closeCtx context.Context) {
	s.logger.Debugf("Closing subscription to %s", s.entity)

	s.sessionMu.Lock()
	for _, session := range s.sessionReceivers {
		if err := session.Close(closeCtx); err != nil {
			s.logger.Warnf("Error closing session receiver: %s", err.Error())
		}
	}
	s.sessionMu.Unlock()

	// Ensure subscription entity is closed.
	if err := s.receiver.Close(closeCtx); err != nil {
		s.logger.Warnf("Error closing subscription for %s: %+v", s.entity, err)
	}
	s.cancel()
}

// handleAsync handles messages from azure service bus asynchronously.
// runHandlerFn is responsible for calling the message handler function
// and marking messages as complete/abandon.
func (s *Subscription) handleAsync(ctx context.Context, msgs []*azservicebus.ReceivedMessage, runHandlerFn func(ctx context.Context)) {
	go func() {
		var (
			consumeToken           bool
			takenConcurrentHandler bool
		)

		defer func() {
			for _, msg := range msgs {
				// Release a handler if needed
				if takenConcurrentHandler {
					<-s.handleChan
					s.logger.Debugf("Released message handle for %s on %s", msg.MessageID, s.entity)
				}

				// If we got a retriable error (app handler returned a retriable error, or a network error while connecting to the app, etc) consume a retriable error token
				// We do it here, after the handler has been released but before removing the active message (which would allow us to retrieve more messages)
				if consumeToken {
					s.logger.Debugf("Taking a retriable error token")
					before := time.Now()
					_ = s.retriableErrLimit.Take()
					s.logger.Debugf("Resumed after pausing for %v", time.Since(before))
				}

				// Remove the message from the map of active ones
				s.removeActiveMessage(msg.MessageID, *msg.SequenceNumber)
			}

			// Remove an entry from activeOperationsChan to allow processing more messages
			<-s.activeOperationsChan
		}()

		for _, msg := range msgs {
			// If handleChan is non-nil, we have a limit on how many handler we can process
			if cap(s.handleChan) > 0 {
				s.logger.Debugf("Taking message handle for %s on %s", msg.MessageID, s.entity)
				select {
				// Context is done, so we will stop waiting
				case <-ctx.Done():
					s.logger.Debugf("Message context done for %s on %s", msg.MessageID, s.entity)
					return
				// Blocks until we have a handler available
				case s.handleChan <- struct{}{}:
					takenConcurrentHandler = true
					s.logger.Debugf("Taken message handle for %s on %s", msg.MessageID, s.entity)
				}
			}
		}

		// Invoke the handler to process the message.
		runHandlerFn(ctx)
	}()
}

func (s *Subscription) tryRenewLocks() {
	if s.sessionsEnabled {
		s.tryRenewSessionLocks()
	} else {
		s.tryRenewMessageLocks()
	}
}

func (s *Subscription) tryRenewSessionLocks() {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if len(s.sessionReceivers) == 0 {
		return
	}

	var ctx, cancel = context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	for _, session := range s.sessionReceivers {
		session.RenewLocks(ctx, &azservicebus.ReceivedMessage{}) // mesage not used.
	}
}

func (s *Subscription) tryRenewMessageLocks() {
	if s.receiver == nil {
		return
	}

	// Snapshot the messages to try to renew locks for.
	msgs := make([]*azservicebus.ReceivedMessage, 0)
	s.mu.RLock()
	for _, m := range s.activeMessages {
		msgs = append(msgs, m)
	}
	s.mu.RUnlock()
	if len(msgs) == 0 {
		s.logger.Debugf("No active messages require lock renewal for %s", s.entity)
		return
	}

	// Lock renewal is best effort and not guaranteed to succeed, warnings are expected.
	s.logger.Debugf("Trying to renew %d active message lock(s) for %s", len(msgs), s.entity)
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	for _, msg := range msgs {
		ctx, cancel = context.WithTimeout(context.Background(), s.timeout)

		// Renew the lock for the message.
		err = s.receiver.RenewLocks(ctx, msg)

		if err != nil {
			s.logger.Debugf("Couldn't renew all active message lock(s) for %s, ", s.entity, err)
		}
		cancel()
	}
}

// AbandonMessage marks a messsage as abandoned.
func (s *Subscription) AbandonMessage(ctx context.Context, m *azservicebus.ReceivedMessage) {
	s.logger.Debugf("Abandoning message %s on %s", m.MessageID, s.entity)

	// Use a background context in case a.ctx has been canceled already
	err := s.receiver.AbandonMessage(ctx, m, nil)
	if err != nil {
		// Log only
		s.logger.Warnf("Error abandoning message %s on %s: %s", m.MessageID, s.entity, err.Error())
	}

	// If we're here, it means we got a retriable error, so we need to consume a retriable error token before this (synchronous) method returns
	// If there have been too many retriable errors per second, this method slows the consumer down
	s.logger.Debugf("Taking a retriable error token")
	before := time.Now()
	_ = s.retriableErrLimit.Take()
	s.logger.Debugf("Resumed after pausing for %v", time.Since(before))
}

// CompleteMessage marks a message as complete.
func (s *Subscription) CompleteMessage(ctx context.Context, m *azservicebus.ReceivedMessage) {
	s.logger.Debugf("Completing message %s on %s", m.MessageID, s.entity)

	// Use a background context in case a.ctx has been canceled already
	err := s.receiver.CompleteMessage(ctx, m, nil)
	if err != nil {
		// Log only
		s.logger.Warnf("Error completing message %s on %s: %s", m.MessageID, s.entity, err.Error())
	}
}

func (s *Subscription) addActiveMessage(m *azservicebus.ReceivedMessage) error {
	if m.SequenceNumber == nil {
		return fmt.Errorf("message sequence number is nil")
	}
	s.logger.Debugf("Adding message %s with sequence number %d to active messages on %s", m.MessageID, *m.SequenceNumber, s.entity)
	s.mu.Lock()
	s.activeMessages[*m.SequenceNumber] = m
	s.mu.Unlock()
	return nil
}

func (s *Subscription) removeActiveMessage(messageID string, messageKey int64) {
	s.logger.Debugf("Removing message %s with sequence number %d from active messages on %s", messageID, messageKey, s.entity)
	s.mu.Lock()
	delete(s.activeMessages, messageKey)
	s.mu.Unlock()
}
