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
	"sync"
	"time"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.uber.org/ratelimit"

	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// HandlerFunc is the type for handlers that receive messages
type HandlerFunc func(ctx context.Context, msg *azservicebus.ReceivedMessage) error

// Subscription is an object that manages a subscription to an Azure Service Bus receiver, for a topic or queue.
type Subscription struct {
	entity             string
	mu                 sync.RWMutex
	activeMessages     map[string]*azservicebus.ReceivedMessage
	activeMessagesChan chan struct{}
	receiver           *azservicebus.Receiver
	timeout            time.Duration
	retriableErrLimit  ratelimit.Limiter
	handleChan         chan struct{}
	logger             logger.Logger
	ctx                context.Context
	cancel             context.CancelFunc
}

// NewSubscription returns a new Subscription object.
// Parameter "entity" is usually in the format "topic <topicname>" or "queue <queuename>" and it's only used for logging.
func NewSubscription(
	parentCtx context.Context,
	maxActiveMessages int,
	timeoutInSec int,
	maxRetriableEPS int,
	maxConcurrentHandlers *int,
	entity string,
	logger logger.Logger,
) *Subscription {
	ctx, cancel := context.WithCancel(parentCtx)
	s := &Subscription{
		entity:             entity,
		activeMessages:     make(map[string]*azservicebus.ReceivedMessage),
		activeMessagesChan: make(chan struct{}, maxActiveMessages),
		timeout:            time.Duration(timeoutInSec) * time.Second,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
	}

	if maxRetriableEPS > 0 {
		s.retriableErrLimit = ratelimit.New(maxRetriableEPS)
	} else {
		s.retriableErrLimit = ratelimit.NewUnlimited()
	}

	if maxConcurrentHandlers != nil {
		s.logger.Debugf("Subscription to %s is limited to %d message handler(s)", entity, *maxConcurrentHandlers)
		s.handleChan = make(chan struct{}, *maxConcurrentHandlers)
	}

	return s
}

// Connect to a Service Bus topic or queue, blocking until it succeeds; it can retry forever (until the context is canceled).
func (s *Subscription) Connect(newReceiverFunc func() (*azservicebus.Receiver, error)) (err error) {
	// Connections need to retry forever with a maximum backoff of 5 minutes and exponential scaling.
	config := retry.DefaultConfig()
	config.Policy = retry.PolicyExponential
	config.MaxInterval = 5 * time.Minute
	config.MaxElapsedTime = 0
	backoff := config.NewBackOffWithContext(s.ctx)

	err = retry.NotifyRecover(
		func() error {
			clientAttempt, innerErr := newReceiverFunc()
			if innerErr != nil {
				return innerErr
			}
			s.receiver = clientAttempt
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

// ReceiveAndBlock is a blocking call to receive messages on an Azure Service Bus subscription from a topic or queue.
func (s *Subscription) ReceiveAndBlock(handler HandlerFunc, lockRenewalInSec int, onFirstSuccess func()) error {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

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

	// Receiver loop
	for {
		select {
		// This blocks if there are too many active messages already
		case s.activeMessagesChan <- struct{}{}:
		// Return if context is canceled
		case <-ctx.Done():
			s.logger.Debugf("Receive context for %s done", s.entity)
			return ctx.Err()
		}

		// This method blocks until we get a message or the context is canceled
		msgs, err := s.receiver.ReceiveMessages(s.ctx, 1, nil)
		if err != nil {
			if err != context.Canceled {
				s.logger.Errorf("Error reading from %s. %s", s.entity, err.Error())
			}
			// Return the error. This will cause the Service Bus component to try and reconnect.
			return err
		}

		// Invoke only once
		if onFirstSuccess != nil {
			onFirstSuccess()
			onFirstSuccess = nil
		}

		l := len(msgs)
		if l == 0 {
			// We got no message, which is unusual too
			s.logger.Warn("Received 0 messages from Service Bus")
			continue
		} else if l > 1 {
			// We are requesting one message only; this should never happen
			s.logger.Errorf("Expected one message from Service Bus, but received %d", l)
		}

		msg := msgs[0]
		s.logger.Debugf("Received message: %s; current active message usage: %d/%d", msg.MessageID, len(s.activeMessagesChan), cap(s.activeMessagesChan))
		// body, _ := msg.Body()
		// s.logger.Debugf("Message body: %s", string(body))

		s.addActiveMessage(msg)

		s.logger.Debugf("Processing received message: %s", msg.MessageID)
		s.handleAsync(s.ctx, msg, handler)
	}
}

// Close the receiver and stops watching for new messages.
func (s *Subscription) Close(closeCtx context.Context) {
	s.logger.Debugf("Closing subscription to %s", s.entity)

	// Ensure subscription entity is closed.
	if err := s.receiver.Close(closeCtx); err != nil {
		s.logger.Warnf("Error closing subscription for %s: %+v", s.entity, err)
	}
	s.cancel()
}

func (s *Subscription) handleAsync(ctx context.Context, msg *azservicebus.ReceivedMessage, handler HandlerFunc) {
	go func() {
		var consumeToken bool
		var err error

		// If handleChan is non-nil, we have a limit on how many handler we can process
		limitConcurrentHandlers := cap(s.handleChan) > 0
		defer func() {
			// Release a handler if needed
			if limitConcurrentHandlers {
				<-s.handleChan
				s.logger.Debugf("Released message handle for %s on %s", msg.MessageID, s.entity)
			}

			// If we got a retriable error (app handler returned a retriable error, or a network error while connecting to the app, etc) consume a retriable error token
			// We do it here, after the handler has been released but before removing the active message (which would allow us to retrieve more messages)
			if consumeToken {
				s.logger.Debugf("Taking a retriable error token")
				before := time.Now()
				_ = s.retriableErrLimit.Take()
				s.logger.Debugf("Resumed after pausing for %v", time.Now().Sub(before))
			}

			// Remove the message from the map of active ones
			s.removeActiveMessage(msg.MessageID)

			// Remove an entry from activeMessageChan to allow processing more messages
			<-s.activeMessagesChan
		}()

		if limitConcurrentHandlers {
			s.logger.Debugf("Taking message handle for %s on %s", msg.MessageID, s.entity)
			select {
			// Context is done, so we will stop waiting
			case <-ctx.Done():
				s.logger.Debugf("Message context done for %s on %s", msg.MessageID, s.entity)
				return
			// Blocks until we have a handler available
			case s.handleChan <- struct{}{}:
				s.logger.Debugf("Taken message handle for %s on %s", msg.MessageID, s.entity)
			}
		}

		// Invoke the handler to process the message
		err = handler(ctx, msg)

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
	}()
}

func (s *Subscription) tryRenewLocks() {
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
		err = s.receiver.RenewMessageLock(ctx, msg, nil)
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
	s.logger.Debugf("Resumed after pausing for %v", time.Now().Sub(before))
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

func (s *Subscription) addActiveMessage(m *azservicebus.ReceivedMessage) {
	s.logger.Debugf("Adding message %s to active messages on %s", m.MessageID, s.entity)
	s.mu.Lock()
	s.activeMessages[m.MessageID] = m
	s.mu.Unlock()
}

func (s *Subscription) removeActiveMessage(messageID string) {
	s.logger.Debugf("Removing message %s from active messages on %s", messageID, s.entity)
	s.mu.Lock()
	delete(s.activeMessages, messageID)
	s.mu.Unlock()
}
