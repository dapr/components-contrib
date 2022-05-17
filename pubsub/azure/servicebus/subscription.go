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
	"fmt"
	"sync"
	"time"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.uber.org/ratelimit"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type subscription struct {
	topic              string
	mu                 sync.RWMutex
	activeMessages     map[string]*azservicebus.ReceivedMessage
	activeMessagesChan chan struct{}
	receiver           *azservicebus.Receiver
	timeout            time.Duration
	handlerTimeout     time.Duration
	retriableErrLimit  ratelimit.Limiter
	handleChan         chan struct{}
	logger             logger.Logger
	ctx                context.Context
	cancel             context.CancelFunc
}

func newSubscription(
	parentCtx context.Context,
	topic string,
	receiver *azservicebus.Receiver,
	maxActiveMessages int,
	timeoutInSec int,
	handlerTimeoutInSec int,
	maxRetriableEPS int,
	maxConcurrentHandlers *int,
	logger logger.Logger,
) *subscription {
	ctx, cancel := context.WithCancel(parentCtx)
	s := &subscription{
		topic:              topic,
		activeMessages:     make(map[string]*azservicebus.ReceivedMessage),
		activeMessagesChan: make(chan struct{}, maxActiveMessages),
		receiver:           receiver,
		timeout:            time.Duration(timeoutInSec) * time.Second,
		handlerTimeout:     time.Duration(handlerTimeoutInSec) * time.Second,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
	}

	if maxRetriableEPS > 1 {
		s.retriableErrLimit = ratelimit.New(maxRetriableEPS)
	} else {
		s.retriableErrLimit = ratelimit.NewUnlimited()
	}

	if maxConcurrentHandlers != nil {
		s.logger.Debugf("Subscription to topic %s is limited to %d message handler(s)", topic, *maxConcurrentHandlers)
		s.handleChan = make(chan struct{}, *maxConcurrentHandlers)
	}

	return s
}

// ReceiveAndBlock is a blocking call to receive messages on an Azure Service Bus subscription from a topic.
func (s *subscription) ReceiveAndBlock(handler pubsub.Handler, lockRenewalInSec int) error {
	// Lock renewal loop.
	go func() {
		shouldRenewLocks := lockRenewalInSec > 0
		if !shouldRenewLocks {
			s.logger.Debugf("Lock renewal for topic %s disabled", s.topic)
			return
		}
		t := time.NewTicker(time.Second * time.Duration(lockRenewalInSec))
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				s.logger.Debugf("Lock renewal context for topic %s done", s.topic)
				return
			case <-t.C:
				s.tryRenewLocks()
			}
		}
	}()

	handlerFunc := s.getHandlerFunc(handler)

	// Receiver loop
	for {
		select {
		// This blocks if there are too many active messages already
		case s.activeMessagesChan <- struct{}{}:
		// Return if context is canceled
		case <-s.ctx.Done():
			s.logger.Debugf("Receive context for topic %s done", s.topic)
			return s.ctx.Err()
		}

		// This method blocks until we get a message or the context is canceled
		msgs, err := s.receiver.ReceiveMessages(s.ctx, 1, nil)
		if err != nil {
			s.logger.Errorf("Error reading from topic %s. %s", s.topic, err.Error())
			continue
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
		s.handleAsync(s.ctx, msg, handlerFunc)
	}
}

func (s *subscription) close(ctx context.Context) {
	s.logger.Debugf("Closing subscription to topic %s", s.topic)

	// Ensure subscription entity is closed.
	if err := s.receiver.Close(ctx); err != nil {
		s.logger.Warnf("%s closing subscription entity for topic %s: %+v", errorMessagePrefix, s.topic, err)
	}
}

func (s *subscription) getHandlerFunc(handler pubsub.Handler) func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) (consumeToken bool, err error) {
	handlerTimeout := s.handlerTimeout
	timeout := s.timeout
	return func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) (consumeToken bool, err error) {
		pubsubMsg, err := NewPubsubMessageFromASBMessage(asbMsg, s.topic)
		if err != nil {
			return false, fmt.Errorf("failed to get pubsub message from azure service bus message: %+v", err)
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, handlerTimeout)
		defer handleCancel()
		s.logger.Debugf("Calling app's handler for message %s on topic %s", asbMsg.MessageID, s.topic)
		appErr := handler(handleCtx, pubsubMsg)

		// This context is used for the calls to service bus to finalize (i.e. complete/abandon) the message.
		// If we fail to finalize the message, this message will eventually be reprocessed (at-least once delivery).
		finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), timeout)
		defer finalizeCancel()

		if appErr != nil {
			s.logger.Warnf("Error in app's handler: %+v", appErr)
			if abandonErr := s.abandonMessage(finalizeCtx, asbMsg); abandonErr != nil {
				return true, fmt.Errorf("failed to abandon: %+v", abandonErr)
			}

			return true, nil
		}
		if completeErr := s.completeMessage(finalizeCtx, asbMsg); completeErr != nil {
			return false, fmt.Errorf("failed to complete: %+v", completeErr)
		}

		return false, nil
	}
}

func (s *subscription) handleAsync(ctx context.Context, msg *azservicebus.ReceivedMessage, handlerFunc func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) (consumeToken bool, err error)) {
	go func() {
		var consumeToken bool
		var err error

		// If handleChan is non-nil, we have a limit on how many handler we can process
		limitConcurrentHandlers := cap(s.handleChan) > 0
		defer func() {
			// Release a handler if needed
			if limitConcurrentHandlers {
				<-s.handleChan
				s.logger.Debugf("Released message handle for %s on topic %s", msg.MessageID, s.topic)
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
			s.logger.Debugf("Taking message handle for %s on topic %s", msg.MessageID, s.topic)
			select {
			// Context is done, so we will stop waiting
			case <-ctx.Done():
				s.logger.Debugf("Message context done for %s on topic %s", msg.MessageID, s.topic)
				return
			// Blocks until we have a handler available
			case s.handleChan <- struct{}{}:
				s.logger.Debugf("Taken message handle for %s on topic %s", msg.MessageID, s.topic)
			}
		}

		consumeToken, err = handlerFunc(ctx, msg)
		if err != nil {
			// Log the error only, as we're running asynchronously
			s.logger.Errorf("%s error handling message %s on topic '%s', %s", errorMessagePrefix, msg.MessageID, s.topic, err)
		}
	}()
}

func (s *subscription) tryRenewLocks() {
	// Snapshot the messages to try to renew locks for.
	msgs := make([]*azservicebus.ReceivedMessage, 0)
	s.mu.RLock()
	for _, m := range s.activeMessages {
		msgs = append(msgs, m)
	}
	s.mu.RUnlock()
	if len(msgs) == 0 {
		s.logger.Debugf("No active messages require lock renewal for topic %s", s.topic)
		return
	}

	// Lock renewal is best effort and not guaranteed to succeed, warnings are expected.
	s.logger.Debugf("Trying to renew %d active message lock(s) for topic %s", len(msgs), s.topic)
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	for _, msg := range msgs {
		ctx, cancel = context.WithTimeout(context.Background(), s.timeout)
		err = s.receiver.RenewMessageLock(ctx, msg, nil)
		if err != nil {
			s.logger.Debugf("Couldn't renew all active message lock(s) for topic %s, ", s.topic, err)
		}
		cancel()
	}
}

func (s *subscription) abandonMessage(ctx context.Context, m *azservicebus.ReceivedMessage) error {
	s.logger.Debugf("Abandoning message %s on topic %s", m.MessageID, s.topic)
	return s.receiver.AbandonMessage(ctx, m, nil)
}

func (s *subscription) completeMessage(ctx context.Context, m *azservicebus.ReceivedMessage) error {
	s.logger.Debugf("Completing message %s on topic %s", m.MessageID, s.topic)
	return s.receiver.CompleteMessage(ctx, m, nil)
}

func (s *subscription) addActiveMessage(m *azservicebus.ReceivedMessage) {
	s.logger.Debugf("Adding message %s to active messages on topic %s", m.MessageID, s.topic)
	s.mu.Lock()
	s.activeMessages[m.MessageID] = m
	s.mu.Unlock()
}

func (s *subscription) removeActiveMessage(messageID string) {
	s.logger.Debugf("Removing message %s from active messages on topic %s", messageID, s.topic)
	s.mu.Lock()
	delete(s.activeMessages, messageID)
	s.mu.Unlock()
}
