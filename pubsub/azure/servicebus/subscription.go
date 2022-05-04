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

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type subscription struct {
	topic                   string
	mu                      sync.RWMutex
	activeMessages          map[string]*azservicebus.ReceivedMessage
	receiver                *azservicebus.Receiver
	limitConcurrentHandlers bool
	handleChan              chan handle
	logger                  logger.Logger
	prefetchChan            chan *azservicebus.ReceivedMessage
	ctx                     context.Context
	cancel                  context.CancelFunc
}

func newSubscription(parentCtx context.Context, topic string, receiver *azservicebus.Receiver, maxConcurrentHandlers *int, prefetchCount *int, logger logger.Logger) *subscription {
	ctx, cancel := context.WithCancel(parentCtx)
	s := &subscription{
		topic:          topic,
		activeMessages: make(map[string]*azservicebus.ReceivedMessage),
		receiver:       receiver,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
	}

	if maxConcurrentHandlers != nil {
		s.logger.Debugf("Subscription to topic %s is limited to %d message handler(s)", topic, *maxConcurrentHandlers)
		s.limitConcurrentHandlers = true
		s.handleChan = make(chan handle, *maxConcurrentHandlers)
		for i := 0; i < *maxConcurrentHandlers; i++ {
			s.handleChan <- handle{}
		}
	}

	if prefetchCount != nil {
		s.logger.Debugf("Subscription to topic %s will prefetch %d message(s)", topic, *prefetchCount)
		s.prefetchChan = make(chan *azservicebus.ReceivedMessage, *prefetchCount)
	} else {
		s.prefetchChan = make(chan *azservicebus.ReceivedMessage, 1)
	}

	return s
}

// ReceiveAndBlock is a blocking call to receive messages on an Azure Service Bus subscription from a topic.
func (s *subscription) ReceiveAndBlock(handler pubsub.Handler, lockRenewalInSec int, handlerTimeoutInSec int, timeoutInSec int, maxActiveMessages int, maxActiveMessagesRecoveryInSec int) error {
	// Lock renewal loop.
	go func() {
		shouldRenewLocks := lockRenewalInSec > 0
		if !shouldRenewLocks {
			s.logger.Debugf("Lock renewal for topic %s disabled", s.topic)
			return
		}
		for {
			select {
			case <-s.ctx.Done():
				s.logger.Debugf("Lock renewal context for topic %s done", s.topic)
				return
			case <-time.After(time.Second * time.Duration(lockRenewalInSec)):
				s.tryRenewLocks()
			}
		}
	}()

	// Prefetch loop.
	go func() {
		for {
			msgs, err := s.receiver.ReceiveMessages(s.ctx, 1, nil)
			if err != nil {
				s.logger.Errorf("Error reading from topic %s. %s", s.topic, err.Error())
			}
			for _, msg := range msgs {
				s.logger.Infof("Prefetch received msg: %s", msg.MessageID)
				s.prefetchChan <- msg
			}
		}
	}()

	asyncHandler := s.asyncWrapper(s.getHandlerFunc(handler, handlerTimeoutInSec, timeoutInSec))

	// Receiver loop.
	for {
		s.mu.RLock()
		activeMessageLen := len(s.activeMessages)
		s.mu.RUnlock()
		if activeMessageLen >= maxActiveMessages {
			// Max active messages reached, sleep to allow the current active messages to be processed before getting more.
			s.logger.Debugf("Max active messages %d reached for topic %s, recovering for %d seconds", maxActiveMessages, s.topic, maxActiveMessagesRecoveryInSec)

			select {
			case <-s.ctx.Done():
				s.logger.Debugf("Receive context for topic %s done", s.topic)
				return s.ctx.Err()
			case <-time.After(time.Second * time.Duration(maxActiveMessagesRecoveryInSec)):
				continue
			}
		}

		if err := s.receiveMessage(s.ctx, asyncHandler); err != nil {
			return err
		}
	}
}

func (s *subscription) close(ctx context.Context) {
	s.logger.Debugf("Closing subscription to topic %s", s.topic)

	// Ensure subscription entity is closed.
	if err := s.receiver.Close(ctx); err != nil {
		s.logger.Warnf("%s closing subscription entity for topic %s: %+v", errorMessagePrefix, s.topic, err)
	}
}

func (s *subscription) getHandlerFunc(handler pubsub.Handler, handlerTimeoutInSec int, timeoutInSec int) func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) error {
	return func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) error {
		pubsubMsg, err := NewPubsubMessageFromASBMessage(asbMsg, s.topic)
		if err != nil {
			return fmt.Errorf("failed to get pubsub message from azure service bus message: %+v", err)
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, time.Second*time.Duration(handlerTimeoutInSec))
		defer handleCancel()
		s.logger.Debugf("Calling app's handler for message %s on topic %s", asbMsg.MessageID, s.topic)
		appErr := handler(handleCtx, pubsubMsg)

		// This context is used for the calls to service bus to finalize (i.e. complete/abandon) the message.
		// If we fail to finalize the message, this message will eventually be reprocessed (at-least once delivery).
		finalizeCtx, finalizeCancel := context.WithTimeout(ctx, time.Second*time.Duration(timeoutInSec))
		defer finalizeCancel()

		if appErr != nil {
			s.logger.Warnf("Error in app's handler: %+v", appErr)
			if abandonErr := s.abandonMessage(finalizeCtx, asbMsg); abandonErr != nil {
				return fmt.Errorf("failed to abandon: %+v", abandonErr)
			}

			return nil
		}
		if completeErr := s.completeMessage(finalizeCtx, asbMsg); completeErr != nil {
			return fmt.Errorf("failed to complete: %+v", completeErr)
		}

		return nil
	}
}

func (s *subscription) asyncWrapper(handlerFunc func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) error) func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) error {
	return func(ctx context.Context, msg *azservicebus.ReceivedMessage) error {
		go func() {
			s.addActiveMessage(msg)
			defer s.removeActiveMessage(msg.MessageID)

			if s.limitConcurrentHandlers {
				s.logger.Debugf("Taking message handle for %s on topic %s", msg.MessageID, s.topic)
				select {
				case <-ctx.Done():
					s.logger.Debugf("Message context done for %s on topic %s", msg.MessageID, s.topic)

					return
				case <-s.handleChan: // Take or wait on a free handle before getting a new message.
					s.logger.Debugf("Taken message handle for %s on topic %s", msg.MessageID, s.topic)
				}

				defer func() {
					s.logger.Debugf("Releasing message handle for %s on topic %s", msg.MessageID, s.topic)
					s.handleChan <- handle{} // Release a handle when complete.
					s.logger.Debugf("Released message handle for %s on topic %s", msg.MessageID, s.topic)
				}()
			}

			err := handlerFunc(ctx, msg)
			if err != nil {
				s.logger.Errorf("%s error handling message %s on topic '%s', %s", errorMessagePrefix, msg.MessageID, s.topic, err)
			}
		}()

		return nil
	}
}

func (s *subscription) tryRenewLocks() {
	s.mu.RLock()
	activeMessageLen := len(s.activeMessages)
	s.mu.RUnlock()
	if activeMessageLen == 0 {
		s.logger.Debugf("No active messages require lock renewal for topic %s", s.topic)
		return
	}

	// Snapshot the messages to try to renew locks for.
	msgs := make([]*azservicebus.ReceivedMessage, 0)
	s.mu.RLock()
	for _, m := range s.activeMessages {
		msgs = append(msgs, m)
	}
	s.mu.RUnlock()

	// Lock renewal is best effort and not guaranteed to succeed, warnings are expected.
	s.logger.Debugf("Trying to renew %d active message lock(s) for topic %s", len(msgs), s.topic)
	for _, msg := range msgs {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := s.receiver.RenewMessageLock(ctx, msg, nil)
		if err != nil {
			s.logger.Debugf("Couldn't renew all active message lock(s) for topic %s, ", s.topic, err)
		}
		cancel()
	}
}

func (s *subscription) receiveMessage(ctx context.Context, handler func(ctx context.Context, asbMsg *azservicebus.ReceivedMessage) error) error {
	s.logger.Debugf("Waiting to receive message on topic %s", s.topic)
	msg := <-s.prefetchChan

	s.logger.Infof("Process received message: %s", msg.MessageID)
	body, _ := msg.Body()
	s.logger.Infof("Message body: %s", string(body))

	err := handler(ctx, msg)
	if err != nil {
		return fmt.Errorf("%s error processing message on topic %s, %w", errorMessagePrefix, s.topic, err)
	}

	return nil
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
