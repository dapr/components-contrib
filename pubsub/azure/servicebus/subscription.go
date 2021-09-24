package servicebus

import (
	"context"
	"fmt"
	"sync"
	"time"

	azservicebus "github.com/Azure/azure-service-bus-go"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type subscription struct {
	topic                   string
	mu                      sync.RWMutex
	activeMessages          map[string]*azservicebus.Message
	entity                  *azservicebus.Subscription
	limitConcurrentHandlers bool
	handleChan              chan handle
	logger                  logger.Logger
}

func newSubscription(topic string, sub *azservicebus.Subscription, maxConcurrentHandlers *int, logger logger.Logger) *subscription {
	s := &subscription{
		topic:          topic,
		activeMessages: make(map[string]*azservicebus.Message),
		entity:         sub,
		logger:         logger,
	}

	if maxConcurrentHandlers != nil {
		s.logger.Debugf("Subscription to topic %s is limited to %d message handler(s)", topic, *maxConcurrentHandlers)
		s.limitConcurrentHandlers = true
		s.handleChan = make(chan handle, *maxConcurrentHandlers)
		for i := 0; i < *maxConcurrentHandlers; i++ {
			s.handleChan <- handle{}
		}
	}

	return s
}

// ReceiveAndBlock is a blocking call to receive messages on an Azure Service Bus subscription from a topic.
func (s *subscription) ReceiveAndBlock(ctx context.Context, handler pubsub.Handler, lockRenewalInSec int, handlerTimeoutInSec int, timeoutInSec int, maxActiveMessages int, maxActiveMessagesRecoveryInSec int) error {
	// Close subscription
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeoutInSec))
		defer closeCancel()
		s.close(closeCtx)
	}()

	// Lock renewal loop
	go func() {
		shouldRenewLocks := lockRenewalInSec > 0
		if !shouldRenewLocks {
			s.logger.Debugf("Lock renewal for topic %s disabled", s.topic)

			return
		}
		for {
			select {
			case <-ctx.Done():
				s.logger.Debugf("Lock renewal context for topic %s done", s.topic)

				return
			case <-time.After(time.Second * time.Duration(lockRenewalInSec)):
				s.tryRenewLocks()
			}
		}
	}()

	asyncHandler := s.asyncWrapper(s.getHandlerFunc(handler, handlerTimeoutInSec, timeoutInSec))

	// Receiver loop
	for {
		s.mu.RLock()
		activeMessageLen := len(s.activeMessages)
		s.mu.RUnlock()
		if activeMessageLen >= maxActiveMessages {
			// Max active messages reached, sleep to allow the current active messages to be processed before getting more
			s.logger.Debugf("Max active messages %d reached for topic %s, recovering for %d seconds", maxActiveMessages, s.topic, maxActiveMessagesRecoveryInSec)

			select {
			case <-ctx.Done():
				s.logger.Debugf("Receive context for topic %s done", s.topic)

				return ctx.Err()
			case <-time.After(time.Second * time.Duration(maxActiveMessagesRecoveryInSec)):
				continue
			}
		}

		if err := s.receiveMessage(ctx, asyncHandler); err != nil {
			return err
		}
	}
}

func (s *subscription) close(ctx context.Context) {
	s.logger.Debugf("Closing subscription to topic %s", s.topic)

	// Ensure subscription entity is closed
	if err := s.entity.Close(ctx); err != nil {
		s.logger.Warnf("%s closing subscription entity for topic %s: %+v", errorMessagePrefix, s.topic, err)
	}
}

func (s *subscription) getHandlerFunc(handler pubsub.Handler, handlerTimeoutInSec int, timeoutInSec int) azservicebus.HandlerFunc {
	return func(ctx context.Context, message *azservicebus.Message) error {
		msg := &pubsub.NewMessage{
			Data:  message.Data,
			Topic: s.topic,
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, time.Second*time.Duration(handlerTimeoutInSec))
		defer handleCancel()
		s.logger.Debugf("Calling app's handler for message %s on topic %s", message.ID, s.topic)
		appErr := handler(handleCtx, msg)

		// This context is used for the calls to service bus to finalize (i.e. complete/abandon) the message.
		// If we fail to finalize the message, this message will eventually be reprocessed (at-least once delivery).
		finalizeCtx, finalizeCancel := context.WithTimeout(ctx, time.Second*time.Duration(timeoutInSec))
		defer finalizeCancel()

		if appErr != nil {
			s.logger.Warnf("Error in app's handler: %+v", appErr)
			if abandonErr := s.abandonMessage(finalizeCtx, message); abandonErr != nil {
				return fmt.Errorf("failed to abandon: %+v", abandonErr)
			}

			return nil
		}
		if completeErr := s.completeMessage(finalizeCtx, message); completeErr != nil {
			return fmt.Errorf("failed to complete: %+v", completeErr)
		}

		return nil
	}
}

func (s *subscription) asyncWrapper(handlerFunc azservicebus.HandlerFunc) azservicebus.HandlerFunc {
	return func(ctx context.Context, msg *azservicebus.Message) error {
		go func() {
			s.addActiveMessage(msg)
			defer s.removeActiveMessage(msg.ID)

			if s.limitConcurrentHandlers {
				s.logger.Debugf("Taking message handle for %s on topic %s", msg.ID, s.topic)
				select {
				case <-ctx.Done():
					s.logger.Debugf("Message context done for %s on topic %s", msg.ID, s.topic)

					return
				case <-s.handleChan: // Take or wait on a free handle before getting a new message
					s.logger.Debugf("Taken message handle for %s on topic %s", msg.ID, s.topic)
				}

				defer func() {
					s.logger.Debugf("Releasing message handle for %s on topic %s", msg.ID, s.topic)
					s.handleChan <- handle{} // Release a handle when complete
					s.logger.Debugf("Released message handle for %s on topic %s", msg.ID, s.topic)
				}()
			}

			err := handlerFunc(ctx, msg)
			if err != nil {
				s.logger.Errorf("%s error handling message %s on topic '%s', %s", errorMessagePrefix, msg.ID, s.topic, err)
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

	// Snapshot the messages to try to renew locks for
	msgs := make([]*azservicebus.Message, 0)
	s.mu.RLock()
	for _, m := range s.activeMessages {
		msgs = append(msgs, m)
	}
	s.mu.RUnlock()

	// Lock renewal is best effort and not guaranteed to succeed, warnings are expected.
	s.logger.Debugf("Trying to renew %d active message lock(s) for topic %s", len(msgs), s.topic)
	err := s.entity.RenewLocks(context.Background(), msgs...)
	if err != nil {
		s.logger.Debugf("Couldn't renew all active message lock(s) for topic %s, ", s.topic, err)
	}
}

func (s *subscription) receiveMessage(ctx context.Context, handler azservicebus.HandlerFunc) error {
	s.logger.Debugf("Waiting to receive message on topic %s", s.topic)
	if err := s.entity.ReceiveOne(ctx, handler); err != nil {
		return fmt.Errorf("%s error receiving message on topic %s, %w", errorMessagePrefix, s.topic, err)
	}

	return nil
}

func (s *subscription) abandonMessage(ctx context.Context, m *azservicebus.Message) error {
	s.logger.Debugf("Abandoning message %s on topic %s", m.ID, s.topic)

	return m.Abandon(ctx)
}

func (s *subscription) completeMessage(ctx context.Context, m *azservicebus.Message) error {
	s.logger.Debugf("Completing message %s on topic %s", m.ID, s.topic)

	return m.Complete(ctx)
}

func (s *subscription) addActiveMessage(m *azservicebus.Message) {
	s.logger.Debugf("Adding message %s to active messages on topic %s", m.ID, s.topic)
	s.mu.Lock()
	s.activeMessages[m.ID] = m
	s.mu.Unlock()
}

func (s *subscription) removeActiveMessage(messageID string) {
	s.logger.Debugf("Removing message %s from active messages on topic %s", messageID, s.topic)
	s.mu.Lock()
	delete(s.activeMessages, messageID)
	s.mu.Unlock()
}
