package servicebus

import (
	"context"
	"fmt"
	"sync"
	"time"

	azservicebus "github.com/Azure/azure-service-bus-go"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

type subscription struct {
	topic                   string
	mu                      sync.RWMutex
	activeMessages          map[string]*azservicebus.Message
	entity                  *azservicebus.Subscription
	limitConcurrentHandlers bool
	handlerChan             chan handler
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
		s.handlerChan = make(chan handler, *maxConcurrentHandlers)
		for i := 0; i < *maxConcurrentHandlers; i++ {
			s.handlerChan <- handler{}
		}
	}

	return s
}

// ReceiveAndBlock is a blocking call to receive messages on an Azure Service Bus subscription from a topic
func (s *subscription) ReceiveAndBlock(ctx context.Context, appHandler func(msg *pubsub.NewMessage) error, lockRenewalInSec int, handlerTimeoutInSec int, timeoutInSec int, maxActiveMessages int, maxActiveMessagesRecoveryInSec int) error {
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

	asyncHandler := s.asyncWrapper(s.getHandlerFunc(appHandler, handlerTimeoutInSec, timeoutInSec))

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
		s.logger.Errorf("%s closing subscription entity for topic %s: %+v", errorMessagePrefix, s.topic, err)
	}
}

func (s *subscription) getHandlerFunc(appHandler func(msg *pubsub.NewMessage) error, handlerTimeoutInSec int, timeoutInSec int) azservicebus.HandlerFunc {
	return func(ctx context.Context, message *azservicebus.Message) error {
		msg := &pubsub.NewMessage{
			Data:  message.Data,
			Topic: s.topic,
		}

		// TODO(#1721): Context should be propogated to the app handler call for timeout and cancellation.
		//
		// handleCtx, handleCancel := context.WithTimeout(ctx, time.Second*time.Duration(handlerTimeoutInSec))
		// defer handleCancel()
		s.logger.Debugf("Calling app's handler for message %s on topic %s", message.ID, s.topic)
		err := appHandler(msg)

		// The context isn't handled downstream so we time out these
		// operations here to avoid getting stuck waiting for a message
		// to finalize (abandon/complete) if the connection has dropped.
		errs := make(chan error, 1)
		if err != nil {
			s.logger.Warnf("Error in app's handler: %+v", err)
			go func() {
				errs <- s.abandonMessage(ctx, message)
			}()
		}
		go func() {
			errs <- s.completeMessage(ctx, message)
		}()
		select {
		case err := <-errs:
			return err
		case <-time.After(time.Second * time.Duration(timeoutInSec)):
			return fmt.Errorf("%s call to finalize message %s has timedout", errorMessagePrefix, message.ID)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *subscription) asyncWrapper(handlerFunc azservicebus.HandlerFunc) azservicebus.HandlerFunc {
	return func(ctx context.Context, msg *azservicebus.Message) error {
		go func() {
			s.addActiveMessage(msg)
			defer s.removeActiveMessage(msg.ID)

			if s.limitConcurrentHandlers {
				s.logger.Debugf("Taking message handler for %s on topic %s", msg.ID, s.topic)
				select {
				case <-ctx.Done():
					s.logger.Debugf("Message context done for %s on topic %s", msg.ID, s.topic)

					return
				case <-s.handlerChan: // Take or wait on a free handler before getting a new message
					s.logger.Debugf("Taken message handler for %s on topic %s", msg.ID, s.topic)
				}

				defer func() {
					s.logger.Debugf("Releasing message handler for %s on topic %s", msg.ID, s.topic)
					s.handlerChan <- handler{} // Release a handler when complete
					s.logger.Debugf("Released message handler for %s on topic %s", msg.ID, s.topic)
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
		s.logger.Warnf("Couldn't renew all active message lock(s) for topic %s, ", s.topic, err)
	}
}

func (s *subscription) receiveMessage(ctx context.Context, handler azservicebus.HandlerFunc) error {
	s.logger.Debugf("Waiting to receive message on topic %s", s.topic)
	if err := s.entity.ReceiveOne(ctx, handler); err != nil {
		return fmt.Errorf("%s error receiving message on topic %s, %s", errorMessagePrefix, s.topic, err)
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
