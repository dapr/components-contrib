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

type activeMessage struct {
	msg    *azservicebus.Message
	cancel context.CancelFunc
}

func newActiveMessage(msg *azservicebus.Message, cancel context.CancelFunc) *activeMessage {
	return &activeMessage{
		msg:    msg,
		cancel: cancel,
	}
}

type subscription struct {
	topic                   string
	mu                      sync.RWMutex
	activeMessages          map[string]*activeMessage
	entity                  *azservicebus.Subscription
	limitConcurrentHandlers bool
	handlerChan             chan handler
	logger                  logger.Logger
}

func newSubscription(topic string, sub *azservicebus.Subscription, maxConcurrentHandlers *int, logger logger.Logger) *subscription {
	s := &subscription{
		topic:          topic,
		activeMessages: make(map[string]*activeMessage),
		entity:         sub,
		logger:         logger,
	}

	if maxConcurrentHandlers != nil {
		s.logger.Debugf("Limited to %d message handler(s)", *maxConcurrentHandlers)
		s.limitConcurrentHandlers = true
		s.handlerChan = make(chan handler, *maxConcurrentHandlers)
		for i := 0; i < *maxConcurrentHandlers; i++ {
			s.handlerChan <- handler{}
		}
	}

	return s
}

// ReceiveMessages is a blocking call to receive messages on an Azure Service Bus subscription from a topic
func (s *subscription) ReceiveMessages(appHandler func(msg *pubsub.NewMessage) error, lockRenewalInSec int, handlerTimeoutInSec int, timeoutInSec int, maxActiveMessages int, maxActiveMessagesRecoveryInSec int) error {
	defer s.close(timeoutInSec)

	done := make(chan struct{}, 1)

	// Lock renewal loop
	go func() {
		shouldRenewLocks := lockRenewalInSec > 0
		if !shouldRenewLocks {
			s.logger.Debugf("Lock renewal disabled")
			return
		}
		for {
			select {
			case <-done:
				return
			default:
			}
			time.Sleep(time.Second * time.Duration(lockRenewalInSec))
			s.tryRenewLocks()
		}
	}()

	// Receiver loop
	asyncHandler := s.asyncWrapper(s.getHandlerFunc(appHandler, handlerTimeoutInSec, timeoutInSec))
	for {
		s.mu.RLock()
		activeMessageLen := len(s.activeMessages)
		s.mu.RUnlock()
		if activeMessageLen >= maxActiveMessages {
			// Max active messages reached, sleep to allow the current active messages to be processed before getting more
			s.logger.Debugf("Max active messages %d reached for topic %s, recovering for %d seconds", maxActiveMessages, s.topic, maxActiveMessagesRecoveryInSec)
			time.Sleep(time.Second * time.Duration(maxActiveMessagesRecoveryInSec))
			continue
		}
		if err := s.receiveMessage(context.Background(), asyncHandler); err != nil {
			done <- struct{}{}
			return err
		}
	}
}

func (s *subscription) close(timeoutInSec int) {
	// Cancel all active messages before closing the handlers channel
	s.mu.RLock()
	for _, msg := range s.activeMessages {
		msg.cancel()
	}
	s.mu.RUnlock()
	if s.limitConcurrentHandlers {
		close(s.handlerChan)
	}

	// Ensure subscription entity is closed
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*time.Duration(timeoutInSec))
	defer cancel()
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

		s.logger.Debugf("Calling app's handler for message %s from topic %s", message.ID, s.topic)
		ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(handlerTimeoutInSec))
		defer cancel()
		err := appHandler(msg) // TODO (*cont.): The context should be propogated here.
		if err != nil {
			s.logger.Debugf("Error in app's handler: %+v", err)
			return s.abandonMessage(ctx, message)
		}
		return s.completeMessage(ctx, message)
	}
}

func (s *subscription) asyncWrapper(handlerFunc azservicebus.HandlerFunc) azservicebus.HandlerFunc {
	return func(ctx context.Context, msg *azservicebus.Message) error {
		// Process messages asynchronously
		go func() {
			// TODO: * This context is used to control the execution of the async message handler
			// 		 including the app's handler and the message finalization (complete/abandon).
			//		 Currently the app handler does not accept a context so cannot be safely cancelled.
			ctx, cancel := context.WithCancel(ctx)
			s.addActiveMessage(newActiveMessage(msg, cancel))
			defer s.removeActiveMessage(msg.ID)

			if s.limitConcurrentHandlers {
				s.logger.Debugf("Attempting to take message handler...")
				<-s.handlerChan // Take or wait on a free handler before getting a new message
				s.logger.Debugf("Taken message handler")

				defer func() {
					s.logger.Debugf("Releasing message handler...")
					s.handlerChan <- handler{} // Release a handler when complete
					s.logger.Debugf("Released message handler")
				}()
			}

			err := handlerFunc(ctx, msg)
			if err != nil {
				s.logger.Errorf("%s error handling message %s from topic '%s', %s", errorMessagePrefix, msg.ID, s.topic, err)
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
		msgs = append(msgs, m.msg)
	}
	s.mu.RUnlock()

	// Lock renewal is best effort and not guarenteed to succeed, warnings are expected.
	s.logger.Debugf("Trying to renew %d active message lock(s) for topic %s", len(msgs), s.topic)
	err := s.entity.RenewLocks(context.Background(), msgs...)
	if err != nil {
		s.logger.Warnf("Couldn't renew all active message lock(s) for topic %s, ", s.topic, err)
	}
}

func (s *subscription) receiveMessage(ctx context.Context, handler azservicebus.HandlerFunc) error {
	s.logger.Debugf("Waiting to receive message from topic %s", s.topic)
	if err := s.entity.ReceiveOne(ctx, handler); err != nil {
		return fmt.Errorf("error receiving message from topic %s, %s", s.topic, err)
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

func (s *subscription) addActiveMessage(m *activeMessage) {
	s.logger.Debugf("Adding message %s to active messages on topic %s", m.msg.ID, s.topic)
	s.mu.Lock()
	s.activeMessages[m.msg.ID] = m
	s.mu.Unlock()
}

func (s *subscription) removeActiveMessage(messageID string) {
	s.logger.Debugf("Removing message %s from active messages on topic", messageID, s.topic)
	s.mu.Lock()
	delete(s.activeMessages, messageID)
	s.mu.Unlock()
}
