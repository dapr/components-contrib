// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebus

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	azservicebus "github.com/Azure/azure-service-bus-go"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	// Keys
	connectionString               = "connectionString"
	consumerID                     = "consumerID"
	maxDeliveryCount               = "maxDeliveryCount"
	timeoutInSec                   = "timeoutInSec"
	lockDurationInSec              = "lockDurationInSec"
	lockRenewalInSec               = "lockRenewalInSec"
	defaultMessageTimeToLiveInSec  = "defaultMessageTimeToLiveInSec"
	autoDeleteOnIdleInSec          = "autoDeleteOnIdleInSec"
	disableEntityManagement        = "disableEntityManagement"
	maxConcurrentHandlers          = "maxConcurrentHandlers"
	handlerTimeoutInSec            = "handlerTimeoutInSec"
	prefetchCount                  = "prefetchCount"
	maxActiveMessages              = "maxActiveMessages"
	maxActiveMessagesRecoveryInSec = "maxActiveMessagesRecoveryInSec"
	errorMessagePrefix             = "azure service bus error:"

	// Defaults
	defaultTimeoutInSec        = 60
	defaultHandlerTimeoutInSec = 60
	defaultLockRenewalInSec    = 20
	// ASB Messages can be up to 256Kb. 10000 messages at this size would roughly use 2.56Gb.
	// We should change this if performance testing suggests a more sensible default.
	defaultMaxActiveMessages              = 10000
	defaultMaxActiveMessagesRecoveryInSec = 2
	defaultDisableEntityManagement        = false
)

type handler = struct{}

type azureServiceBus struct {
	metadata     metadata
	namespace    *azservicebus.Namespace
	topicManager *azservicebus.TopicManager
	logger       logger.Logger
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation
func NewAzureServiceBus(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger: logger,
	}
}

type messageHandle struct {
	msg    *azservicebus.Message
	cancel context.CancelFunc
}

func newMessageHandle(msg *azservicebus.Message, cancel context.CancelFunc) *messageHandle {
	return &messageHandle{
		msg:    msg,
		cancel: cancel,
	}
}

type subscriptionManager struct {
	topic          string
	mu             sync.RWMutex
	activeMessages map[string]*messageHandle
	sub            *azservicebus.Subscription
	logger         logger.Logger
}

func newSubscriptionManager(topic string, sub *azservicebus.Subscription, logger logger.Logger) *subscriptionManager {
	return &subscriptionManager{
		topic:          topic,
		activeMessages: make(map[string]*messageHandle),
		sub:            sub,
		logger:         logger,
	}
}

func parseAzureServiceBusMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}

	/* Required configuration settings - no defaults */
	if val, ok := meta.Properties[connectionString]; ok && val != "" {
		m.ConnectionString = val
	} else {
		return m, fmt.Errorf("%s missing connection string", errorMessagePrefix)
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return m, fmt.Errorf("%s missing consumerID", errorMessagePrefix)
	}

	/* Optional configuration settings - defaults will be set by the client */
	m.TimeoutInSec = defaultTimeoutInSec
	if val, ok := meta.Properties[timeoutInSec]; ok && val != "" {
		var err error
		m.TimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid timeoutInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.DisableEntityManagement = defaultDisableEntityManagement
	if val, ok := meta.Properties[disableEntityManagement]; ok && val != "" {
		var err error
		m.DisableEntityManagement, err = strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid disableEntityManagement %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.HandlerTimeoutInSec = defaultHandlerTimeoutInSec
	if val, ok := meta.Properties[handlerTimeoutInSec]; ok && val != "" {
		var err error
		m.HandlerTimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid handlerTimeoutInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.LockRenewalInSec = defaultLockRenewalInSec
	if val, ok := meta.Properties[lockRenewalInSec]; ok && val != "" {
		var err error
		m.LockRenewalInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockRenewalInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxActiveMessages = defaultMaxActiveMessages
	if val, ok := meta.Properties[maxActiveMessages]; ok && val != "" {
		var err error
		m.MaxActiveMessages, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxActiveMessages %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxActiveMessagesRecoveryInSec = defaultMaxActiveMessagesRecoveryInSec
	if val, ok := meta.Properties[maxActiveMessagesRecoveryInSec]; ok && val != "" {
		var err error
		m.MaxActiveMessagesRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid recoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	/* Nullable configuration settings - defaults will be set by the server */
	if val, ok := meta.Properties[maxDeliveryCount]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxDeliveryCount %s, %s", errorMessagePrefix, val, err)
		}
		m.MaxDeliveryCount = &valAsInt
	}

	if val, ok := meta.Properties[lockDurationInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockDurationInSec %s, %s", errorMessagePrefix, val, err)
		}
		m.LockDurationInSec = &valAsInt
	}

	if val, ok := meta.Properties[defaultMessageTimeToLiveInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid defaultMessageTimeToLiveInSec %s, %s", errorMessagePrefix, val, err)
		}
		m.DefaultMessageTimeToLiveInSec = &valAsInt
	}

	if val, ok := meta.Properties[autoDeleteOnIdleInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid autoDeleteOnIdleInSecKey %s, %s", errorMessagePrefix, val, err)
		}
		m.AutoDeleteOnIdleInSec = &valAsInt
	}

	if val, ok := meta.Properties[maxConcurrentHandlers]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxConcurrentHandlers %s, %s", errorMessagePrefix, val, err)
		}
		m.MaxConcurrentHandlers = &valAsInt
	}

	if val, ok := meta.Properties[prefetchCount]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid prefetchCount %s, %s", errorMessagePrefix, val, err)
		}
		m.PrefetchCount = &valAsInt
	}

	return m, nil
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) error {
	m, err := parseAzureServiceBusMetadata(metadata)
	if err != nil {
		return err
	}

	a.metadata = m
	a.namespace, err = azservicebus.NewNamespace(azservicebus.NamespaceWithConnectionString(a.metadata.ConnectionString))
	if err != nil {
		return err
	}

	a.topicManager = a.namespace.NewTopicManager()
	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	if !a.metadata.DisableEntityManagement {
		err := a.ensureTopic(req.Topic)
		if err != nil {
			return err
		}
	}

	sender, err := a.namespace.NewTopic(req.Topic)
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	err = sender.Send(ctx, azservicebus.NewMessage(req.Data))
	if err != nil {
		return err
	}
	return nil
}

func (a *azureServiceBus) Subscribe(req pubsub.SubscribeRequest, daprHandler func(msg *pubsub.NewMessage) error) error {
	subID := a.metadata.ConsumerID
	if !a.metadata.DisableEntityManagement {
		err := a.ensureSubscription(subID, req.Topic)
		if err != nil {
			return err
		}
	}
	topic, err := a.namespace.NewTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("%s could not instantiate topic %s, %s", errorMessagePrefix, req.Topic, err)
	}

	var opts []azservicebus.SubscriptionOption
	if a.metadata.PrefetchCount != nil {
		opts = append(opts, azservicebus.SubscriptionWithPrefetchCount(uint32(*a.metadata.PrefetchCount)))
	}
	sub, err := topic.NewSubscription(subID, opts...)
	if err != nil {
		return fmt.Errorf("%s could not instantiate subscription %s for topic %s", errorMessagePrefix, subID, req.Topic)
	}

	subManager := newSubscriptionManager(req.Topic, sub, a.logger)
	asbHandler := azservicebus.HandlerFunc(a.getHandlerFunc(daprHandler, subManager))
	go a.handleSubscriptionMessages(asbHandler, subManager)

	return nil
}

func (a *azureServiceBus) getHandlerFunc(daprHandler func(msg *pubsub.NewMessage) error, subManager *subscriptionManager) func(ctx context.Context, message *azservicebus.Message) error {
	return func(ctx context.Context, message *azservicebus.Message) error {
		msg := &pubsub.NewMessage{
			Data:  message.Data,
			Topic: subManager.topic,
		}

		a.logger.Debugf("Calling app's handler for message %s from topic %s", message.ID, msg.Topic)
		err := daprHandler(msg)
		if err != nil {
			a.logger.Debugf("Error in app's handler: %+v", err)
			return subManager.abandonMessage(ctx, message)
		}
		return subManager.completeMessage(ctx, message)
	}
}

func (a *azureServiceBus) handleSubscriptionMessages(asbHandler azservicebus.HandlerFunc, subManager *subscriptionManager) {
	topic := subManager.topic

	// Limiting the number of concurrent handlers will throttle
	// how many messages are processed concurrently.
	limitConcurrentHandlers := a.metadata.MaxConcurrentHandlers != nil
	var handlers chan handler
	if limitConcurrentHandlers {
		a.logger.Debugf("Limited to %d message handler(s)", *a.metadata.MaxConcurrentHandlers)
		handlers = make(chan handler, *a.metadata.MaxConcurrentHandlers)
		for i := 0; i < *a.metadata.MaxConcurrentHandlers; i++ {
			handlers <- handler{}
		}
		defer func() {
			// Cancel all asynchronous message handles
			// before closing the handlers channel
			subManager.mu.RLock()
			for _, msg := range subManager.activeMessages {
				msg.cancel()
			}
			subManager.mu.Unlock()
			close(handlers)
		}()
	}

	// Async message handler
	var asyncAsbHandler azservicebus.HandlerFunc = func(ctx context.Context, msg *azservicebus.Message) error {
		ctx, cancel := context.WithCancel(ctx)
		msgHandle := newMessageHandle(msg, cancel)
		subManager.addActiveMessage(msgHandle)

		// Process messages asynchronously
		go func() {
			if limitConcurrentHandlers {
				a.logger.Debugf("Attempting to take message handler...")
				<-handlers // Take or wait on a free handler before getting a new message
				a.logger.Debugf("Taken message handler")

				defer func() {
					a.logger.Debugf("Releasing message handler...")
					handlers <- handler{} // Release a handler
					a.logger.Debugf("Released message handler")
				}()
			}

			ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(a.metadata.HandlerTimeoutInSec))
			defer cancel()

			a.logger.Debugf("Handling message %s", msg.ID)
			err := asbHandler(ctx, msg)
			if err != nil {
				a.logger.Errorf("%s error handling message %s from topic '%s', %s", errorMessagePrefix, msg.ID, topic, err)
			}
		}()
		return nil
	}

	// Lock renewal loop
	go func() {
		if !a.lockRenewalEnabled() {
			a.logger.Debugf("Lock renewal disabled")
			return
		}

		for {
			time.Sleep(time.Second * time.Duration(a.metadata.LockRenewalInSec))

			subManager.mu.RLock()
			activeMessageLen := len(subManager.activeMessages)
			subManager.mu.RUnlock()
			if activeMessageLen == 0 {
				a.logger.Debugf("No active messages require lock renewal for topic %s", topic)
				continue
			}

			msgs := make([]*azservicebus.Message, 0)
			subManager.mu.RLock()
			for _, m := range subManager.activeMessages {
				msgs = append(msgs, m.msg)
			}
			subManager.mu.RUnlock()
			a.logger.Debugf("Renewing %d active message lock(s) for topic %s", len(msgs), topic)
			err := subManager.sub.RenewLocks(context.Background(), msgs...)
			if err != nil {
				// We cannot guarentee messages aren't finalized after we took a snapshot so this may occur.
				a.logger.Warnf("%s error renewing active message lock(s) for topic %s, ", errorMessagePrefix, topic, err)
			}
		}
	}()

	// Receiver loop
	for {
		// If we have too many active messages don't receive any more for a while.
		subManager.mu.RLock()
		activeMessageLen := len(subManager.activeMessages)
		subManager.mu.RUnlock()
		if activeMessageLen < a.metadata.MaxActiveMessages {
			a.logger.Debugf("Waiting to receive message from topic %s", topic)
			if err := subManager.sub.ReceiveOne(context.Background(), asyncAsbHandler); err != nil {
				a.logger.Errorf("%s error receiving from topic %s, %s", errorMessagePrefix, topic, err)
				// Must close to reset sub's receiver
				if err := subManager.sub.Close(context.Background()); err != nil {
					a.logger.Errorf("%s error closing subscription to topic %s, %s", errorMessagePrefix, topic, err)
					return
				}
			}
		} else {
			// Sleep to allow the current active messages to be processed before getting more.
			a.logger.Debugf("Max active messages %d reached for topic %s, recovering for %d seconds", a.metadata.MaxActiveMessages, topic, a.metadata.MaxActiveMessagesRecoveryInSec)
			time.Sleep(time.Second * time.Duration(a.metadata.MaxActiveMessagesRecoveryInSec))
		}
	}
}

func (a *azureServiceBus) lockRenewalEnabled() bool {
	return a.metadata.LockRenewalInSec > 0
}

func (a *azureServiceBus) ensureTopic(topic string) error {
	entity, err := a.getTopicEntity(topic)
	if err != nil {
		return err
	}

	if entity == nil {
		err = a.createTopicEntity(topic)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *azureServiceBus) ensureSubscription(name string, topic string) error {
	err := a.ensureTopic(topic)
	if err != nil {
		return err
	}

	subManager, err := a.namespace.NewSubscriptionManager(topic)
	if err != nil {
		return err
	}

	entity, err := a.getSubscriptionEntity(subManager, topic, name)
	if err != nil {
		return err
	}

	if entity == nil {
		err = a.createSubscriptionEntity(subManager, topic, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *azureServiceBus) getTopicEntity(topic string) (*azservicebus.TopicEntity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	if a.topicManager == nil {
		return nil, fmt.Errorf("%s init() has not been called", errorMessagePrefix)
	}
	topicEntity, err := a.topicManager.Get(ctx, topic)
	if err != nil && !azservicebus.IsErrNotFound(err) {
		return nil, fmt.Errorf("%s could not get topic %s, %s", errorMessagePrefix, topic, err)
	}
	return topicEntity, nil
}

func (a *azureServiceBus) createTopicEntity(topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err := a.topicManager.Put(ctx, topic)
	if err != nil {
		return fmt.Errorf("%s could not put topic %s, %s", errorMessagePrefix, topic, err)
	}
	return nil
}

func (a *azureServiceBus) getSubscriptionEntity(mgr *azservicebus.SubscriptionManager, topic, subscription string) (*azservicebus.SubscriptionEntity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	entity, err := mgr.Get(ctx, subscription)
	if err != nil && !azservicebus.IsErrNotFound(err) {
		return nil, fmt.Errorf("%s could not get subscription %s, %s", errorMessagePrefix, subscription, err)
	}
	return entity, nil
}

func (a *azureServiceBus) createSubscriptionEntity(mgr *azservicebus.SubscriptionManager, topic, subscription string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	opts, err := a.createSubscriptionManagementOptions()
	if err != nil {
		return err
	}

	_, err = mgr.Put(ctx, subscription, opts...)
	if err != nil {
		return fmt.Errorf("%s could not put subscription %s, %s", errorMessagePrefix, subscription, err)
	}
	return nil
}

func (a *azureServiceBus) createSubscriptionManagementOptions() ([]azservicebus.SubscriptionManagementOption, error) {
	var opts []azservicebus.SubscriptionManagementOption
	if a.metadata.MaxDeliveryCount != nil {
		opts = append(opts, subscriptionManagementOptionsWithMaxDeliveryCount(a.metadata.MaxDeliveryCount))
	}
	if a.metadata.LockDurationInSec != nil {
		opts = append(opts, subscriptionManagementOptionsWithLockDuration(a.metadata.LockDurationInSec))
	}
	if a.metadata.DefaultMessageTimeToLiveInSec != nil {
		opts = append(opts, subscriptionManagementOptionsWithDefaultMessageTimeToLive(a.metadata.DefaultMessageTimeToLiveInSec))
	}
	if a.metadata.AutoDeleteOnIdleInSec != nil {
		opts = append(opts, subscriptionManagementOptionsWithAutoDeleteOnIdle(a.metadata.AutoDeleteOnIdleInSec))
	}
	return opts, nil
}

func (s *subscriptionManager) abandonMessage(ctx context.Context, m *azservicebus.Message) error {
	s.removeActiveMessage(m.ID)
	s.logger.Debugf("Abandoning message %s on topic %s", m.ID, s.topic)
	return m.Abandon(ctx)
}

func (s *subscriptionManager) completeMessage(ctx context.Context, m *azservicebus.Message) error {
	s.removeActiveMessage(m.ID)
	s.logger.Debugf("Completing message %s on topic %s", m.ID, s.topic)
	return m.Complete(ctx)
}

func (s *subscriptionManager) addActiveMessage(m *messageHandle) {
	s.logger.Debugf("Adding message %s to active messages on topic %s", m.msg.ID, s.topic)
	s.mu.Lock()
	s.activeMessages[m.msg.ID] = m
	s.mu.Unlock()
}

func (s *subscriptionManager) removeActiveMessage(messageID string) {
	s.logger.Debugf("Removing message %s from active messages on topic", messageID, s.topic)
	s.mu.Lock()
	delete(s.activeMessages, messageID)
	s.mu.Unlock()
}

func subscriptionManagementOptionsWithMaxDeliveryCount(maxDeliveryCount *int) azservicebus.SubscriptionManagementOption {
	return func(d *azservicebus.SubscriptionDescription) error {
		mdc := int32(*maxDeliveryCount)
		d.MaxDeliveryCount = &mdc
		return nil
	}
}

func subscriptionManagementOptionsWithAutoDeleteOnIdle(durationInSec *int) azservicebus.SubscriptionManagementOption {
	return func(d *azservicebus.SubscriptionDescription) error {
		duration := fmt.Sprintf("PT%dS", *durationInSec)
		d.AutoDeleteOnIdle = &duration
		return nil
	}
}

func subscriptionManagementOptionsWithDefaultMessageTimeToLive(durationInSec *int) azservicebus.SubscriptionManagementOption {
	return func(d *azservicebus.SubscriptionDescription) error {
		duration := fmt.Sprintf("PT%dS", *durationInSec)
		d.DefaultMessageTimeToLive = &duration
		return nil
	}
}

func subscriptionManagementOptionsWithLockDuration(durationInSec *int) azservicebus.SubscriptionManagementOption {
	return func(d *azservicebus.SubscriptionDescription) error {
		duration := fmt.Sprintf("PT%dS", *durationInSec)
		d.LockDuration = &duration
		return nil
	}
}
