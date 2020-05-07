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
	connectionString              = "connectionString"
	consumerID                    = "consumerID"
	maxDeliveryCount              = "maxDeliveryCount"
	timeoutInSec                  = "timeoutInSec"
	lockDurationInSec             = "lockDurationInSec"
	lockRenewalInSec              = "lockRenewalInSec"
	defaultMessageTimeToLiveInSec = "defaultMessageTimeToLiveInSec"
	autoDeleteOnIdleInSec         = "autoDeleteOnIdleInSec"
	disableEntityManagement       = "disableEntityManagement"
	numConcurrentHandlers         = "numConcurrentHandlers"
	handlerTimeoutInSec           = "handlerTimeoutInSec"
	prefetchCount                 = "prefetchCount"
	errorMessagePrefix            = "azure service bus error:"

	// Defaults
	defaultTimeoutInSec            = 60
	defaultHandlerTimeoutInSec     = 60
	defaultLockRenewalInSec        = 20
	defaultDisableEntityManagement = false
)

type handler = struct{}

type azureServiceBus struct {
	metadata       metadata
	namespace      *azservicebus.Namespace
	topicManager   *azservicebus.TopicManager
	activeMessages map[string]*azservicebus.Message
	mu             sync.RWMutex
	logger         logger.Logger
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation
func NewAzureServiceBus(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		activeMessages: make(map[string]*azservicebus.Message),
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

	if val, ok := meta.Properties[numConcurrentHandlers]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid numConcurrentHandlers %s, %s", errorMessagePrefix, val, err)
		}
		m.NumConcurrentHandlers = &valAsInt
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

	asbHandler := azservicebus.HandlerFunc(a.getHandlerFunc(req.Topic, daprHandler))
	go a.handleSubscriptionMessages(req.Topic, sub, asbHandler)

	return nil
}

func (a *azureServiceBus) getHandlerFunc(topic string, daprHandler func(msg *pubsub.NewMessage) error) func(ctx context.Context, message *azservicebus.Message) error {
	return func(ctx context.Context, message *azservicebus.Message) error {
		msg := &pubsub.NewMessage{
			Data:  message.Data,
			Topic: topic,
		}

		a.logger.Debugf("Calling app's handler for message %s", message.ID)
		err := daprHandler(msg)
		if err != nil {
			return a.abondonMessage(ctx, message)
		}
		return a.completeMessage(ctx, message)
	}
}

func (a *azureServiceBus) abondonMessage(ctx context.Context, m *azservicebus.Message) error {
	a.logger.Debugf("Removing message %s from active messages", m.ID)
	a.mu.Lock()
	delete(a.activeMessages, m.ID)
	a.mu.Unlock()

	a.logger.Debugf("Abandoning message %s", m.ID)
	return m.Abandon(ctx)
}

func (a *azureServiceBus) completeMessage(ctx context.Context, m *azservicebus.Message) error {
	a.logger.Debugf("Removing message %s from active messages", m.ID)
	a.mu.Lock()
	delete(a.activeMessages, m.ID)
	a.mu.Unlock()

	a.logger.Debugf("Completing message %s", m.ID)
	return m.Complete(ctx)
}

func (a *azureServiceBus) handleSubscriptionMessages(topic string, sub *azservicebus.Subscription, asbHandler azservicebus.HandlerFunc) {
	// Limiting the number of concurrent handlers will throttle
	// how many messages are receieved and processed concurrently.
	limitNumConcurrentHandlers := a.metadata.NumConcurrentHandlers != nil
	var handlers chan handler
	if limitNumConcurrentHandlers {
		a.logger.Debugf("Limited to %d message handler(s)", *a.metadata.NumConcurrentHandlers)
		handlers = make(chan handler, *a.metadata.NumConcurrentHandlers)
		for i := 0; i < *a.metadata.NumConcurrentHandlers; i++ {
			handlers <- handler{}
		}
		defer close(handlers)
	}

	// Message handler
	var asyncAsbHandler azservicebus.HandlerFunc = func(ctx context.Context, msg *azservicebus.Message) error {
		a.logger.Debugf("Received message %s from topic", msg.ID)

		a.mu.Lock()
		a.activeMessages[msg.ID] = msg
		a.mu.Unlock()

		// Process messages asynchronously
		go func() {
			if limitNumConcurrentHandlers {
				defer func() {
					a.logger.Debugf("Releasing message handler")
					handlers <- handler{} // Release a handler
					a.logger.Debugf("Released message handler")
				}()
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.HandlerTimeoutInSec))
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
		for {
			time.Sleep(time.Second * time.Duration(a.metadata.LockRenewalInSec))

			if (len(a.activeMessages) == 0){
				a.logger.Debugf("No active messages to renew lock for")
				continue
			}

			msgs := make([]*azservicebus.Message, 0)
			a.mu.RLock()
			for _, m := range a.activeMessages {
				msgs = append(msgs, m)
			}
			a.mu.RUnlock()
			a.logger.Debugf("Renewing %d active message lock(s)", len(msgs))
			err := sub.RenewLocks(context.Background(), msgs...)
			if err != nil {
				a.logger.Errorf("%s error renewing active message lock(s) for topic %s, ", errorMessagePrefix, topic, err)
			}
		}
	}()

	// Receiver loop
	for {
		if limitNumConcurrentHandlers {
			a.logger.Debugf("Attempting to take message handler")
			<-handlers // Take or wait on a free handler before getting a new message
			a.logger.Debugf("Taken message handler")
		}

		a.logger.Debugf("Waiting to receive message from topic")
		if err := sub.ReceiveOne(context.Background(), asyncAsbHandler); err != nil {
			a.logger.Errorf("%s error receiving from topic %s, %s", errorMessagePrefix, topic, err)
			// Must close to reset sub's receiver
			if err := sub.Close(context.Background()); err != nil {
				a.logger.Errorf("%s error closing subscription to topic %s, %s", errorMessagePrefix, topic, err)
				return
			}
		}
	}
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