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
	"strconv"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/cenkalti/backoff/v4"

	azservicebus "github.com/Azure/azure-service-bus-go"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	// Keys.
	connectionString                = "connectionString"
	consumerID                      = "consumerID"
	maxDeliveryCount                = "maxDeliveryCount"
	timeoutInSec                    = "timeoutInSec"
	lockDurationInSec               = "lockDurationInSec"
	lockRenewalInSec                = "lockRenewalInSec"
	defaultMessageTimeToLiveInSec   = "defaultMessageTimeToLiveInSec"
	autoDeleteOnIdleInSec           = "autoDeleteOnIdleInSec"
	disableEntityManagement         = "disableEntityManagement"
	maxConcurrentHandlers           = "maxConcurrentHandlers"
	handlerTimeoutInSec             = "handlerTimeoutInSec"
	prefetchCount                   = "prefetchCount"
	maxActiveMessages               = "maxActiveMessages"
	maxActiveMessagesRecoveryInSec  = "maxActiveMessagesRecoveryInSec"
	maxReconnectionAttempts         = "maxReconnectionAttempts"
	connectionRecoveryInSec         = "connectionRecoveryInSec"
	publishMaxRetries               = "publishMaxRetries"
	publishInitialRetryInternalInMs = "publishInitialRetryInternalInMs"
	namespaceName                   = "namespaceName"
	errorMessagePrefix              = "azure service bus error:"

	// Defaults.
	defaultTimeoutInSec        = 60
	defaultHandlerTimeoutInSec = 60
	defaultLockRenewalInSec    = 20
	// ASB Messages can be up to 256Kb. 10000 messages at this size would roughly use 2.56Gb.
	// We should change this if performance testing suggests a more sensible default.
	defaultMaxActiveMessages               = 10000
	defaultMaxActiveMessagesRecoveryInSec  = 2
	defaultDisableEntityManagement         = false
	defaultMaxReconnectionAttempts         = 30
	defaultConnectionRecoveryInSec         = 2
	defaultPublishMaxRetries               = 5
	defaultPublishInitialRetryInternalInMs = 500
)

var retriableSendingErrors = map[amqp.ErrorCondition]struct{}{
	"com.microsoft:server-busy'":             {},
	amqp.ErrorResourceLimitExceeded:          {},
	amqp.ErrorResourceLocked:                 {},
	amqp.ErrorTransferLimitExceeded:          {},
	amqp.ErrorInternalError:                  {},
	amqp.ErrorIllegalState:                   {},
	"com.microsoft:message-lock-lost":        {},
	"com.microsoft:session-cannot-be-locked": {},
	"com.microsoft:timeout":                  {},
	"com.microsoft:session-lock-lost":        {},
	"com.microsoft:store-lock-lost":          {},
}

type handle = struct{}

type azureServiceBus struct {
	metadata      metadata
	namespace     *azservicebus.Namespace
	topicManager  *azservicebus.TopicManager
	logger        logger.Logger
	subscriptions []*subscription
	features      []pubsub.Feature
	topics        map[string]*azservicebus.Topic
	topicsLock    *sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation.
func NewAzureServiceBus(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger:        logger,
		subscriptions: []*subscription{},
		features:      []pubsub.Feature{pubsub.FeatureMessageTTL},
		topics:        map[string]*azservicebus.Topic{},
		topicsLock:    &sync.RWMutex{},
	}
}

func parseAzureServiceBusMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}

	/* Required configuration settings - no defaults. */
	if val, ok := meta.Properties[connectionString]; ok && val != "" {
		m.ConnectionString = val

		// The connection string and the namespace cannot both be present.
		if namespace, present := meta.Properties[namespaceName]; present && namespace != "" {
			return m, fmt.Errorf("%s connectionString and namespaceName cannot both be specified", errorMessagePrefix)
		}
	} else if val, ok := meta.Properties[namespaceName]; ok && val != "" {
		m.NamespaceName = val
	} else {
		return m, fmt.Errorf("%s missing connection string and namespace name", errorMessagePrefix)
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return m, fmt.Errorf("%s missing consumerID", errorMessagePrefix)
	}

	/* Optional configuration settings - defaults will be set by the client. */
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

	m.MaxReconnectionAttempts = defaultMaxReconnectionAttempts
	if val, ok := meta.Properties[maxReconnectionAttempts]; ok && val != "" {
		var err error
		m.MaxReconnectionAttempts, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxReconnectionAttempts %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.ConnectionRecoveryInSec = defaultConnectionRecoveryInSec
	if val, ok := meta.Properties[connectionRecoveryInSec]; ok && val != "" {
		var err error
		m.ConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid connectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	/* Nullable configuration settings - defaults will be set by the server. */
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

	m.PublishMaxRetries = defaultPublishMaxRetries
	if val, ok := meta.Properties[publishMaxRetries]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid publishMaxRetries %s, %s", errorMessagePrefix, val, err)
		}
		m.PublishMaxRetries = valAsInt
	}

	m.PublishInitialRetryIntervalInMs = defaultPublishInitialRetryInternalInMs
	if val, ok := meta.Properties[publishInitialRetryInternalInMs]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid publishInitialRetryIntervalInMs %s, %s", errorMessagePrefix, val, err)
		}
		m.PublishInitialRetryIntervalInMs = valAsInt
	}

	return m, nil
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) error {
	m, err := parseAzureServiceBusMetadata(metadata)
	if err != nil {
		return err
	}

	userAgent := "dapr-" + logger.DaprVersion
	a.metadata = m
	if a.metadata.ConnectionString != "" {
		a.namespace, err = azservicebus.NewNamespace(
			azservicebus.NamespaceWithConnectionString(a.metadata.ConnectionString),
			azservicebus.NamespaceWithUserAgent(userAgent))

		if err != nil {
			return err
		}
	} else {
		// Initialization code
		settings, err := azauth.NewEnvironmentSettings(azauth.AzureServiceBusResourceName, metadata.Properties)
		if err != nil {
			return err
		}

		tokenProvider, err := settings.GetAADTokenProvider()
		if err != nil {
			return err
		}

		a.namespace, err = azservicebus.NewNamespace(azservicebus.NamespaceWithTokenProvider(tokenProvider),
			azservicebus.NamespaceWithUserAgent(userAgent))
		if err != nil {
			return err
		}

		// We set these separately as the ServiceBus SDK does not provide a way to pass the environment via the options
		// pattern unless you allow it to recreate the entire environment which seems wasteful.
		a.namespace.Name = a.metadata.NamespaceName
		a.namespace.Environment = *settings.AzureEnvironment
		a.namespace.Suffix = settings.AzureEnvironment.ServiceBusEndpointSuffix
	}

	a.topicManager = a.namespace.NewTopicManager()

	a.ctx, a.cancel = context.WithCancel(context.Background())

	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	var sender *azservicebus.Topic
	var err error

	a.topicsLock.RLock()
	if topic, ok := a.topics[req.Topic]; ok {
		sender = topic
	}
	a.topicsLock.RUnlock()

	if sender == nil {
		// Ensure the topic exists the first time it is referenced.
		if !a.metadata.DisableEntityManagement {
			if err = a.ensureTopic(req.Topic); err != nil {
				return err
			}
		}
		a.topicsLock.Lock()
		sender, err = a.namespace.NewTopic(req.Topic)
		a.topics[req.Topic] = sender
		a.topicsLock.Unlock()

		if err != nil {
			return err
		}
	}

	msg, err := NewASBMessageFromPubsubRequest(req)
	if err != nil {
		return err
	}

	return a.doPublish(sender, msg)
}

func (a *azureServiceBus) doPublish(sender *azservicebus.Topic, msg *azservicebus.Message) error {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Duration(a.metadata.PublishInitialRetryIntervalInMs) * time.Millisecond
	bo := backoff.WithMaxRetries(ebo, uint64(a.metadata.PublishMaxRetries))
	bo = backoff.WithContext(bo, a.ctx)

	return retry.NotifyRecover(func() error {
		ctx, cancel := context.WithTimeout(a.ctx, time.Second*time.Duration(a.metadata.TimeoutInSec))
		defer cancel()

		err := sender.Send(ctx, msg)
		if err == nil {
			return nil
		}

		var amqpError *amqp.Error
		if errors.As(err, &amqpError) {
			if _, ok := retriableSendingErrors[amqpError.Condition]; ok {
				return amqpError // Retries.
			}
		}
		var connClosedError azservicebus.ErrConnectionClosed
		if errors.As(err, &connClosedError) {
			return connClosedError // Retries.
		}

		return backoff.Permanent(err) // Does not retry.
	}, bo, func(err error, _ time.Duration) {
		a.logger.Debugf("Could not publish service bus message. Retrying...: %v", err)
	}, func() {
		a.logger.Debug("Successfully published service bus message after it previously failed")
	})
}

func (a *azureServiceBus) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	subID := a.metadata.ConsumerID
	if !a.metadata.DisableEntityManagement {
		err := a.ensureSubscription(subID, req.Topic)
		if err != nil {
			return err
		}
	}

	go func() {
		// Limit the number of attempted reconnects we make.
		reconnAttempts := make(chan struct{}, a.metadata.MaxReconnectionAttempts)
		for i := 0; i < a.metadata.MaxReconnectionAttempts; i++ {
			reconnAttempts <- struct{}{}
		}

		// len(reconnAttempts) should be considered stale but we can afford a little error here.
		readAttemptsStale := func() int { return len(reconnAttempts) }

		// Periodically refill the reconnect attempts channel to avoid
		// exhausting all the refill attempts due to intermittent issues
		// ocurring over a longer period of time.
		reconnCtx, reconnCancel := context.WithCancel(a.ctx)
		defer reconnCancel()
		go func() {
			for {
				select {
				case <-reconnCtx.Done():
					a.logger.Debugf("Reconnect context for topic %s is done", req.Topic)

					return
				case <-time.After(2 * time.Minute):
					attempts := readAttemptsStale()
					if attempts < a.metadata.MaxReconnectionAttempts {
						reconnAttempts <- struct{}{}
					}
					a.logger.Debugf("Number of reconnect attempts remaining for topic %s: %d", req.Topic, attempts)
				}
			}
		}()

		// Reconnect loop.
		for {
			topic, err := a.namespace.NewTopic(req.Topic)
			if err != nil {
				a.logger.Errorf("%s could not instantiate topic %s, %s", errorMessagePrefix, req.Topic, err)

				return
			}

			var opts []azservicebus.SubscriptionOption
			if a.metadata.PrefetchCount != nil {
				opts = append(opts, azservicebus.SubscriptionWithPrefetchCount(uint32(*a.metadata.PrefetchCount)))
			}
			subEntity, err := topic.NewSubscription(subID, opts...)
			if err != nil {
				a.logger.Errorf("%s could not instantiate subscription %s for topic %s", errorMessagePrefix, subID, req.Topic)

				return
			}
			sub := newSubscription(req.Topic, subEntity, a.metadata.MaxConcurrentHandlers, a.logger)
			a.subscriptions = append(a.subscriptions, sub)
			// ReceiveAndBlock will only return with an error
			// that it cannot handle internally. The subscription
			// connection is closed when this method returns.
			// If that occurs, we will log the error and attempt
			// to re-establish the subscription connection until
			// we exhaust the number of reconnect attempts.
			ctx, cancel := context.WithCancel(context.Background())
			innerErr := sub.ReceiveAndBlock(ctx,
				handler,
				a.metadata.LockRenewalInSec,
				a.metadata.HandlerTimeoutInSec,
				a.metadata.TimeoutInSec,
				a.metadata.MaxActiveMessages,
				a.metadata.MaxActiveMessagesRecoveryInSec)
			if innerErr != nil {
				var detachError *amqp.DetachError
				var amqpError *amqp.Error
				if errors.Is(innerErr, detachError) ||
					(errors.As(innerErr, &amqpError) && amqpError.Condition == amqp.ErrorDetachForced) {
					a.logger.Debug(innerErr)
				} else {
					a.logger.Error(innerErr)
				}
			}
			cancel() // Cancel receive context.

			attempts := readAttemptsStale()
			if attempts == 0 {
				a.logger.Errorf("Subscription to topic %s lost connection, unable to recover after %d attempts", sub.topic, a.metadata.MaxReconnectionAttempts)

				return
			}

			a.logger.Warnf("Subscription to topic %s lost connection, attempting to reconnect... [%d/%d]", sub.topic, a.metadata.MaxReconnectionAttempts-attempts, a.metadata.MaxReconnectionAttempts)
			time.Sleep(time.Second * time.Duration(a.metadata.ConnectionRecoveryInSec))
			<-reconnAttempts
		}
	}()

	return nil
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

func (a *azureServiceBus) Close() error {
	for _, s := range a.subscriptions {
		s.close(a.ctx)
	}

	for _, t := range a.topics {
		t.Close(a.ctx)
	}

	a.cancel()

	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return a.features
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
