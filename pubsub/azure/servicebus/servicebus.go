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
	"strings"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/cenkalti/backoff/v4"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	admin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

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
	client        *azservicebus.Client
	adminClient   *admin.Client
	logger        logger.Logger
	subscriptions []*subscription
	features      []pubsub.Feature
	topics        map[string]*azservicebus.Sender
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
		topics:        map[string]*azservicebus.Sender{},
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
		a.client, err = azservicebus.NewClientFromConnectionString(a.metadata.ConnectionString, &azservicebus.ClientOptions{
			ApplicationID: userAgent,
		})

		if err != nil {
			return err
		}

		a.adminClient, err = admin.NewClientFromConnectionString(a.metadata.ConnectionString, &admin.ClientOptions{})

		if err != nil {
			return err
		}
	} else {
		// Initialization code
		settings, err := azauth.NewEnvironmentSettings(azauth.AzureServiceBusResourceName, metadata.Properties)
		if err != nil {
			return err
		}

		token, err := settings.GetTokenCredential()
		if err != nil {
			return err
		}

		a.client, err = azservicebus.NewClient(a.metadata.NamespaceName, token, &azservicebus.ClientOptions{
			ApplicationID: userAgent,
		})
		if err != nil {
			return err
		}

		a.adminClient, err = admin.NewClient(a.metadata.NamespaceName, token, &admin.ClientOptions{})

		if err != nil {
			return err
		}
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())

	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	var sender *azservicebus.Sender
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
		sender, err = a.client.NewSender(req.Topic, &azservicebus.NewSenderOptions{})
		a.topics[req.Topic] = sender
		a.topicsLock.Unlock()

		if err != nil {
			return err
		}
	}

	a.logger.Infof("Creating message with body: %s", string(req.Data))
	msg, err := NewASBMessageFromPubsubRequest(req)
	if err != nil {
		return err
	}

	return a.doPublish(sender, msg)
}

func (a *azureServiceBus) doPublish(sender *azservicebus.Sender, msg *azservicebus.Message) error {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Duration(a.metadata.PublishInitialRetryIntervalInMs) * time.Millisecond
	bo := backoff.WithMaxRetries(ebo, uint64(a.metadata.PublishMaxRetries))
	bo = backoff.WithContext(bo, a.ctx)

	return retry.NotifyRecover(func() error {
		ctx, cancel := context.WithTimeout(a.ctx, time.Second*time.Duration(a.metadata.TimeoutInSec))
		defer cancel()

		err := sender.SendMessage(ctx, msg)
		if err == nil {
			return nil
		}

		var amqpError *amqp.Error
		if errors.As(err, &amqpError) {
			if _, ok := retriableSendingErrors[amqpError.Condition]; ok {
				return amqpError // Retries.
			}
		}

		if errors.Is(err, amqp.ErrConnClosed) {
			return err // Retries.
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
			subEntity, err := a.client.NewReceiverForSubscription(req.Topic, subID, &azservicebus.ReceiverOptions{})
			if err != nil {
				a.logger.Errorf("%s could not instantiate subscription %s for topic %s", errorMessagePrefix, subID, req.Topic)

				return
			}
			sub := newSubscription(req.Topic, subEntity, a.metadata.MaxConcurrentHandlers, a.metadata.PrefetchCount, a.logger)

			a.client.NewReceiverForSubscription(sub.topic, subID, &azservicebus.ReceiverOptions{})
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
	shouldCreate, err := a.shouldCreateTopic(topic)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = a.createTopic(topic)
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

	shouldCreate, err := a.shouldCreateSubscription(a.adminClient, topic, name)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = a.createSubscription(a.adminClient, topic, name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *azureServiceBus) shouldCreateTopic(topic string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	if a.adminClient == nil {
		return false, fmt.Errorf("%s init() has not been called", errorMessagePrefix)
	}
	_, err := a.adminClient.GetTopic(ctx, topic, &admin.GetTopicOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "entity does not exist") {
			return true, nil
		}
		return false, fmt.Errorf("%s could not get topic %s, %s", errorMessagePrefix, topic, err.Error())
	}

	return false, nil
}

func (a *azureServiceBus) createTopic(topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err := a.adminClient.CreateTopic(ctx, topic, &admin.TopicProperties{}, &admin.CreateTopicOptions{})
	if err != nil {
		return fmt.Errorf("%s could not put topic %s, %s", errorMessagePrefix, topic, err)
	}

	return nil
}

func (a *azureServiceBus) shouldCreateSubscription(mgr *admin.Client, topic, subscription string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err := mgr.GetSubscription(ctx, topic, subscription, &admin.GetSubscriptionOptions{})
	if err != nil {
		a.logger.Infof("Could not get subscription - %s", err.Error())
		if strings.Contains(err.Error(), "was not found") {
			return true, nil
		}
		return false, fmt.Errorf("%s could not get subscription %s, %s", errorMessagePrefix, subscription, err)
	}

	return false, nil
}

func (a *azureServiceBus) createSubscription(mgr *admin.Client, topic, subscription string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	props, err := a.createSubscriptionProperties()
	if err != nil {
		return err
	}

	_, err = mgr.CreateSubscription(ctx, topic, subscription, props, &admin.CreateSubscriptionOptions{})
	if err != nil {
		return fmt.Errorf("%s could not put subscription %s, %s", errorMessagePrefix, subscription, err)
	}

	return nil
}

func (a *azureServiceBus) createSubscriptionProperties() (*admin.SubscriptionProperties, error) {
	properties := &admin.SubscriptionProperties{}

	if a.metadata.MaxDeliveryCount != nil {
		maxDeliveryCount := int32(*a.metadata.MaxDeliveryCount)
		properties.MaxDeliveryCount = &maxDeliveryCount
	}

	if a.metadata.LockDurationInSec != nil {
		lockDuration := time.Second * time.Duration(*a.metadata.LockDurationInSec)
		properties.LockDuration = &lockDuration
	}

	if a.metadata.DefaultMessageTimeToLiveInSec != nil {
		defaultMessageTimeToLive := time.Second * time.Duration(*a.metadata.DefaultMessageTimeToLiveInSec)
		properties.DefaultMessageTimeToLive = &defaultMessageTimeToLive
	}

	if a.metadata.AutoDeleteOnIdleInSec != nil {
		autoDeleteOnIdle := time.Second * time.Duration(*a.metadata.AutoDeleteOnIdleInSec)
		properties.AutoDeleteOnIdle = &autoDeleteOnIdle
	}

	return properties, nil
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
