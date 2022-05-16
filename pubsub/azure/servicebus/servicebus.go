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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
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
	maxActiveMessages               = "maxActiveMessages"
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

type azureServiceBus struct {
	metadata    metadata
	client      *servicebus.Client
	adminClient *sbadmin.Client
	logger      logger.Logger
	features    []pubsub.Feature
	topics      map[string]*servicebus.Sender
	topicsLock  *sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation.
func NewAzureServiceBus(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger:     logger,
		features:   []pubsub.Feature{pubsub.FeatureMessageTTL},
		topics:     map[string]*servicebus.Sender{},
		topicsLock: &sync.RWMutex{},
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

func (a *azureServiceBus) Init(metadata pubsub.Metadata) (err error) {
	a.metadata, err = parseAzureServiceBusMetadata(metadata)
	if err != nil {
		return err
	}

	userAgent := "dapr-" + logger.DaprVersion
	if a.metadata.ConnectionString != "" {
		a.client, err = servicebus.NewClientFromConnectionString(a.metadata.ConnectionString, &servicebus.ClientOptions{
			ApplicationID: userAgent,
		})
		if err != nil {
			return err
		}

		a.adminClient, err = sbadmin.NewClientFromConnectionString(a.metadata.ConnectionString, nil)
		if err != nil {
			return err
		}
	} else {
		settings, innerErr := azauth.NewEnvironmentSettings(azauth.AzureServiceBusResourceName, metadata.Properties)
		if innerErr != nil {
			return innerErr
		}

		token, innerErr := settings.GetTokenCredential()
		if innerErr != nil {
			return innerErr
		}

		a.client, innerErr = servicebus.NewClient(a.metadata.NamespaceName, token, &servicebus.ClientOptions{
			ApplicationID: userAgent,
		})
		if innerErr != nil {
			return innerErr
		}

		a.adminClient, innerErr = sbadmin.NewClient(a.metadata.NamespaceName, token, nil)
		if innerErr != nil {
			return innerErr
		}
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())

	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	var sender *servicebus.Sender
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
		sender, err = a.client.NewSender(req.Topic, nil)
		a.topics[req.Topic] = sender
		a.topicsLock.Unlock()

		if err != nil {
			return err
		}
	}

	// a.logger.Debugf("Creating message with body: %s", string(req.Data))
	msg, err := NewASBMessageFromPubsubRequest(req)
	if err != nil {
		return err
	}

	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Duration(a.metadata.PublishInitialRetryIntervalInMs) * time.Millisecond
	bo := backoff.WithMaxRetries(ebo, uint64(a.metadata.PublishMaxRetries))
	bo = backoff.WithContext(bo, a.ctx)

	msgID := "nil"
	if msg.MessageID != nil {
		msgID = *msg.MessageID
	}
	return retry.NotifyRecover(
		func() (err error) {
			ctx, cancel := context.WithTimeout(a.ctx, time.Second*time.Duration(a.metadata.TimeoutInSec))
			defer cancel()

			err = sender.SendMessage(ctx, msg, nil)
			if err != nil {
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
			}
			return nil
		},
		bo,
		func(err error, _ time.Duration) {
			a.logger.Debugf("Could not publish service bus message (%s). Retrying...: %v", msgID, err)
		},
		func() {
			a.logger.Debugf("Successfully published service bus message (%s) after it previously failed", msgID)
		},
	)
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
			subEntity, err := a.client.NewReceiverForSubscription(req.Topic, subID, nil)
			if err != nil {
				a.logger.Errorf("%s could not instantiate subscription %s for topic %s", errorMessagePrefix, subID, req.Topic)
				return
			}
			sub := newSubscription(
				a.ctx,
				req.Topic,
				subEntity,
				a.metadata.MaxActiveMessages,
				a.metadata.TimeoutInSec,
				a.metadata.HandlerTimeoutInSec,
				a.metadata.MaxConcurrentHandlers,
				a.logger,
			)

			// ReceiveAndBlock will only return with an error
			// that it cannot handle internally. The subscription
			// connection is closed when this method returns.
			// If that occurs, we will log the error and attempt
			// to re-establish the subscription connection until
			// we exhaust the number of reconnect attempts.
			innerErr := sub.ReceiveAndBlock(
				handler,
				a.metadata.LockRenewalInSec,
			)
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
			sub.close(ctx)
			cancel()

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

	shouldCreate, err := a.shouldCreateSubscription(topic, name)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = a.createSubscription(topic, name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *azureServiceBus) shouldCreateTopic(topic string) (bool, error) {
	ctx, cancel := context.WithTimeout(a.ctx, time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	if a.adminClient == nil {
		return false, fmt.Errorf("%s init() has not been called", errorMessagePrefix)
	}
	res, err := a.adminClient.GetTopic(ctx, topic, nil)
	if err != nil {
		return false, fmt.Errorf("%s could not get topic %s, %s", errorMessagePrefix, topic, err.Error())
	}
	if res == nil {
		// If res is nil, the topic does not exist
		return true, nil
	}

	return false, nil
}

func (a *azureServiceBus) createTopic(topic string) error {
	ctx, cancel := context.WithTimeout(a.ctx, time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err := a.adminClient.CreateTopic(ctx, topic, nil)
	if err != nil {
		return fmt.Errorf("%s could not create topic %s, %s", errorMessagePrefix, topic, err)
	}

	return nil
}

func (a *azureServiceBus) shouldCreateSubscription(topic, subscription string) (bool, error) {
	ctx, cancel := context.WithTimeout(a.ctx, time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	res, err := a.adminClient.GetSubscription(ctx, topic, subscription, nil)
	if err != nil {
		return false, fmt.Errorf("%s could not get subscription %s, %s", errorMessagePrefix, subscription, err)
	}
	if res == nil {
		// If res is subscription, the topic does not exist
		return true, nil
	}

	return false, nil
}

func (a *azureServiceBus) createSubscription(topic, subscription string) error {
	props, err := a.createSubscriptionProperties()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(a.ctx, time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err = a.adminClient.CreateSubscription(ctx, topic, subscription, &sbadmin.CreateSubscriptionOptions{
		Properties: props,
	})
	if err != nil {
		return fmt.Errorf("%s could not create subscription %s, %s", errorMessagePrefix, subscription, err)
	}

	return nil
}

func (a *azureServiceBus) createSubscriptionProperties() (*sbadmin.SubscriptionProperties, error) {
	properties := &sbadmin.SubscriptionProperties{}

	if a.metadata.MaxDeliveryCount != nil {
		maxDeliveryCount := int32(*a.metadata.MaxDeliveryCount)
		properties.MaxDeliveryCount = &maxDeliveryCount
	}

	if a.metadata.LockDurationInSec != nil {
		lockDuration := contrib_metadata.Duration{
			Duration: time.Duration(*a.metadata.LockDurationInSec) * time.Second,
		}
		properties.LockDuration = to.Ptr(lockDuration.ToISOString())
	}

	if a.metadata.DefaultMessageTimeToLiveInSec != nil {
		defaultMessageTimeToLive := contrib_metadata.Duration{
			Duration: time.Duration(*a.metadata.DefaultMessageTimeToLiveInSec) * time.Second,
		}
		properties.DefaultMessageTimeToLive = to.Ptr(defaultMessageTimeToLive.ToISOString())
	}

	if a.metadata.AutoDeleteOnIdleInSec != nil {
		autoDeleteOnIdle := contrib_metadata.Duration{
			Duration: time.Duration(*a.metadata.AutoDeleteOnIdleInSec) * time.Second,
		}
		properties.AutoDeleteOnIdle = to.Ptr(autoDeleteOnIdle.ToISOString())
	}

	return properties, nil
}

func (a *azureServiceBus) Close() error {
	// Cancel the context which will stop all subscriptions too
	a.cancel()

	// Close all topics
	var ctx context.Context
	var cancel context.CancelFunc
	var err error
	for _, t := range a.topics {
		a.logger.Debugf("Closing topic %s", t)
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(a.metadata.TimeoutInSec)*time.Second)
		err = t.Close(ctx)
		if err != nil {
			// Log only
			a.logger.Warnf("%s closing topic %s: %+v", errorMessagePrefix, t, err)
		}
		cancel()
	}

	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return a.features
}
