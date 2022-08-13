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

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	errorMessagePrefix = "azure service bus error:"
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

	publishCtx    context.Context
	publishCancel context.CancelFunc
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

func parseAzureServiceBusMetadata(meta pubsub.Metadata, logger logger.Logger) (metadata, error) {
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

	m.MaxRetriableErrorsPerSec = defaultMaxRetriableErrorsPerSec
	if val, ok := meta.Properties[maxRetriableErrorsPerSec]; ok && val != "" {
		var err error
		m.MaxRetriableErrorsPerSec, err = strconv.Atoi(val)
		if err == nil && m.MaxRetriableErrorsPerSec < 0 {
			err = errors.New("must not be negative")
		}
		if err != nil {
			return m, fmt.Errorf("%s invalid maxRetriableErrorsPerSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	if val, ok := meta.Properties[minConnectionRecoveryInSec]; ok && val != "" {
		var err error
		m.MinConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid minConnectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	if val, ok := meta.Properties[maxConnectionRecoveryInSec]; ok && val != "" {
		var err error
		m.MaxConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxConnectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
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

	/* Deprecated properties - show a warning. */
	// TODO: Remove in the future
	if _, ok := meta.Properties[connectionRecoveryInSec]; ok && logger != nil {
		logger.Warn("pubsub.azure.servicebus: metadata property 'connectionRecoveryInSec' has been deprecated and is now ignored - use 'minConnectionRecoveryInSec' and 'maxConnectionRecoveryInSec' instead. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/")
	}
	if _, ok := meta.Properties[maxReconnectionAttempts]; ok && logger != nil {
		logger.Warn("pubsub.azure.servicebus: metadata property 'maxReconnectionAttempts' has been deprecated and is now ignored. See: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus/")
	}

	return m, nil
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) (err error) {
	a.metadata, err = parseAzureServiceBusMetadata(metadata, a.logger)
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

	a.publishCtx, a.publishCancel = context.WithCancel(context.Background())

	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	sender, err := a.senderForTopic(a.publishCtx, req.Topic)
	if err != nil {
		return err
	}

	// a.logger.Debugf("Creating message with body: %s", string(req.Data))
	msg, err := NewASBMessageFromPubsubRequest(req)
	if err != nil {
		return err
	}

	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Duration(a.metadata.PublishInitialRetryIntervalInMs) * time.Millisecond
	bo := backoff.WithMaxRetries(ebo, uint64(a.metadata.PublishMaxRetries))
	bo = backoff.WithContext(bo, a.publishCtx)

	msgID := "nil"
	if msg.MessageID != nil {
		msgID = *msg.MessageID
	}
	return retry.NotifyRecover(
		func() (err error) {
			ctx, cancel := context.WithTimeout(a.publishCtx, time.Second*time.Duration(a.metadata.TimeoutInSec))
			defer cancel()

			err = sender.SendMessage(ctx, msg, nil)
			if err != nil {
				var amqpError *amqp.Error
				var expError *servicebus.Error
				if errors.As(err, &amqpError) {
					if _, ok := retriableSendingErrors[amqpError.Condition]; ok {
						return amqpError // Retries.
					}
				}

				if errors.Is(err, amqp.ErrConnClosed) {
					return err // Retries.
				}

				if errors.As(err, &expError) {
					if expError.Code == "connlost" {
						a.logger.Warn(expError.Error())
						return expError // Retries.
					}
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

func (a *azureServiceBus) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	subID := a.metadata.ConsumerID
	if !a.metadata.DisableEntityManagement {
		err := a.ensureSubscription(subscribeCtx, subID, req.Topic)
		if err != nil {
			return err
		}
	}

	// Reconnection backoff policy
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.InitialInterval = time.Duration(a.metadata.MinConnectionRecoveryInSec) * time.Second
	bo.MaxInterval = time.Duration(a.metadata.MaxConnectionRecoveryInSec) * time.Second

	go func() {
		// Reconnect loop.
		for {
			sub := impl.NewSubscription(
				subscribeCtx,
				a.metadata.MaxActiveMessages,
				a.metadata.TimeoutInSec,
				a.metadata.MaxRetriableErrorsPerSec,
				a.metadata.MaxConcurrentHandlers,
				"topic "+req.Topic,
				a.logger,
			)

			// Blocks until a successful connection (or until context is canceled)
			err := sub.Connect(func() (*servicebus.Receiver, error) {
				return a.client.NewReceiverForSubscription(req.Topic, subID, nil)
			})
			if err != nil {
				// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
				if err != context.Canceled {
					a.logger.Errorf("%s could not instantiate subscription %s for topic %s", errorMessagePrefix, subID, req.Topic)
				}
				return
			}

			// ReceiveAndBlock will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
			// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
			err = sub.ReceiveAndBlock(
				a.getHandlerFunc(req.Topic, handler),
				a.metadata.LockRenewalInSec,
				func() {
					// Reset the backoff when the subscription is successful and we have received the first message
					bo.Reset()
				},
			)
			if err != nil {
				var detachError *amqp.DetachError
				var amqpError *amqp.Error
				if errors.Is(err, detachError) ||
					(errors.As(err, &amqpError) && amqpError.Condition == amqp.ErrorDetachForced) {
					a.logger.Debug(err)
				} else {
					a.logger.Error(err)
				}
			}

			// Gracefully close the connection (in case it's not closed already)
			// Use a background context here (with timeout) because ctx may be closed already
			closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
			sub.Close(closeCtx)
			closeCancel()

			// If context was canceled, do not attempt to reconnect
			if subscribeCtx.Err() != nil {
				a.logger.Debug("Context canceled; will not reconnect")
				return
			}

			wait := bo.NextBackOff()
			a.logger.Warnf("Subscription to topic %s lost connection, attempting to reconnect in %s...", req.Topic, wait)
			time.Sleep(wait)
		}
	}()

	return nil
}

func (a *azureServiceBus) getHandlerFunc(topic string, handler pubsub.Handler) impl.HandlerFunc {
	return func(ctx context.Context, asbMsg *servicebus.ReceivedMessage) error {
		pubsubMsg, err := NewPubsubMessageFromASBMessage(asbMsg, topic)
		if err != nil {
			return fmt.Errorf("failed to get pubsub message from azure service bus message: %+v", err)
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, time.Duration(a.metadata.HandlerTimeoutInSec)*time.Second)
		defer handleCancel()
		a.logger.Debugf("Calling app's handler for message %s on topic %s", asbMsg.MessageID, topic)
		return handler(handleCtx, pubsubMsg)
	}
}

// senderForTopic returns the sender for a topic, or creates a new one if it doesn't exist
func (a *azureServiceBus) senderForTopic(ctx context.Context, topic string) (*servicebus.Sender, error) {
	a.topicsLock.RLock()
	sender, ok := a.topics[topic]
	a.topicsLock.RUnlock()
	if ok && sender != nil {
		return sender, nil
	}

	// Ensure the topic exists the first time it is referenced.
	var err error
	if !a.metadata.DisableEntityManagement {
		if err = a.ensureTopic(ctx, topic); err != nil {
			return nil, err
		}
	}
	a.topicsLock.Lock()
	defer a.topicsLock.Unlock()
	sender, err = a.client.NewSender(topic, nil)
	if err != nil {
		return nil, err
	}
	a.topics[topic] = sender

	return sender, nil
}

func (a *azureServiceBus) ensureTopic(ctx context.Context, topic string) error {
	shouldCreate, err := a.shouldCreateTopic(ctx, topic)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = a.createTopic(ctx, topic)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *azureServiceBus) ensureSubscription(ctx context.Context, name string, topic string) error {
	err := a.ensureTopic(ctx, topic)
	if err != nil {
		return err
	}

	shouldCreate, err := a.shouldCreateSubscription(ctx, topic, name)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = a.createSubscription(ctx, topic, name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *azureServiceBus) shouldCreateTopic(parentCtx context.Context, topic string) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(a.metadata.TimeoutInSec))
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

func (a *azureServiceBus) createTopic(parentCtx context.Context, topic string) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err := a.adminClient.CreateTopic(ctx, topic, nil)
	if err != nil {
		return fmt.Errorf("%s could not create topic %s, %s", errorMessagePrefix, topic, err)
	}

	return nil
}

func (a *azureServiceBus) shouldCreateSubscription(parentCtx context.Context, topic, subscription string) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(a.metadata.TimeoutInSec))
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

func (a *azureServiceBus) createSubscription(parentCtx context.Context, topic, subscription string) error {
	props, err := a.createSubscriptionProperties()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(a.metadata.TimeoutInSec))
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

func (a *azureServiceBus) Close() (err error) {
	a.topicsLock.Lock()
	defer a.topicsLock.Unlock()

	a.publishCancel()

	// Close all topics, up to 3 in parallel
	workersCh := make(chan bool, 3)
	for k, t := range a.topics {
		// Blocks if we have too many goroutines
		workersCh <- true
		go func(k string, t *servicebus.Sender) {
			a.logger.Debugf("Closing topic %s", k)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(a.metadata.TimeoutInSec)*time.Second)
			err = t.Close(ctx)
			cancel()
			if err != nil {
				// Log only
				a.logger.Warnf("%s closing topic %s: %+v", errorMessagePrefix, k, err)
			}
			<-workersCh
		}(k, t)
	}
	for i := 0; i < cap(workersCh); i++ {
		// Wait for all workers to be done
		workersCh <- true
	}
	close(workersCh)

	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return a.features
}
