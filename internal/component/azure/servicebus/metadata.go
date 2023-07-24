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
	"errors"
	"fmt"
	"time"

	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// Metadata options for Service Bus components.
// Note: AzureAD-related keys are handled separately.
type Metadata struct {
	/** For bindings and pubsubs **/
	ConnectionString                string `mapstructure:"connectionString"`
	ConsumerID                      string `mapstructure:"consumerID"` // Only topics
	TimeoutInSec                    int    `mapstructure:"timeoutInSec"`
	HandlerTimeoutInSec             int    `mapstructure:"handlerTimeoutInSec"`
	LockRenewalInSec                int    `mapstructure:"lockRenewalInSec"`
	MaxActiveMessages               int    `mapstructure:"maxActiveMessages"`
	MaxConnectionRecoveryInSec      int    `mapstructure:"maxConnectionRecoveryInSec"`
	MinConnectionRecoveryInSec      int    `mapstructure:"minConnectionRecoveryInSec"`
	DisableEntityManagement         bool   `mapstructure:"disableEntityManagement"`
	MaxRetriableErrorsPerSec        int    `mapstructure:"maxRetriableErrorsPerSec"`
	MaxDeliveryCount                *int32 `mapstructure:"maxDeliveryCount"`              // Only used during subscription creation - default is set by the server (10)
	LockDurationInSec               *int   `mapstructure:"lockDurationInSec"`             // Only used during subscription creation - default is set by the server (60s)
	DefaultMessageTimeToLiveInSec   *int   `mapstructure:"defaultMessageTimeToLiveInSec"` // Only used during subscription creation - default is set by the server (depends on the tier)
	AutoDeleteOnIdleInSec           *int   `mapstructure:"autoDeleteOnIdleInSec"`         // Only used during subscription creation - default is set by the server (disabled)
	MaxConcurrentHandlers           int    `mapstructure:"maxConcurrentHandlers"`
	PublishMaxRetries               int    `mapstructure:"publishMaxRetries"`
	PublishInitialRetryIntervalInMs int    `mapstructure:"publishInitialRetryIntervalInMs"`
	NamespaceName                   string `mapstructure:"namespaceName"` // Only for Azure AD

	/** For bindings only **/
	QueueName string `mapstructure:"queueName" mdonly:"bindings"` // Only queues
}

// Keys.
const (
	keyConnectionString                = "connectionString"
	keyConsumerID                      = "consumerID"
	keyTimeoutInSec                    = "timeoutInSec"
	keyHandlerTimeoutInSec             = "handlerTimeoutInSec"
	keyLockRenewalInSec                = "lockRenewalInSec"
	keyMaxActiveMessages               = "maxActiveMessages"
	keyMaxConnectionRecoveryInSec      = "maxConnectionRecoveryInSec"
	keyMinConnectionRecoveryInSec      = "minConnectionRecoveryInSec"
	keyDisableEntityManagement         = "disableEntityManagement"
	keyMaxRetriableErrorsPerSec        = "maxRetriableErrorsPerSec"
	keyMaxDeliveryCount                = "maxDeliveryCount"
	keyLockDurationInSec               = "lockDurationInSec"
	keyDefaultMessageTimeToLiveInSec   = "defaultMessageTimeToLiveInSec" // Alias: "ttlInSeconds" (mdutils.TTLMetadataKey)
	keyAutoDeleteOnIdleInSec           = "autoDeleteOnIdleInSec"
	keyMaxConcurrentHandlers           = "maxConcurrentHandlers"
	keyPublishMaxRetries               = "publishMaxRetries"
	keyPublishInitialRetryIntervalInMs = "publishInitialRetryIntervalInMs" // Alias: "publishInitialRetryInternalInMs" (backwards compatibility due to typo)
	keyNamespaceName                   = "namespaceName"
	keyQueueName                       = "queueName"
)

// Defaults.
const (
	// Default timeout for network requests.
	defaultTimeoutInSec = 60

	// Default timeout for handlers.
	defaultHandlerTimeoutInSecPubSub  = 60
	defaultHandlerTimeoutInSecBinding = 0 // No timeout for handlers in bindings.

	// Default lock renewal interval, in seconds.
	defaultLockRenewalInSec = 20

	// Default rate of retriable errors per second.
	defaultMaxRetriableErrorsPerSec = 10

	// Default number of maximum active messages.
	defaultMaxActiveMessagesPubSub  = 1000
	defaultMaxActiveMessagesBinding = 1

	defaultDisableEntityManagement = false

	// Default minimum and maximum recovery time while trying to reconnect.
	defaultMinConnectionRecoveryInSec = 2
	defaultMaxConnectionRecoveryInSec = 300

	// Max concurrent handlers.
	defaultMaxConcurrentHandlersPubSub  = 0 // No limit for pubsubs.
	defaultMaxConcurrentHandlersBinding = 1 // Limited to 1 for backwards-compatibility.

	defaultPublishMaxRetries               = 5
	defaultPublishInitialRetryIntervalInMs = 500
)

// Modes for ParseMetadata.
const (
	MetadataModeBinding byte = 1 << iota
	MetadataModeTopics

	MetadataModeQueues byte = 0
)

// ParseMetadata parses metadata keys that are common to all Service Bus components
func ParseMetadata(md map[string]string, logger logger.Logger, mode byte) (m *Metadata, err error) {
	m = &Metadata{
		TimeoutInSec:                    defaultTimeoutInSec,
		LockRenewalInSec:                defaultLockRenewalInSec,
		MaxActiveMessages:               defaultMaxActiveMessagesPubSub,
		MaxConnectionRecoveryInSec:      defaultMaxConnectionRecoveryInSec,
		MinConnectionRecoveryInSec:      defaultMinConnectionRecoveryInSec,
		DisableEntityManagement:         defaultDisableEntityManagement,
		MaxRetriableErrorsPerSec:        defaultMaxRetriableErrorsPerSec,
		MaxConcurrentHandlers:           defaultMaxConcurrentHandlersPubSub,
		PublishMaxRetries:               defaultPublishMaxRetries,
		PublishInitialRetryIntervalInMs: defaultPublishInitialRetryIntervalInMs,
	}

	if (mode & MetadataModeBinding) != 0 {
		m.HandlerTimeoutInSec = defaultHandlerTimeoutInSecBinding
		m.MaxActiveMessages = defaultMaxActiveMessagesBinding
		m.MaxConcurrentHandlers = defaultMaxConcurrentHandlersBinding
	} else {
		m.HandlerTimeoutInSec = defaultHandlerTimeoutInSecPubSub
		m.MaxActiveMessages = defaultMaxActiveMessagesPubSub
		m.MaxConcurrentHandlers = defaultMaxConcurrentHandlersPubSub
	}

	// upgrade deprecated metadata keys

	if val, ok := md["publishInitialRetryInternalInMs"]; ok && val != "" {
		// TODO: Remove in a future Dapr release
		logger.Warn("Found deprecated metadata property 'publishInitialRetryInternalInMs'; please use 'publishInitialRetryIntervalInMs'")
		md["publishInitialRetryIntervalInMs"] = val
		delete(md, "publishInitialRetryInternalInMs")
	}

	mdErr := mdutils.DecodeMetadata(md, &m)
	if mdErr != nil {
		return m, mdErr
	}

	/* Required configuration settings - no defaults. */
	if m.ConnectionString != "" {
		// The connection string and the namespace cannot both be present.
		if m.NamespaceName != "" {
			return m, errors.New("connectionString and namespaceName cannot both be specified")
		}
	} else if m.NamespaceName == "" {
		return m, errors.New("either one of connection string or namespace name are required")
	}

	if (mode & MetadataModeTopics) != 0 {
		if m.ConsumerID == "" {
			return m, errors.New("missing consumerID")
		}
	}

	if (mode&MetadataModeBinding) != 0 && (mode&MetadataModeTopics) == 0 {
		if m.QueueName == "" {
			return m, errors.New("missing queueName")
		}
	}

	if m.MaxActiveMessages < 1 {
		err = errors.New("must be 1 or greater")
		return m, err
	}

	if m.MaxRetriableErrorsPerSec < 0 {
		err = errors.New("must not be negative")
		return m, err
	}

	/* Nullable configuration settings - defaults will be set by the server. */

	if m.DefaultMessageTimeToLiveInSec == nil {
		duration, found, ttlErr := mdutils.TryGetTTL(md)
		if ttlErr != nil {
			return m, fmt.Errorf("invalid %s %s: %s", mdutils.TTLMetadataKey, duration, ttlErr)
		}
		if found {
			m.DefaultMessageTimeToLiveInSec = ptr.Of(int(duration.Seconds()))
		}
	}

	if m.DefaultMessageTimeToLiveInSec != nil && *m.DefaultMessageTimeToLiveInSec == 0 {
		return m, errors.New("defaultMessageTimeToLiveInSec must be greater than 0")
	}

	return m, nil
}

// CreateSubscriptionProperties returns the SubscriptionProperties object to create new Subscriptions to Service Bus topics.
func (a Metadata) CreateSubscriptionProperties(opts SubscribeOptions) *sbadmin.SubscriptionProperties {
	properties := &sbadmin.SubscriptionProperties{}

	if a.MaxDeliveryCount != nil {
		properties.MaxDeliveryCount = a.MaxDeliveryCount
	}

	if a.LockDurationInSec != nil {
		properties.LockDuration = toDurationISOString(*a.LockDurationInSec)
	}

	if a.DefaultMessageTimeToLiveInSec != nil {
		properties.DefaultMessageTimeToLive = toDurationISOString(*a.DefaultMessageTimeToLiveInSec)
	}

	if a.AutoDeleteOnIdleInSec != nil {
		properties.AutoDeleteOnIdle = toDurationISOString(*a.AutoDeleteOnIdleInSec)
	}

	if opts.RequireSessions {
		properties.RequiresSession = ptr.Of(true)
	}

	return properties
}

// CreateQueueProperties returns the QueueProperties object to create new Queues in Service Bus.
func (a Metadata) CreateQueueProperties() *sbadmin.QueueProperties {
	properties := &sbadmin.QueueProperties{}

	if a.MaxDeliveryCount != nil {
		properties.MaxDeliveryCount = a.MaxDeliveryCount
	}

	if a.LockDurationInSec != nil {
		properties.LockDuration = toDurationISOString(*a.LockDurationInSec)
	}

	if a.DefaultMessageTimeToLiveInSec != nil {
		properties.DefaultMessageTimeToLive = toDurationISOString(*a.DefaultMessageTimeToLiveInSec)
	}

	if a.AutoDeleteOnIdleInSec != nil {
		properties.AutoDeleteOnIdle = toDurationISOString(*a.AutoDeleteOnIdleInSec)
	}

	return properties
}

func toDurationISOString(valInSec int) *string {
	valDuration := mdutils.Duration{
		Duration: time.Duration(valInSec) * time.Second,
	}
	return ptr.Of(valDuration.ToISOString())
}
