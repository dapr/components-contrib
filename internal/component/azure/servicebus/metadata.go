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
	"strconv"
	"time"

	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	"github.com/dapr/components-contrib/internal/utils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// Metadata options for Service Bus components.
// Note: AzureAD-related keys are handled separately.
type Metadata struct {
	/** For bindings and pubsubs **/
	ConnectionString                string `json:"connectionString"`
	ConsumerID                      string `json:"consumerID"` // Only topics
	TimeoutInSec                    int    `json:"timeoutInSec"`
	HandlerTimeoutInSec             int    `json:"handlerTimeoutInSec"`
	LockRenewalInSec                int    `json:"lockRenewalInSec"`
	MaxActiveMessages               int    `json:"maxActiveMessages"`
	MaxConnectionRecoveryInSec      int    `json:"maxConnectionRecoveryInSec"`
	MinConnectionRecoveryInSec      int    `json:"minConnectionRecoveryInSec"`
	DisableEntityManagement         bool   `json:"disableEntityManagement"`
	MaxRetriableErrorsPerSec        int    `json:"maxRetriableErrorsPerSec"`
	MaxDeliveryCount                *int32 `json:"maxDeliveryCount"`              // Only used during subscription creation - default is set by the server (10)
	LockDurationInSec               *int   `json:"lockDurationInSec"`             // Only used during subscription creation - default is set by the server (60s)
	DefaultMessageTimeToLiveInSec   *int   `json:"defaultMessageTimeToLiveInSec"` // Only used during subscription creation - default is set by the server (depends on the tier)
	AutoDeleteOnIdleInSec           *int   `json:"autoDeleteOnIdleInSec"`         // Only used during subscription creation - default is set by the server (disabled)
	MaxConcurrentHandlers           int    `json:"maxConcurrentHandlers"`
	PublishMaxRetries               int    `json:"publishMaxRetries"`
	PublishInitialRetryIntervalInMs int    `json:"publishInitialRetryInternalInMs"`
	NamespaceName                   string `json:"namespaceName"` // Only for Azure AD

	/** For bindings only **/
	QueueName string `json:"queueName"` // Only queues
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
	keyPublishInitialRetryInternalInMs = "publishInitialRetryInternalInMs"
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
	defaultPublishInitialRetryInternalInMs = 500
)

// Modes for ParseMetadata.
const (
	MetadataModeBinding byte = 1 << iota
	MetadataModeTopics

	MetadataModeQueues byte = 0
)

// ParseMetadata parses metadata keys that are common to all Service Bus components
func ParseMetadata(md map[string]string, logger logger.Logger, mode byte) (m *Metadata, err error) {
	m = &Metadata{}

	/* Required configuration settings - no defaults. */
	if val, ok := md[keyConnectionString]; ok && val != "" {
		m.ConnectionString = val

		// The connection string and the namespace cannot both be present.
		if namespace, present := md[keyNamespaceName]; present && namespace != "" {
			return m, errors.New("connectionString and namespaceName cannot both be specified")
		}
	} else if val, ok := md[keyNamespaceName]; ok && val != "" {
		m.NamespaceName = val
	} else {
		return m, errors.New("either one of connection string or namespace name are required")
	}

	if (mode & MetadataModeTopics) != 0 {
		if val, ok := md[keyConsumerID]; ok && val != "" {
			m.ConsumerID = val
		} else {
			return m, errors.New("missing consumerID")
		}
	}

	if (mode&MetadataModeBinding) != 0 && (mode&MetadataModeTopics) == 0 {
		if val, ok := md[keyQueueName]; ok && val != "" {
			m.QueueName = val
		} else {
			return m, errors.New("missing queueName")
		}
	}

	/* Optional configuration settings - defaults will be set by the client. */
	m.TimeoutInSec = defaultTimeoutInSec
	if val, ok := md[keyTimeoutInSec]; ok && val != "" {
		m.TimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid timeoutInSec %s: %s", val, err)
		}
	}

	m.DisableEntityManagement = defaultDisableEntityManagement
	if val, ok := md[keyDisableEntityManagement]; ok && val != "" {
		m.DisableEntityManagement = utils.IsTruthy(val)
	}

	if (mode & MetadataModeBinding) != 0 {
		m.HandlerTimeoutInSec = defaultHandlerTimeoutInSecBinding
	} else {
		m.HandlerTimeoutInSec = defaultHandlerTimeoutInSecPubSub
	}
	if val, ok := md[keyHandlerTimeoutInSec]; ok && val != "" {
		m.HandlerTimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid handlerTimeoutInSec %s: %s", val, err)
		}
	}

	m.LockRenewalInSec = defaultLockRenewalInSec
	if val, ok := md[keyLockRenewalInSec]; ok && val != "" {
		m.LockRenewalInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid lockRenewalInSec %s: %s", val, err)
		}
	}

	if (mode & MetadataModeBinding) != 0 {
		m.MaxActiveMessages = defaultMaxActiveMessagesBinding
	} else {
		m.MaxActiveMessages = defaultMaxActiveMessagesPubSub
	}
	if val, ok := md[keyMaxActiveMessages]; ok && val != "" {
		m.MaxActiveMessages, err = strconv.Atoi(val)
		if err == nil && m.MaxActiveMessages < 1 {
			err = errors.New("must be 1 or greater")
		}
		if err != nil {
			return m, fmt.Errorf("invalid maxActiveMessages %s: %s", val, err)
		}
	}

	m.MaxRetriableErrorsPerSec = defaultMaxRetriableErrorsPerSec
	if val, ok := md[keyMaxRetriableErrorsPerSec]; ok && val != "" {
		m.MaxRetriableErrorsPerSec, err = strconv.Atoi(val)
		if err == nil && m.MaxRetriableErrorsPerSec < 0 {
			err = errors.New("must not be negative")
		}
		if err != nil {
			return m, fmt.Errorf("invalid maxRetriableErrorsPerSec %s: %s", val, err)
		}
	}

	m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	if val, ok := md[keyMinConnectionRecoveryInSec]; ok && val != "" {
		m.MinConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid minConnectionRecoveryInSec %s: %s", val, err)
		}
	}

	m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	if val, ok := md[keyMaxConnectionRecoveryInSec]; ok && val != "" {
		m.MaxConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid maxConnectionRecoveryInSec %s: %s", val, err)
		}
	}

	if (mode & MetadataModeBinding) != 0 {
		m.MaxConcurrentHandlers = defaultMaxConcurrentHandlersBinding
	} else {
		m.MaxConcurrentHandlers = defaultMaxConcurrentHandlersPubSub
	}
	if val, ok := md[keyMaxConcurrentHandlers]; ok && val != "" {
		m.MaxConcurrentHandlers, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid maxConcurrentHandlers %s: %s", val, err)
		}
	}

	m.PublishMaxRetries = defaultPublishMaxRetries
	if val, ok := md[keyPublishMaxRetries]; ok && val != "" {
		m.PublishMaxRetries, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid publishMaxRetries %s: %s", val, err)
		}
	}

	m.PublishInitialRetryIntervalInMs = defaultPublishInitialRetryInternalInMs
	if val, ok := md[keyPublishInitialRetryInternalInMs]; ok && val != "" {
		m.PublishInitialRetryIntervalInMs, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid publishInitialRetryIntervalInMs %s: %s", val, err)
		}
	}

	/* Nullable configuration settings - defaults will be set by the server. */
	if val, ok := md[keyMaxDeliveryCount]; ok && val != "" {
		var valAsInt int64
		valAsInt, err = strconv.ParseInt(val, 10, 32)
		if err != nil {
			return m, fmt.Errorf("invalid maxDeliveryCount %s: %s", val, err)
		}
		m.MaxDeliveryCount = ptr.Of(int32(valAsInt))
	}

	if val, ok := md[keyLockDurationInSec]; ok && val != "" {
		var valAsInt int
		valAsInt, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid lockDurationInSec %s: %s", val, err)
		}
		m.LockDurationInSec = &valAsInt
	}

	if val, ok := md[keyDefaultMessageTimeToLiveInSec]; ok && val != "" {
		var valAsInt int
		valAsInt, err = strconv.Atoi(val)
		if err == nil && valAsInt < 0 {
			err = errors.New("must be greater than 0")
		}
		if err != nil {
			return m, fmt.Errorf("invalid defaultMessageTimeToLiveInSec %s: %s", val, err)
		}
		m.DefaultMessageTimeToLiveInSec = &valAsInt
	} else if val, ok := md[mdutils.TTLMetadataKey]; ok && val != "" {
		var valAsInt int
		valAsInt, err = strconv.Atoi(val)
		if err == nil && valAsInt < 0 {
			err = errors.New("must be greater than 0")
		}
		if err != nil {
			return m, fmt.Errorf("invalid %s %s: %s", mdutils.TTLMetadataKey, val, err)
		}
		m.DefaultMessageTimeToLiveInSec = &valAsInt
	}

	if val, ok := md[keyAutoDeleteOnIdleInSec]; ok && val != "" {
		var valAsInt int
		valAsInt, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("invalid autoDeleteOnIdleInSecKey %s: %s", val, err)
		}
		m.AutoDeleteOnIdleInSec = &valAsInt
	}

	return m, nil
}

// CreateSubscriptionProperties returns the SubscriptionProperties object to create new Subscriptions to Service Bus topics.
func (a Metadata) CreateSubscriptionProperties() *sbadmin.SubscriptionProperties {
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
