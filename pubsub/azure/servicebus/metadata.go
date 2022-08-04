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

// Reference for settings:
// https://github.com/Azure/azure-service-bus-go/blob/54b2faa53e5216616e59725281be692acc120c34/subscription_manager.go#L101
type metadata struct {
	ConnectionString                string `json:"connectionString"`
	ConsumerID                      string `json:"consumerID"`
	TimeoutInSec                    int    `json:"timeoutInSec"`
	HandlerTimeoutInSec             int    `json:"handlerTimeoutInSec"`
	LockRenewalInSec                int    `json:"lockRenewalInSec"`
	MaxActiveMessages               int    `json:"maxActiveMessages"`
	MaxConnectionRecoveryInSec      int    `json:"maxConnectionRecoveryInSec"`
	MinConnectionRecoveryInSec      int    `json:"minConnectionRecoveryInSec"`
	DisableEntityManagement         bool   `json:"disableEntityManagement"`
	MaxRetriableErrorsPerSec        int    `json:"maxRetriableErrorsPerSec"`
	MaxDeliveryCount                *int   `json:"maxDeliveryCount"`
	LockDurationInSec               *int   `json:"lockDurationInSec"`
	DefaultMessageTimeToLiveInSec   *int   `json:"defaultMessageTimeToLiveInSec"`
	AutoDeleteOnIdleInSec           *int   `json:"autoDeleteOnIdleInSec"`
	MaxConcurrentHandlers           *int   `json:"maxConcurrentHandlers"`
	PublishMaxRetries               int    `json:"publishMaxRetries"`
	PublishInitialRetryIntervalInMs int    `json:"publishInitialRetryInternalInMs"`
	NamespaceName                   string `json:"namespaceName,omitempty"`
	InitialTopic                    string `json:"initialTopic"`
}

const (
	// Keys.
	connectionString                = "connectionString"
	consumerID                      = "consumerID"
	timeoutInSec                    = "timeoutInSec"
	handlerTimeoutInSec             = "handlerTimeoutInSec"
	lockRenewalInSec                = "lockRenewalInSec"
	maxActiveMessages               = "maxActiveMessages"
	maxConnectionRecoveryInSec      = "maxConnectionRecoveryInSec"
	minConnectionRecoveryInSec      = "minConnectionRecoveryInSec"
	disableEntityManagement         = "disableEntityManagement"
	maxRetriableErrorsPerSec        = "maxRetriableErrorsPerSec"
	maxDeliveryCount                = "maxDeliveryCount"
	lockDurationInSec               = "lockDurationInSec"
	defaultMessageTimeToLiveInSec   = "defaultMessageTimeToLiveInSec"
	autoDeleteOnIdleInSec           = "autoDeleteOnIdleInSec"
	maxConcurrentHandlers           = "maxConcurrentHandlers"
	publishMaxRetries               = "publishMaxRetries"
	publishInitialRetryInternalInMs = "publishInitialRetryInternalInMs"
	namespaceName                   = "namespaceName"
	initialTopic                    = "initialTopic"

	// Deprecated keys.
	maxReconnectionAttempts = "maxReconnectionAttempts"
	connectionRecoveryInSec = "connectionRecoveryInSec"

	// Defaults.
	defaultTimeoutInSec             = 60
	defaultHandlerTimeoutInSec      = 60
	defaultLockRenewalInSec         = 20
	defaultMaxRetriableErrorsPerSec = 10
	// ASB Messages can be up to 256Kb. 10000 messages at this size would roughly use 2.56Gb.
	// We should change this if performance testing suggests a more sensible default.
	defaultMaxActiveMessages               = 10000
	defaultDisableEntityManagement         = false
	defaultMinConnectionRecoveryInSec      = 2
	defaultMaxConnectionRecoveryInSec      = 300
	defaultPublishMaxRetries               = 5
	defaultPublishInitialRetryInternalInMs = 500
)
