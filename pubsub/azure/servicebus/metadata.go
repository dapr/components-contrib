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
	MaxActiveMessagesRecoveryInSec  int    `json:"maxActiveMessagesRecoveryInSec"`
	MaxReconnectionAttempts         int    `json:"maxReconnectionAttempts"`
	ConnectionRecoveryInSec         int    `json:"connectionRecoveryInSec"`
	DisableEntityManagement         bool   `json:"disableEntityManagement"`
	MaxDeliveryCount                *int   `json:"maxDeliveryCount"`
	LockDurationInSec               *int   `json:"lockDurationInSec"`
	DefaultMessageTimeToLiveInSec   *int   `json:"defaultMessageTimeToLiveInSec"`
	AutoDeleteOnIdleInSec           *int   `json:"autoDeleteOnIdleInSec"`
	MaxConcurrentHandlers           *int   `json:"maxConcurrentHandlers"`
	PrefetchCount                   *int   `json:"prefetchCount"`
	PublishMaxRetries               int    `json:"publishMaxRetries"`
	PublishInitialRetryIntervalInMs int    `json:"publishInitialRetryInternalInMs"`
	NamespaceName                   string `json:"namespaceName,omitempty"`
}
