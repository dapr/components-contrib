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

package pulsar

import (
	"time"

	"github.com/dapr/components-contrib/common/authentication/oauth2"
)

type pulsarMetadata struct {
	Host                             string                    `mapstructure:"host"`
	ConsumerID                       string                    `mapstructure:"consumerID"`
	EnableTLS                        bool                      `mapstructure:"enableTLS"`
	DisableBatching                  bool                      `mapstructure:"disableBatching"`
	BatchingMaxPublishDelay          time.Duration             `mapstructure:"batchingMaxPublishDelay"`
	BatchingMaxSize                  uint                      `mapstructure:"batchingMaxSize"`
	BatchingMaxMessages              uint                      `mapstructure:"batchingMaxMessages"`
	Tenant                           string                    `mapstructure:"tenant"`
	Namespace                        string                    `mapstructure:"namespace"`
	Persistent                       bool                      `mapstructure:"persistent"`
	RedeliveryDelay                  time.Duration             `mapstructure:"redeliveryDelay"`
	internalTopicSchemas             map[string]schemaMetadata `mapstructure:"-"`
	PublicKey                        string                    `mapstructure:"publicKey"`
	PrivateKey                       string                    `mapstructure:"privateKey"`
	Keys                             string                    `mapstructure:"keys"`
	MaxConcurrentHandlers            uint                      `mapstructure:"maxConcurrentHandlers"`
	ReceiverQueueSize                int                       `mapstructure:"receiverQueueSize"`
	SubscriptionType                 string                    `mapstructure:"subscribeType"`
	SubscriptionInitialPosition      string                    `mapstructure:"subscribeInitialPosition"`
	ReplicateSubscriptionState       bool                      `mapstructure:"replicateSubscriptionState"`
	SubscriptionMode                 string                    `mapstructure:"subscribeMode"`
	Token                            string                    `mapstructure:"token"`
	oauth2.ClientCredentialsMetadata `mapstructure:",squash"`
}

type schemaMetadata struct {
	protocol string
	value    string
}
