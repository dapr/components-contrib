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

package pubsub

import "time"

// GCPPubSubMetaData pubsub metadata.
type metadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
	ConsumerID          string `mapstructure:"consumerID"              mdignore:"true"`
	Type                string `mapstructure:"type"                    mdignore:"true"`
	IdentityProjectID   string `mapstructure:"identityProjectID"       mdignore:"true"`
	ProjectID           string `mapstructure:"projectID"               mdignore:"true"`
	PrivateKeyID        string `mapstructure:"privateKeyID"            mdignore:"true"`
	PrivateKey          string `mapstructure:"privateKey"              mdignore:"true"`
	ClientEmail         string `mapstructure:"clientEmail"             mdignore:"true"`
	ClientID            string `mapstructure:"clientID"                mdignore:"true"`
	AuthURI             string `mapstructure:"authURI"                 mdignore:"true"`
	TokenURI            string `mapstructure:"tokenURI"                mdignore:"true"`
	AuthProviderCertURL string `mapstructure:"authProviderX509CertUrl" mdignore:"true"`
	ClientCertURL       string `mapstructure:"clientX509CertUrl"       mdignore:"true"`

	DisableEntityManagement  bool          `mapstructure:"disableEntityManagement"`
	EnableMessageOrdering    bool          `mapstructure:"enableMessageOrdering"`
	MaxReconnectionAttempts  int           `mapstructure:"maxReconnectionAttempts"`
	ConnectionRecoveryInSec  int           `mapstructure:"connectionRecoveryInSec"`
	ConnectionEndpoint       string        `mapstructure:"endpoint"`
	OrderingKey              string        `mapstructure:"orderingKey"`
	DeadLetterTopic          string        `mapstructure:"deadLetterTopic"`
	MaxDeliveryAttempts      int           `mapstructure:"maxDeliveryAttempts"`
	MaxOutstandingMessages   int           `mapstructure:"maxOutstandingMessages"`
	MaxOutstandingBytes      int           `mapstructure:"maxOutstandingBytes"`
	MaxConcurrentConnections int           `mapstructure:"maxConcurrentConnections"`
	AckDeadline              time.Duration `mapstructure:"ackDeadline"`
}
