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

// GCPPubSubMetaData pubsub metadata.
type metadata struct {
	ConsumerID              string `mapstructure:"consumerID"`
	Type                    string `mapstructure:"type"`
	IdentityProjectID       string `mapstructure:"identityProjectID"`
	ProjectID               string `mapstructure:"projectID"`
	PrivateKeyID            string `mapstructure:"privateKeyID"`
	PrivateKey              string `mapstructure:"privateKey"`
	ClientEmail             string `mapstructure:"clientEmail"`
	ClientID                string `mapstructure:"clientID"`
	AuthURI                 string `mapstructure:"authURI"`
	TokenURI                string `mapstructure:"tokenURI"`
	AuthProviderCertURL     string `mapstructure:"authProviderX509CertUrl"`
	ClientCertURL           string `mapstructure:"clientX509CertUrl"`
	DisableEntityManagement bool   `mapstructure:"disableEntityManagement"`
	EnableMessageOrdering   bool   `mapstructure:"enableMessageOrdering"`
	MaxReconnectionAttempts int    `mapstructure:"maxReconnectionAttempts"`
	ConnectionRecoveryInSec int    `mapstructure:"connectionRecoveryInSec"`
}
