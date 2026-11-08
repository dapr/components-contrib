/*
Copyright 2026 The Dapr Authors
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

package googleai

import (
	"github.com/dapr/components-contrib/conversation"
)

// GoogleAILangchainMetadata extends LangchainMetadata with GoogleAI-specific properties.
type GoogleAILangchainMetadata struct {
	conversation.LangchainMetadata `json:",inline" mapstructure:",squash"`

	// When WorkloadIdentityProvider is set, the component authenticates with
	// Google Cloud workload identity federation, exchanging a JWT SVID minted by
	// the Dapr runtime for a Google access token, instead of using Key.
	// It is the full resource name of the workload identity pool provider, and
	// is also the audience of the JWT SVID.
	WorkloadIdentityProvider string `json:"workloadIdentityProvider" mapstructure:"workloadIdentityProvider"`
	// ServiceAccountEmail, when set, is impersonated with the federated token.
	ServiceAccountEmail string `json:"serviceAccountEmail" mapstructure:"serviceAccountEmail"`
}
