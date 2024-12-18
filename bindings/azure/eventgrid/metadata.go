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

package eventgrid

type eventgridMetadata struct {
	// Component Name
	Name string `json:"-" mapstructure:"-"`

	// The HTTPS endpoint of the webhook Event Grid sends events (formatted as Cloud Events) to. If you're not re-writing URLs on ingress, it should be	in the form of: `"https://[YOUR HOSTNAME]/<path>"` If testing on your local machine, you can use something like `ngrok` to create a public endpoint.
	SubscriberEndpoint string `json:"subscriberEndpoint" mapstructure:"subscriberEndpoint" binding:"input"`
	// The container port that the input binding listens on when receiving events on the webhook
	HandshakePort string `json:"handshakePort" mapstructure:"handshakePort" binding:"input"`
	// The identifier of the resource to which the event subscription needs to be created or updated.
	Scope string `json:"scope" mapstructure:"scope" binding:"input"`

	// The name of the event subscription. Event subscription names must be between 3 and 64 characters long and should use alphanumeric letters only.
	EventSubscriptionName string `json:"eventSubscriptionName,omitempty" mapstructure:"eventSubscriptionName" binding:"input"`

	// The Access Key to be used for publishing an Event Grid Event to a custom topic
	AccessKey string `json:"accessKey" mapstructure:"accessKey" binding:"output"`
	// The topic endpoint in which this output binding should publish events
	TopicEndpoint string `json:"topicEndpoint" mapstructure:"topicEndpoint" binding:"output"`

	// Internal
	azureTenantID       string            `mdignore:"true"` // Accepted values include: azureTenantID or tenantID
	azureClientID       string            `mdignore:"true"` // Accepted values include: azureClientID or clientID
	azureSubscriptionID string            `mdignore:"true"` // Accepted values: azureSubscriptionID or subscriptionID
	subscriberPath      string            `mdignore:"true"`
	properties          map[string]string `mdignore:"true"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func Defaults() eventgridMetadata {
	return eventgridMetadata{
		HandshakePort: "8080",
	}
}

// Note: we do not include any mdignored field.
func Examples() eventgridMetadata {
	return eventgridMetadata{
		SubscriberEndpoint:    "https://[YOUR HOSTNAME]/<path>",
		HandshakePort:         "9000",
		EventSubscriptionName: "/subscriptions/{subscriptionId}/",
		AccessKey:             "accessKey",      // TODO: improve
		TopicEndpoint:         "topic-endpoint", // TODO: improve
		Scope:                 "/subscriptions/{subscriptionId}/",
	}
}
