/*
Copyright 2024 The Dapr Authors
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

package storagequeues

import "time"

type storagequeuesMetadata struct {
	// The storage account name.
	AccountName string `json:"accountName,string" binding:"input" binding:"output"`
	// Authenticate using a pre-shared "account key".
	AccountKey string `json:"accountKey,string" binding:"input" binding:"output" authenticationProfile:"accountKey"`
	// The name of the Azure Storage queue.
	QueueName string `json:"queueName,string" binding:"input" binding:"output"`
	// Optional custom endpoint URL. This is useful when using the Azurite emulator or when using custom domains for Azure Storage (although this is not officially supported). The endpoint must be the full base URL, including the protocol (`http://` or `https://`), the IP or FQDN, and optional port.
	QueueEndpoint string `json:"queueEndpoint,string,omitempty" binding:"input" binding:"output"`
	// Configuration to decode base64 file content before saving to Storage Queues (e.g. in case of saving a file with binary content).
	DecodeBase64 bool `json:"decodeBase64,string,omitempty" binding:"input"`
	// When enabled, the data payload is base64-encoded before being sent to Azure Storage Queues.
	EncodeBase64 bool `json:"encodeBase64,string,omitempty" binding:"output"`
	// Set the interval to poll Azure Storage Queues for new messages.
	PollingInterval time.Duration `json:"pollingInterval,string,omitempty"  binding:"output" mapstructure:"pollingInterval"`
	// Set the default message Time To Live (TTL). It's also possible to specify a per-message TTL by setting the `ttl` property in the invocation request's metadata.
	TTL *time.Duration `json:"ttl,string,omitempty" binding:"output" mapstructure:"ttl" mapstructurealiases:"ttlInSeconds"`
	// Allows setting a custom queue visibility timeout to avoid immediate retrying of recently-failed messages.
	VisibilityTimeout *time.Duration `json:"visibilityTimeout,string,omitempty" binding:"input"`

	// Remind to self that in the docs this is just endpoint, but metadata in repo shows queueEndpoint...
	// TODO: docs need updating to just be ttl!
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func Defaults() storagequeuesMetadata {
	ttl := 10 * time.Minute
	vis := 30 * time.Second
	return storagequeuesMetadata{
		PollingInterval:   10 * time.Second,
		TTL:               &ttl,
		DecodeBase64:      false,
		EncodeBase64:      false,
		VisibilityTimeout: &vis,
	}
}

// Note: we do not include any mdignored field.
func Examples() storagequeuesMetadata {
	exampleDuration := 30 * time.Second
	return storagequeuesMetadata{
		AccountName: "mystorageaccount",
		AccountKey:  "my-secret-key",
		QueueName:   "myqueue",
		QueueEndpoint: `"http://127.0.0.1:10001" or
      "https://accountName.queue.example.com"`,
		DecodeBase64:      true,
		EncodeBase64:      true,
		PollingInterval:   exampleDuration,
		TTL:               &exampleDuration,
		VisibilityTimeout: &exampleDuration,
	}
}
