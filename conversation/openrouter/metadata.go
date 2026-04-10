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

package openrouter

import "github.com/dapr/components-contrib/conversation"

// OpenRouterMetadata holds configuration for the OpenRouter conversation component.
type OpenRouterMetadata struct {
	conversation.LangchainMetadata `json:",inline" mapstructure:",squash"`

	// SiteURL is forwarded as HTTP-Referer for OpenRouter attribution and ranking.
	// Optional but recommended so OpenRouter can identify your application.
	SiteURL *string `json:"siteURL" mapstructure:"siteURL"`

	// SiteTitle is forwarded as X-Title for OpenRouter attribution.
	// Optional but recommended so OpenRouter can identify your application.
	SiteTitle string `json:"siteTitle" mapstructure:"siteTitle"`
}
