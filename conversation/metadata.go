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
package conversation

import "github.com/dapr/components-contrib/metadata"

// Metadata represents a set of conversation specific properties.
type Metadata struct {
	metadata.Base `json:",inline"`
}

// LangchainMetadata is a common metadata structure for langchain supported implementations.
type LangchainMetadata struct {
	Key      string `json:"key"`
	Model    string `json:"model"`
	CacheTTL string `json:"cacheTTL"`
	Endpoint string `json:"endpoint"`
}
