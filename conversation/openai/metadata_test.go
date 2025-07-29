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

package openai

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenaiLangchainMetadata(t *testing.T) {
	t.Run("json marshaling with endpoint", func(t *testing.T) {
		metadata := OpenAILangchainMetadata{
			Key:        "test-key",
			Model:      "gpt-4",
			CacheTTL:   "10m",
			Endpoint:   "https://custom-endpoint.openai.azure.com/",
			APIType:    "azure",
			APIVersion: "2025-01-01-preview",
		}

		bytes, err := json.Marshal(metadata)
		require.NoError(t, err)

		var unmarshaled OpenAILangchainMetadata
		err = json.Unmarshal(bytes, &unmarshaled)
		require.NoError(t, err)

		assert.Equal(t, metadata.Key, unmarshaled.Key)
		assert.Equal(t, metadata.Model, unmarshaled.Model)
		assert.Equal(t, metadata.CacheTTL, unmarshaled.CacheTTL)
		assert.Equal(t, metadata.Endpoint, unmarshaled.Endpoint)
		assert.Equal(t, metadata.APIType, unmarshaled.APIType)
		assert.Equal(t, metadata.APIVersion, unmarshaled.APIVersion)
	})

	t.Run("json unmarshaling with endpoint", func(t *testing.T) {
		jsonStr := `{"key": "test-key", "model": "gpt-4", "endpoint": "https://custom-endpoint.openai.azure.com/", "apiType": "azure", "apiVersion": "2025-01-01-preview"}`

		var metadata OpenAILangchainMetadata
		err := json.Unmarshal([]byte(jsonStr), &metadata)
		require.NoError(t, err)

		assert.Equal(t, "test-key", metadata.Key)
		assert.Equal(t, "gpt-4", metadata.Model)
		assert.Equal(t, "https://custom-endpoint.openai.azure.com/", metadata.Endpoint)
		assert.Equal(t, "azure", metadata.APIType)
		assert.Equal(t, "2025-01-01-preview", metadata.APIVersion)
	})
}
