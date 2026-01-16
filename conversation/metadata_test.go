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

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLangchainMetadata(t *testing.T) {
	t.Run("json marshaling with endpoint", func(t *testing.T) {
		ttl := 10 * time.Minute
		metadata := LangchainMetadata{
			Key:              "test-key",
			Model:            DefaultOpenAIModel,
			ResponseCacheTTL: &ttl,
			Endpoint:         "https://custom-endpoint.example.com",
		}

		bytes, err := json.Marshal(metadata)
		require.NoError(t, err)

		var unmarshaled LangchainMetadata
		err = json.Unmarshal(bytes, &unmarshaled)
		require.NoError(t, err)

		assert.Equal(t, metadata.Key, unmarshaled.Key)
		assert.Equal(t, metadata.Model, unmarshaled.Model)
		assert.Equal(t, metadata.ResponseCacheTTL, unmarshaled.ResponseCacheTTL)
		assert.Equal(t, metadata.Endpoint, unmarshaled.Endpoint)
	})

	t.Run("json unmarshaling with endpoint", func(t *testing.T) {
		jsonStr := `{"key": "test-key", "endpoint": "https://api.openai.com/v1"}`

		var metadata LangchainMetadata
		err := json.Unmarshal([]byte(jsonStr), &metadata)
		require.NoError(t, err)

		assert.Equal(t, "test-key", metadata.Key)
		assert.Equal(t, "https://api.openai.com/v1", metadata.Endpoint)
	})
}
