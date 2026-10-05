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

package deepseek

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kmeta "github.com/dapr/kit/metadata"
)

func TestDeepseekMetadataDecode(t *testing.T) {
	t.Run("maxTokens decodes into the embedded LangchainMetadata", func(t *testing.T) {
		md := DeepseekMetadata{}
		err := kmeta.DecodeMetadata(map[string]string{
			"key":       "test-key",
			"maxTokens": "2048",
		}, &md)
		require.NoError(t, err)

		assert.Equal(t, "test-key", md.Key)
		require.NotNil(t, md.MaxTokens)
		assert.Equal(t, int64(2048), *md.MaxTokens)
	})

	t.Run("absent maxTokens stays nil", func(t *testing.T) {
		md := DeepseekMetadata{}
		err := kmeta.DecodeMetadata(map[string]string{"key": "test-key"}, &md)
		require.NoError(t, err)

		assert.Nil(t, md.MaxTokens)
	})
}
