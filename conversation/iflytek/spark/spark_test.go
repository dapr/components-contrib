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

package spark

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test"))
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{},
		}})
		require.EqualError(t, err, "spark api key is required")
	})

	t.Run("defaults", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test")).(*Spark)
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{"key": "test-key"},
		}})
		require.NoError(t, err)
		assert.Equal(t, defaultEndpoint, s.md.Endpoint)
		assert.Equal(t, "test-key", s.md.Key)
	})

	t.Run("with cache", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test"))
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{"key": "test-key", "cacheTTL": "10m"},
		}})
		require.NoError(t, err)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := NewSpark(logger.NewLogger("test")).(*Spark).GetComponentMetadata()
	for _, name := range []string{"key", "model", "endpoint", "responseCacheTTL"} {
		assert.Contains(t, md, name)
	}
	assert.NotContains(t, md, "Key")
	assert.NotContains(t, md, "MaxTokens")
	assert.NotContains(t, md, "maxTokens")
}
