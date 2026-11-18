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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

func TestOpenrouterMetadataDecode(t *testing.T) {
	t.Run("all fields decode", func(t *testing.T) {
		md := OpenrouterMetadata{}
		err := kmeta.DecodeMetadata(map[string]string{
			"key":           "test-key",
			"model":         "anthropic/claude-sonnet-4.6",
			"endpoint":      "https://gateway.example.com/v1",
			"cacheTTL":      "10m",
			"maxTokens":     "2048",
			"appURL":        "https://myapp.example.com",
			"appTitle":      "My App",
			"appCategories": "personal-agent,general-chat",
		}, &md)
		require.NoError(t, err)

		assert.Equal(t, "test-key", md.Key)
		assert.Equal(t, "anthropic/claude-sonnet-4.6", md.Model)
		assert.Equal(t, "https://gateway.example.com/v1", md.Endpoint)
		require.NotNil(t, md.ResponseCacheTTL)
		assert.Equal(t, 10*time.Minute, *md.ResponseCacheTTL)
		require.NotNil(t, md.MaxTokens)
		assert.Equal(t, int64(2048), *md.MaxTokens)
		assert.Equal(t, "https://myapp.example.com", md.AppURL)
		assert.Equal(t, "My App", md.AppTitle)
		assert.Equal(t, "personal-agent,general-chat", md.AppCategories)
	})

	t.Run("absent maxTokens stays nil", func(t *testing.T) {
		md := OpenrouterMetadata{}
		err := kmeta.DecodeMetadata(map[string]string{"key": "test-key"}, &md)
		require.NoError(t, err)

		assert.Nil(t, md.MaxTokens)
	})
}

func TestInit(t *testing.T) {
	t.Run("key is required", func(t *testing.T) {
		err := NewOpenrouter(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{})
		require.ErrorContains(t, err, "api key is required")
	})

	t.Run("default model", func(t *testing.T) {
		t.Setenv("OPENROUTER_MODEL", "")
		o := NewOpenrouter(logger.NewLogger("test")).(*Openrouter)
		require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{"key": "test-key"}}}))
		assert.Equal(t, conversation.DefaultOpenrouterModel, o.GetModel())
	})

	t.Run("model from metadata", func(t *testing.T) {
		t.Setenv("OPENROUTER_MODEL", "")
		o := NewOpenrouter(logger.NewLogger("test")).(*Openrouter)
		require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
			"key":   "test-key",
			"model": "openai/gpt-5-nano",
		}}}))
		assert.Equal(t, "openai/gpt-5-nano", o.GetModel())
	})

	t.Run("model from environment variable", func(t *testing.T) {
		t.Setenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct")
		o := NewOpenrouter(logger.NewLogger("test")).(*Openrouter)
		require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{"key": "test-key"}}}))
		assert.Equal(t, "meta-llama/llama-3.3-70b-instruct", o.GetModel())
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := new(Openrouter).GetComponentMetadata()
	for _, field := range []string{
		"key", "model", "endpoint", "maxTokens", "headers", "appURL", "appTitle", "appCategories",
		"fallbackModels", "providerOrder", "providerOnly", "providerIgnore", "providerSort", "allowProviderFallbacks",
		"requireParameters", "dataCollection", "zdr", "quantizations", "maxPromptPrice", "maxCompletionPrice",
	} {
		assert.Contains(t, md, field)
	}
}
