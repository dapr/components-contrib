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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	testLogger := logger.NewLogger("test")

	tests := []struct {
		name   string
		props  map[string]string
		testFn func(t *testing.T, o *OpenRouter, err error)
	}{
		{
			name: "default model and endpoint",
			props: map[string]string{
				"key": "sk-or-v1-test",
			},
			testFn: func(t *testing.T, o *OpenRouter, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM.Model)
				assert.Equal(t, conversation.DefaultOpenRouterModel, o.LLM.GetModel())
			},
		},
		{
			name: "custom model",
			props: map[string]string{
				"key":   "sk-or-v1-test",
				"model": "anthropic/claude-3-5-sonnet",
			},
			testFn: func(t *testing.T, o *OpenRouter, err error) {
				require.NoError(t, err)
				assert.Equal(t, "anthropic/claude-3-5-sonnet", o.LLM.GetModel())
			},
		},
		{
			name: "custom endpoint",
			props: map[string]string{
				"key":      "sk-or-v1-test",
				"endpoint": "https://proxy.example.com/v1",
			},
			testFn: func(t *testing.T, o *OpenRouter, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM.Model)
			},
		},
		{
			name: "with attribution headers",
			props: map[string]string{
				"key":       "sk-or-v1-test",
				"siteURL":   "https://myapp.example.com",
				"siteTitle": "My Dapr App",
			},
			testFn: func(t *testing.T, o *OpenRouter, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM.Model)
			},
		},
		{
			name: "with response cache TTL",
			props: map[string]string{
				"key":      "sk-or-v1-test",
				"cacheTTL": "5m",
			},
			testFn: func(t *testing.T, o *OpenRouter, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM.Model)
			},
		},
		{
			name:  "missing API key returns an error from the SDK",
			props: map[string]string{},
			testFn: func(t *testing.T, o *OpenRouter, err error) {
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NewOpenRouter(testLogger).(*OpenRouter)
			err := o.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{Properties: tt.props},
			})
			tt.testFn(t, o, err)
		})
	}
}

func TestGetComponentMetadata(t *testing.T) {
	o := NewOpenRouter(logger.NewLogger("test")).(*OpenRouter)
	md := o.GetComponentMetadata()
	assert.NotEmpty(t, md)
}
