/*
Copyright 2025 The Dapr Authors
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

package anthropic

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	testCases := []struct {
		name          string
		metadata      map[string]string
		expectedModel string
		testFn        func(*testing.T, *Anthropic, error)
	}{
		{
			name: "with default settings",
			metadata: map[string]string{
				"key":   "test-key",
				"model": conversation.DefaultAnthropicModel,
			},
			expectedModel: conversation.DefaultAnthropicModel,
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with custom model name",
			metadata: map[string]string{
				"key":   "test-key",
				"model": "claude-opus-4-7",
			},
			expectedModel: "claude-opus-4-7",
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with custom endpoint",
			metadata: map[string]string{
				"key":      "test-key",
				"model":    conversation.DefaultAnthropicModel,
				"endpoint": "https://api.anthropic.com/v1",
			},
			expectedModel: conversation.DefaultAnthropicModel,
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with apiType foundry and endpoint",
			metadata: map[string]string{
				"key":      "test-key",
				"model":    "claude-opus-4-7",
				"apiType":  "foundry",
				"endpoint": "https://my-resource.services.ai.azure.com/anthropic/v1",
			},
			expectedModel: "claude-opus-4-7",
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with apiType foundry but missing endpoint",
			metadata: map[string]string{
				"key":     "test-key",
				"model":   "claude-opus-4-7",
				"apiType": "foundry",
			},
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.Error(t, err)
				assert.EqualError(t, err, "endpoint must be provided when apiType is set to 'foundry'")
			},
		},
		{
			name: "with apiType foundry case-insensitive",
			metadata: map[string]string{
				"key":      "test-key",
				"apiType":  "Foundry",
				"endpoint": "https://my-resource.services.ai.azure.com/anthropic/v1",
			},
			expectedModel: conversation.DefaultAnthropicModel,
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAnthropic(logger.NewLogger("anthropic test"))
			err := a.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{
					Properties: tc.metadata,
				},
			})
			inner := a.(*Anthropic)
			tc.testFn(t, inner, err)
			if tc.expectedModel != "" {
				assert.Equal(t, tc.expectedModel, inner.LLM.GetModel(), "LLM model name must be set on the response after Init")
			}
		})
	}
}

func TestEndpointInMetadata(t *testing.T) {
	a := &Anthropic{}

	md := a.GetComponentMetadata()
	if len(md) == 0 {
		t.Skip("Metadata is not enabled, skipping test")
	}

	_, exists := md["endpoint"]
	assert.True(t, exists, "endpoint field should exist in metadata")

	_, exists = md["apiType"]
	assert.True(t, exists, "apiType field should exist in metadata")
}
