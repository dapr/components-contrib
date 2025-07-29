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
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	testCases := []struct {
		name     string
		metadata map[string]string
		testFn   func(*testing.T, *OpenAI, error)
	}{
		{
			name: "with default endpoint",
			metadata: map[string]string{
				"key":   "test-key",
				"model": "gpt-4",
			},
			testFn: func(t *testing.T, o *OpenAI, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM)
			},
		},
		{
			name: "with custom endpoint",
			metadata: map[string]string{
				"key":      "test-key",
				"model":    "gpt-4",
				"endpoint": "https://api.openai.com/v1",
			},
			testFn: func(t *testing.T, o *OpenAI, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM)
				// Since we can't directly access the client's baseURL,
				// we're mainly testing that initialization succeeds
			},
		},
		{
			name: "with apiType openai",
			metadata: map[string]string{
				"key":     "test-key",
				"model":   "gpt-4",
				"apiType": "openai",
			},
			testFn: func(t *testing.T, o *OpenAI, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM)
			},
		},
		{
			name: "with apiType azure",
			metadata: map[string]string{
				"key":        "test-key",
				"model":      "gpt-4",
				"apiType":    "azure",
				"endpoint":   "https://custom-endpoint.openai.azure.com/",
				"apiVersion": "2025-01-01-preview",
			},
			testFn: func(t *testing.T, o *OpenAI, err error) {
				require.NoError(t, err)
				assert.NotNil(t, o.LLM)
			},
		},
		{
			name: "with apiType azure but missing apiVersion",
			metadata: map[string]string{
				"key":      "test-key",
				"model":    "gpt-4",
				"apiType":  "azure",
				"endpoint": "https://custom-endpoint.openai.azure.com/",
			},
			testFn: func(t *testing.T, o *OpenAI, err error) {
				require.Error(t, err)
				assert.EqualError(t, err, "apiVersion must be provided when apiType is set to 'azure'")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := NewOpenAI(logger.NewLogger("openai test"))
			err := o.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{
					Properties: tc.metadata,
				},
			})
			tc.testFn(t, o.(*OpenAI), err)
		})
	}
}

func TestEndpointInMetadata(t *testing.T) {
	// Create an instance of OpenAI component
	o := &OpenAI{}

	// This test relies on the metadata tag
	md := o.GetComponentMetadata()
	if len(md) == 0 {
		t.Skip("Metadata is not enabled, skipping test")
	}

	// Print all available metadata keys for debugging
	t.Logf("Available metadata keys: %v", func() []string {
		keys := make([]string, 0, len(md))
		for k := range md {
			keys = append(keys, k)
		}
		return keys
	}())

	// Verify endpoint field exists (note: field names are capitalized in metadata)
	_, exists := md["Endpoint"]
	assert.True(t, exists, "Endpoint field should exist in metadata")
}
