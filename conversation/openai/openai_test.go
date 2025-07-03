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
	"context"
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

func TestStreamFuncWithoutToolCalls(t *testing.T) {
	testCases := []struct {
		name           string
		chunk          []byte
		expectCallback bool
		description    string
	}{
		{
			name:           "empty chunk",
			chunk:          []byte{},
			expectCallback: false,
			description:    "Empty chunks should be filtered out",
		},
		{
			name:           "nil chunk",
			chunk:          nil,
			expectCallback: false,
			description:    "Nil chunks should be filtered out",
		},
		{
			name:           "regular text chunk",
			chunk:          []byte("Hello world"),
			expectCallback: true,
			description:    "Regular text chunks should pass through",
		},
		{
			name:           "json text chunk",
			chunk:          []byte(`{"message": "Hello world"}`),
			expectCallback: true,
			description:    "JSON that's not tool calls should pass through",
		},
		{
			name:           "single tool call chunk",
			chunk:          []byte(`[{"id":"call_123","type":"function","function":{"name":"get_weather","arguments":"{\"location\":\"Tokyo\"}"}}]`),
			expectCallback: false,
			description:    "Single tool call chunks should be filtered out",
		},
		{
			name:           "multiple tool calls chunk",
			chunk:          []byte(`[{"id":"call_123","type":"function","function":{"name":"get_weather","arguments":"{\"location\":\"Tokyo\"}"}},{"id":"call_456","type":"function","function":{"name":"get_time","arguments":"{\"timezone\":\"JST\"}"}}]`),
			expectCallback: false,
			description:    "Multiple tool calls chunks should be filtered out",
		},
		{
			name:           "empty tool calls array",
			chunk:          []byte(`[]`),
			expectCallback: false,
			description:    "Empty tool calls array should be filtered out",
		},
		{
			name:           "tool call with minimal fields",
			chunk:          []byte(`[{"type":"function"}]`),
			expectCallback: false,
			description:    "Tool calls with minimal fields should be filtered out",
		},
		{
			name:           "invalid json",
			chunk:          []byte(`{"invalid": json}`),
			expectCallback: true,
			description:    "Invalid JSON should pass through (not tool calls)",
		},
		{
			name:           "streaming response chunk",
			chunk:          []byte(`data: {"choices":[{"delta":{"content":"Hello"}}]}`),
			expectCallback: true,
			description:    "Streaming response chunks should pass through",
		},
		{
			name:           "partial tool call json",
			chunk:          []byte(`[{"id":"call_123","type":`),
			expectCallback: true,
			description:    "Partial/malformed tool call JSON should pass through",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			callbackCalled := false
			var receivedChunk []byte
			var receivedCtx context.Context

			// Create mock stream function
			mockStreamFunc := func(ctx context.Context, chunk []byte) error {
				callbackCalled = true
				receivedChunk = chunk
				receivedCtx = ctx
				return nil
			}

			// Create the wrapper function
			wrappedFunc := streamFuncWithoutToolCalls(mockStreamFunc)

			// Test context
			ctx := t.Context()

			// Call the wrapper function
			err := wrappedFunc(ctx, tc.chunk)

			// Verify no error occurred
			require.NoError(t, err, "Wrapper function should not return error")

			// Verify callback behavior
			if tc.expectCallback {
				assert.True(t, callbackCalled, tc.description)
				assert.Equal(t, tc.chunk, receivedChunk, "Received chunk should match input")
				assert.Equal(t, ctx, receivedCtx, "Received context should match input")
			} else {
				assert.False(t, callbackCalled, tc.description)
			}
		})
	}
}

func TestStreamFuncWithoutToolCallsErrorPropagation(t *testing.T) {
	// Test that errors from the wrapped function are properly propagated
	expectedErr := assert.AnError

	mockStreamFunc := func(ctx context.Context, chunk []byte) error {
		return expectedErr
	}

	wrappedFunc := streamFuncWithoutToolCalls(mockStreamFunc)

	// Test with a regular text chunk that should call the wrapped function
	err := wrappedFunc(t.Context(), []byte("Hello world"))

	// Verify the error is propagated
	assert.Equal(t, expectedErr, err, "Error from wrapped function should be propagated")
}
