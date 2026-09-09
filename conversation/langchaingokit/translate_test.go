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

package langchaingokit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
)

func TestExtractInt64FromGenInfo(t *testing.T) {
	tests := []struct {
		name        string
		genInfo     map[string]any
		key         string
		expected    uint64
		expectedErr bool
	}{
		{
			name: "extract int64 value",
			genInfo: map[string]any{
				completionKey: int64(100),
			},
			key:         completionKey,
			expected:    uint64(100),
			expectedErr: false,
		},
		{
			name: "missing key",
			genInfo: map[string]any{
				"OtherKey": int64(50),
			},
			key:         completionKey,
			expected:    uint64(0),
			expectedErr: false,
		},
		{
			name:        "nil genInfo returns zero",
			genInfo:     nil,
			key:         completionKey,
			expected:    uint64(0),
			expectedErr: false,
		},
		{
			name: "wrong type returns zero",
			genInfo: map[string]any{
				completionKey: "not an int",
			},
			key:         completionKey,
			expected:    uint64(0),
			expectedErr: true,
		},
		{
			name: "zero value",
			genInfo: map[string]any{
				completionKey: int64(0),
			},
			key:         completionKey,
			expected:    uint64(0),
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractUint64FromGenInfo(tt.genInfo, tt.key)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractUsageFromLangchainGenerationInfo(t *testing.T) {
	tests := []struct {
		name     string
		genInfo  map[string]any
		validate func(t *testing.T, result *conversation.Usage, err error)
	}{
		{
			name:    "nil genInfo",
			genInfo: nil,
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				assert.Nil(t, result)
			},
		},
		{
			name:    "empty genInfo",
			genInfo: map[string]any{},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				assert.Nil(t, result)
			},
		},
		{
			name: "basic usage",
			genInfo: map[string]any{
				completionKey: int64(100),
				promptKey:     int64(50),
				totalKey:      int64(150),
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, uint64(100), result.CompletionTokens)
				assert.Equal(t, uint64(50), result.PromptTokens)
				assert.Equal(t, uint64(150), result.TotalTokens)
				assert.Nil(t, result.CompletionTokensDetails)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "usage with completion token details",
			genInfo: map[string]any{
				completionKey:         int64(200),
				promptKey:             int64(100),
				totalKey:              int64(300),
				completionAcceptedKey: int64(10),
				completionAudioKey:    int64(5),
				reasoningKey:          int64(15),
				rejectedKey:           int64(2),
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, uint64(200), result.CompletionTokens)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, uint64(10), result.CompletionTokensDetails.AcceptedPredictionTokens)
				assert.Equal(t, uint64(5), result.CompletionTokensDetails.AudioTokens)
				assert.Equal(t, uint64(15), result.CompletionTokensDetails.ReasoningTokens)
				assert.Equal(t, uint64(2), result.CompletionTokensDetails.RejectedPredictionTokens)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "usage with prompt token details",
			genInfo: map[string]any{
				completionKey:   int64(150),
				promptKey:       int64(75),
				totalKey:        int64(225),
				promptAudioKey:  int64(10),
				promptCachedKey: int64(20),
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, uint64(150), result.CompletionTokens)
				assert.Nil(t, result.CompletionTokensDetails)
				assert.NotNil(t, result.PromptTokensDetails)
				assert.Equal(t, uint64(10), result.PromptTokensDetails.AudioTokens)
				assert.Equal(t, uint64(20), result.PromptTokensDetails.CachedTokens)
			},
		},
		{
			name: "usage with all details",
			genInfo: map[string]any{
				completionKey:         int64(250),
				promptKey:             int64(125),
				totalKey:              int64(375),
				completionAcceptedKey: int64(20),
				completionAudioKey:    int64(8),
				reasoningKey:          int64(25),
				rejectedKey:           int64(3),
				promptAudioKey:        int64(15),
				promptCachedKey:       int64(30),
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, uint64(250), result.CompletionTokens)
				assert.Equal(t, uint64(125), result.PromptTokens)
				assert.Equal(t, uint64(375), result.TotalTokens)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, uint64(20), result.CompletionTokensDetails.AcceptedPredictionTokens)
				assert.Equal(t, uint64(8), result.CompletionTokensDetails.AudioTokens)
				assert.Equal(t, uint64(25), result.CompletionTokensDetails.ReasoningTokens)
				assert.Equal(t, uint64(3), result.CompletionTokensDetails.RejectedPredictionTokens)
				assert.NotNil(t, result.PromptTokensDetails)
				assert.Equal(t, uint64(15), result.PromptTokensDetails.AudioTokens)
				assert.Equal(t, uint64(30), result.PromptTokensDetails.CachedTokens)
			},
		},
		{
			name: "completion details with zero values are not included",
			genInfo: map[string]any{
				completionKey:         int64(100),
				promptKey:             int64(50),
				totalKey:              int64(150),
				completionAcceptedKey: int64(0),
				completionAudioKey:    int64(0),
				reasoningKey:          int64(0),
				rejectedKey:           int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Nil(t, result.CompletionTokensDetails)
			},
		},
		{
			name: "prompt details with zero values are not included",
			genInfo: map[string]any{
				completionKey:   int64(100),
				promptKey:       int64(50),
				totalKey:        int64(150),
				promptAudioKey:  int64(0),
				promptCachedKey: int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "completion details included if any field is non-zero",
			genInfo: map[string]any{
				completionKey:         int64(100),
				promptKey:             int64(50),
				totalKey:              int64(150),
				completionAcceptedKey: int64(0),
				completionAudioKey:    int64(5),
				reasoningKey:          int64(0),
				rejectedKey:           int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, uint64(5), result.CompletionTokensDetails.AudioTokens)
			},
		},
		{
			name: "invalid type",
			genInfo: map[string]any{
				completionKey:         int64(100),
				promptKey:             int64(50),
				totalKey:              int64(150),
				completionAcceptedKey: int64(0),
				completionAudioKey:    int64(5),
				reasoningKey:          int64(0),
				rejectedKey:           "i am the invalid type here",
			},
			validate: func(t *testing.T, result *conversation.Usage, err error) {
				require.Error(t, err)
				require.Nil(t, result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractUsageFromLangchainGenerationInfo(tt.genInfo)
			tt.validate(t, result, err)
		})
	}
}

// resolveCallOptions applies the built options so assertions can inspect the resolved values.
func resolveCallOptions(opts []llms.CallOption) llms.CallOptions {
	var resolved llms.CallOptions
	for _, opt := range opts {
		opt(&resolved)
	}
	return resolved
}

func TestTranslateToolChoice(t *testing.T) {
	tests := map[string]struct {
		toolChoice string
		provider   Provider
		hasTools   bool
		expected   any
	}{
		"non-anthropic provider passes the bare string through": {
			toolChoice: "required",
			hasTools:   true,
			expected:   "required",
		},
		"non-anthropic provider passes a named tool through": {
			toolChoice: "get_weather",
			hasTools:   true,
			expected:   "get_weather",
		},
		"non-anthropic provider passes through without tools": {
			toolChoice: "required",
			expected:   "required",
		},
		"anthropic auto is omitted": {
			toolChoice: "auto",
			provider:   ProviderAnthropic,
			hasTools:   true,
			expected:   nil,
		},
		"anthropic required maps to any": {
			toolChoice: "required",
			provider:   ProviderAnthropic,
			hasTools:   true,
			expected:   map[string]any{"type": "any"},
		},
		"anthropic any maps to any": {
			toolChoice: "any",
			provider:   ProviderAnthropic,
			hasTools:   true,
			expected:   map[string]any{"type": "any"},
		},
		"anthropic none maps to none": {
			toolChoice: "none",
			provider:   ProviderAnthropic,
			hasTools:   true,
			expected:   map[string]any{"type": "none"},
		},
		"anthropic named tool maps to tool": {
			toolChoice: "get_weather",
			provider:   ProviderAnthropic,
			hasTools:   true,
			expected:   map[string]any{"type": "tool", "name": "get_weather"},
		},
		"anthropic omits required without tools": {
			toolChoice: "required",
			provider:   ProviderAnthropic,
			expected:   nil,
		},
		"anthropic omits a named tool without tools": {
			toolChoice: "get_weather",
			provider:   ProviderAnthropic,
			expected:   nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, translateToolChoice(tt.toolChoice, tt.provider, tt.hasTools))
		})
	}
}

// TestTranslateToolChoiceIsDeterministic guards the caching contract: Anthropic invalidates
// cached message blocks when tool_choice changes, so repeated calls must not vary.
func TestTranslateToolChoiceIsDeterministic(t *testing.T) {
	for _, toolChoice := range []string{"auto", "required", "any", "none", "get_weather"} {
		t.Run(toolChoice, func(t *testing.T) {
			first := translateToolChoice(toolChoice, ProviderAnthropic, true)
			for range 3 {
				assert.Equal(t, first, translateToolChoice(toolChoice, ProviderAnthropic, true))
			}
		})
	}
}

func TestGetOptionsFromRequest(t *testing.T) {
	log := logger.NewLogger("test")

	toolChoice := "auto"
	requiredToolChoice := "required"
	tools := []llms.Tool{
		{
			Type: "function",
			Function: &llms.FunctionDefinition{
				Name:        "test_func",
				Description: "a test function",
			},
		},
	}
	ttl := 5 * time.Minute

	tests := map[string]struct {
		request      *conversation.Request
		provider     Provider
		existingOpts []llms.CallOption
		validate     func(t *testing.T, r *conversation.Request, opts []llms.CallOption)
	}{
		"invalid ResponseFormatAsJSONSchema logs warning and continues": {
			request: &conversation.Request{
				ResponseFormatAsJSONSchema: map[string]any{
					"description": "missing type field",
				},
			},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.NotNil(t, opts)
			},
		},
		"valid ResponseFormatAsJSONSchema adds structured output option": {
			request: &conversation.Request{
				ResponseFormatAsJSONSchema: map[string]any{
					"type": "object",
					"name": "test_schema",
					"properties": map[string]any{
						"name": map[string]any{
							"type": "string",
						},
					},
				},
			},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.NotEmpty(t, opts)
			},
		},
		"temperature sets option": {
			request: &conversation.Request{
				Temperature: 0.7,
			},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.NotEmpty(t, opts)
			},
		},
		"zero temperature sets no option": {
			request: &conversation.Request{},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.Empty(t, opts)
			},
		},
		"tools and tool choice set options": {
			request: &conversation.Request{
				Tools:      &tools,
				ToolChoice: &toolChoice,
			},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.Len(t, opts, 2)
				assert.Equal(t, "auto", resolveCallOptions(opts).ToolChoice)
			},
		},
		"anthropic auto tool choice omits the option": {
			request: &conversation.Request{
				Tools:      &tools,
				ToolChoice: &toolChoice,
			},
			provider: ProviderAnthropic,
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.Len(t, opts, 1)
				assert.Nil(t, resolveCallOptions(opts).ToolChoice)
			},
		},
		"anthropic required tool choice sets the object form": {
			request: &conversation.Request{
				Tools:      &tools,
				ToolChoice: &requiredToolChoice,
			},
			provider: ProviderAnthropic,
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.Len(t, opts, 2)
				assert.Equal(t, map[string]any{"type": "any"}, resolveCallOptions(opts).ToolChoice)
			},
		},
		"anthropic tool choice without tools omits the option": {
			request: &conversation.Request{
				ToolChoice: &requiredToolChoice,
			},
			provider: ProviderAnthropic,
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.Empty(t, opts)
			},
		},
		"metadata sets option": {
			request: &conversation.Request{
				Metadata: map[string]string{
					"key": "value",
				},
			},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.NotEmpty(t, opts)
			},
		},
		"prompt cache retention adds to metadata": {
			request: &conversation.Request{
				PromptCacheRetention: &ttl,
			},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.NotEmpty(t, opts)
				assert.NotNil(t, r.Metadata)
				assert.Equal(t, "5m0s", r.Metadata["prompt_cache_retention"])
			},
		},
		"existing options are preserved": {
			request: &conversation.Request{
				Temperature: 0.5,
			},
			existingOpts: []llms.CallOption{llms.WithMaxTokens(100)},
			validate: func(t *testing.T, r *conversation.Request, opts []llms.CallOption) {
				assert.Len(t, opts, 2)
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				opts := getOptionsFromRequest(tt.request, tt.provider, log, tt.existingOpts...)
				tt.validate(t, tt.request, opts)
			})
		})
	}
}

func TestStringMapToAny(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		validate func(t *testing.T, result map[string]any)
	}{
		{
			name:  "nil map returns nil",
			input: nil,
			validate: func(t *testing.T, result map[string]any) {
				assert.Nil(t, result)
			},
		},
		{
			name:  "empty map returns empty map",
			input: map[string]string{},
			validate: func(t *testing.T, result map[string]any) {
				assert.NotNil(t, result)
				assert.Empty(t, result)
			},
		},
		{
			name: "converts map[string]string to map[string]any",
			input: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
			validate: func(t *testing.T, result map[string]any) {
				require.NotNil(t, result)
				assert.Len(t, result, 3)
				assert.Equal(t, "value1", result["key1"])
				assert.Equal(t, "value2", result["key2"])
				assert.Equal(t, "value3", result["key3"])
				// Verify types
				for _, v := range result {
					_, ok := v.(string)
					assert.True(t, ok, "All values should be strings")
				}
			},
		},
		{
			name: "preserves all key-value pairs",
			input: map[string]string{
				"metadata1": "data1",
				"metadata2": "data2",
			},
			validate: func(t *testing.T, result map[string]any) {
				assert.Equal(t, "data1", result["metadata1"])
				assert.Equal(t, "data2", result["metadata2"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stringMapToAny(tt.input)
			tt.validate(t, result)
		})
	}
}
