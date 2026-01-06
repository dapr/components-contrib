package langchaingokit

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

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractInt64FromGenInfo(t *testing.T) {
	tests := []struct {
		name     string
		genInfo  map[string]any
		key      string
		expected int64
	}{
		{
			name: "extract int64 value",
			genInfo: map[string]any{
				"CompletionTokens": int64(100),
			},
			key:      "CompletionTokens",
			expected: int64(100),
		},
		{
			name: "missing key",
			genInfo: map[string]any{
				"OtherKey": int64(50),
			},
			key:      "CompletionTokens",
			expected: int64(0),
		},
		{
			name:     "nil genInfo returns zero",
			genInfo:  nil,
			key:      "CompletionTokens",
			expected: int64(0),
		},
		{
			name: "wrong type returns zero",
			genInfo: map[string]any{
				"CompletionTokens": "not an int",
			},
			key:      "CompletionTokens",
			expected: int64(0),
		},
		{
			name: "zero value",
			genInfo: map[string]any{
				"CompletionTokens": int64(0),
			},
			key:      "CompletionTokens",
			expected: int64(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractInt64FromGenInfo(tt.genInfo, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractUsageFromLangchainGenerationInfo(t *testing.T) {
	tests := []struct {
		name     string
		genInfo  map[string]any
		validate func(t *testing.T, result *conversation.Usage)
	}{
		{
			name:    "nil genInfo",
			genInfo: nil,
			validate: func(t *testing.T, result *conversation.Usage) {
				assert.Nil(t, result)
			},
		},
		{
			name:    "empty genInfo",
			genInfo: map[string]any{},
			validate: func(t *testing.T, result *conversation.Usage) {
				assert.Nil(t, result)
			},
		},
		{
			name: "basic usage",
			genInfo: map[string]any{
				"CompletionTokens": int64(100),
				"PromptTokens":     int64(50),
				"TotalTokens":      int64(150),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(100), result.CompletionTokens)
				assert.Equal(t, int64(50), result.PromptTokens)
				assert.Equal(t, int64(150), result.TotalTokens)
				assert.Nil(t, result.CompletionTokensDetails)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "usage with completion token details",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(200),
				"PromptTokens":                       int64(100),
				"TotalTokens":                        int64(300),
				"CompletionAcceptedPredictionTokens": int64(10),
				"CompletionAudioTokens":              int64(5),
				"CompletionReasoningTokens":          int64(15),
				"CompletionRejectedPredictionTokens": int64(2),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(200), result.CompletionTokens)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, int64(10), result.CompletionTokensDetails.AcceptedPredictionTokens)
				assert.Equal(t, int64(5), result.CompletionTokensDetails.AudioTokens)
				assert.Equal(t, int64(15), result.CompletionTokensDetails.ReasoningTokens)
				assert.Equal(t, int64(2), result.CompletionTokensDetails.RejectedPredictionTokens)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "usage with prompt token details",
			genInfo: map[string]any{
				"CompletionTokens":   int64(150),
				"PromptTokens":       int64(75),
				"TotalTokens":        int64(225),
				"PromptAudioTokens":  int64(10),
				"PromptCachedTokens": int64(20),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(150), result.CompletionTokens)
				assert.Nil(t, result.CompletionTokensDetails)
				assert.NotNil(t, result.PromptTokensDetails)
				assert.Equal(t, int64(10), result.PromptTokensDetails.AudioTokens)
				assert.Equal(t, int64(20), result.PromptTokensDetails.CachedTokens)
			},
		},
		{
			name: "usage with all details",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(250),
				"PromptTokens":                       int64(125),
				"TotalTokens":                        int64(375),
				"CompletionAcceptedPredictionTokens": int64(20),
				"CompletionAudioTokens":              int64(8),
				"CompletionReasoningTokens":          int64(25),
				"CompletionRejectedPredictionTokens": int64(3),
				"PromptAudioTokens":                  int64(15),
				"PromptCachedTokens":                 int64(30),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Equal(t, int64(250), result.CompletionTokens)
				assert.Equal(t, int64(125), result.PromptTokens)
				assert.Equal(t, int64(375), result.TotalTokens)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, int64(20), result.CompletionTokensDetails.AcceptedPredictionTokens)
				assert.Equal(t, int64(8), result.CompletionTokensDetails.AudioTokens)
				assert.Equal(t, int64(25), result.CompletionTokensDetails.ReasoningTokens)
				assert.Equal(t, int64(3), result.CompletionTokensDetails.RejectedPredictionTokens)
				assert.NotNil(t, result.PromptTokensDetails)
				assert.Equal(t, int64(15), result.PromptTokensDetails.AudioTokens)
				assert.Equal(t, int64(30), result.PromptTokensDetails.CachedTokens)
			},
		},
		{
			name: "completion details with zero values are not included",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(100),
				"PromptTokens":                       int64(50),
				"TotalTokens":                        int64(150),
				"CompletionAcceptedPredictionTokens": int64(0),
				"CompletionAudioTokens":              int64(0),
				"CompletionReasoningTokens":          int64(0),
				"CompletionRejectedPredictionTokens": int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Nil(t, result.CompletionTokensDetails)
			},
		},
		{
			name: "prompt details with zero values are not included",
			genInfo: map[string]any{
				"CompletionTokens":   int64(100),
				"PromptTokens":       int64(50),
				"TotalTokens":        int64(150),
				"PromptAudioTokens":  int64(0),
				"PromptCachedTokens": int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.Nil(t, result.PromptTokensDetails)
			},
		},
		{
			name: "completion details included if any field is non-zero",
			genInfo: map[string]any{
				"CompletionTokens":                   int64(100),
				"PromptTokens":                       int64(50),
				"TotalTokens":                        int64(150),
				"CompletionAcceptedPredictionTokens": int64(0),
				"CompletionAudioTokens":              int64(5),
				"CompletionReasoningTokens":          int64(0),
				"CompletionRejectedPredictionTokens": int64(0),
			},
			validate: func(t *testing.T, result *conversation.Usage) {
				require.NotNil(t, result)
				assert.NotNil(t, result.CompletionTokensDetails)
				assert.Equal(t, int64(5), result.CompletionTokensDetails.AudioTokens)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractUsageFromLangchainGenerationInfo(tt.genInfo)
			tt.validate(t, result)
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
