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

package conversation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tmc/langchaingo/llms"
)

func TestExtractUsageFromResponse(t *testing.T) {
	t.Run("nil response", func(t *testing.T) {
		usage := ExtractUsageFromResponse(nil)
		assert.Nil(t, usage)
	})

	t.Run("no choices", func(t *testing.T) {
		resp := &llms.ContentResponse{}
		usage := ExtractUsageFromResponse(resp)
		assert.Nil(t, usage)
	})

	t.Run("no generation info", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{{}},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.Nil(t, usage)
	})

	t.Run("langchain go standard format", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"PromptTokens":     10,
						"CompletionTokens": 20,
					},
				},
			},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.NotNil(t, usage)
		assert.Equal(t, uint64(10), usage.PromptTokens)
		assert.Equal(t, uint64(20), usage.CompletionTokens)
		assert.Equal(t, uint64(30), usage.TotalTokens)
	})

	t.Run("openai nested usage format", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": map[string]any{
							"prompt_tokens":     15,
							"completion_tokens": 25,
							"total_tokens":      40,
						},
					},
				},
			},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.NotNil(t, usage)
		assert.Equal(t, uint64(15), usage.PromptTokens)
		assert.Equal(t, uint64(25), usage.CompletionTokens)
		assert.Equal(t, uint64(40), usage.TotalTokens)
	})

	t.Run("direct lowercase fields format", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"prompt_tokens":     5,
						"completion_tokens": 15,
					},
				},
			},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.NotNil(t, usage)
		assert.Equal(t, uint64(5), usage.PromptTokens)
		assert.Equal(t, uint64(15), usage.CompletionTokens)
		assert.Equal(t, uint64(20), usage.TotalTokens)
	})

	t.Run("google ai style format", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"input_tokens":  8,
						"output_tokens": 12,
					},
				},
			},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.NotNil(t, usage)
		assert.Equal(t, uint64(8), usage.PromptTokens)
		assert.Equal(t, uint64(12), usage.CompletionTokens)
		assert.Equal(t, uint64(20), usage.TotalTokens)
	})

	t.Run("anthropic nested usage format", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": map[string]any{
							"input_tokens":  7,
							"output_tokens": 13,
						},
					},
				},
			},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.NotNil(t, usage)
		assert.Equal(t, uint64(7), usage.PromptTokens)
		assert.Equal(t, uint64(13), usage.CompletionTokens)
		assert.Equal(t, uint64(20), usage.TotalTokens)
	})

	t.Run("anthropic direct fields format", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"InputTokens":  6,
						"OutputTokens": 14,
					},
				},
			},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.NotNil(t, usage)
		assert.Equal(t, uint64(6), usage.PromptTokens)
		assert.Equal(t, uint64(14), usage.CompletionTokens)
		assert.Equal(t, uint64(20), usage.TotalTokens)
	})

	t.Run("no usage found", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{"other_field": "value"},
				},
			},
		}
		usage := ExtractUsageFromResponse(resp)
		assert.Nil(t, usage)
	})
}

func TestExtractUInt64(t *testing.T) {
	testCases := []struct {
		name     string
		input    any
		expected uint64
		ok       bool
	}{
		{"int", 123, 123, true},
		{"int32", int32(123), 123, true},
		{"int64", int64(123), 123, true},
		{"float32", float32(123.0), 123, true},
		{"float64", 123.45, 123, true},
		{"string", "123", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, ok := ExtractUInt64(tc.input)
			assert.Equal(t, tc.ok, ok)
			if ok {
				assert.Equal(t, tc.expected, val)
			}
		})
	}
}
