package anthropic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

func TestAnthropicUsageExtraction(t *testing.T) {
	t.Run("extracts usage from nested usage format", func(t *testing.T) {
		usage := map[string]any{
			"input_tokens":  float64(10),
			"output_tokens": float64(20),
		}
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": usage,
					},
				},
			},
		}

		result := conversation.ExtractUsageFromResponse(resp)
		require.NotNil(t, result)
		assert.Equal(t, uint64(10), result.PromptTokens)
		assert.Equal(t, uint64(20), result.CompletionTokens)
		assert.Equal(t, uint64(30), result.TotalTokens)
	})

	t.Run("extracts usage from direct fields format", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"InputTokens":  int64(15),
						"OutputTokens": int64(25),
					},
				},
			},
		}

		result := conversation.ExtractUsageFromResponse(resp)
		require.NotNil(t, result)
		assert.Equal(t, uint64(15), result.PromptTokens)
		assert.Equal(t, uint64(25), result.CompletionTokens)
		assert.Equal(t, uint64(40), result.TotalTokens)
	})

	t.Run("returns nil for a nil response", func(t *testing.T) {
		result := conversation.ExtractUsageFromResponse(nil)
		assert.Nil(t, result)
	})

	t.Run("returns nil for a response with no choices", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{},
		}
		result := conversation.ExtractUsageFromResponse(resp)
		assert.Nil(t, result)
	})

	t.Run("returns nil when usage key is missing", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"other_info": "value",
					},
				},
			},
		}
		result := conversation.ExtractUsageFromResponse(resp)
		assert.Nil(t, result)
	})

	t.Run("returns nil when usage data is not a map", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": "not-a-map",
					},
				},
			},
		}
		result := conversation.ExtractUsageFromResponse(resp)
		assert.Nil(t, result)
	})

	t.Run("handles missing token keys gracefully", func(t *testing.T) {
		usage := map[string]any{
			"input_tokens": float64(10),
			// "output_tokens" is missing
		}
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": usage,
					},
				},
			},
		}

		result := conversation.ExtractUsageFromResponse(resp)
		require.NotNil(t, result)
		assert.Equal(t, uint64(10), result.PromptTokens)
		assert.Equal(t, uint64(0), result.CompletionTokens)
		assert.Equal(t, uint64(10), result.TotalTokens)
	})

	t.Run("handles non-numeric token values gracefully", func(t *testing.T) {
		usage := map[string]any{
			"input_tokens":  "10", // string instead of number
			"output_tokens": float64(20),
		}
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": usage,
					},
				},
			},
		}

		result := conversation.ExtractUsageFromResponse(resp)
		require.NotNil(t, result)
		assert.Equal(t, uint64(0), result.PromptTokens) // Fails to parse, defaults to 0
		assert.Equal(t, uint64(20), result.CompletionTokens)
		assert.Equal(t, uint64(20), result.TotalTokens)
	})
}
