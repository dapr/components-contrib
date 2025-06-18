package conversation

import (
	"github.com/tmc/langchaingo/llms"
)

// UsageInfo represents token usage information extracted from LLM responses
type UsageInfo struct {
	PromptTokens     int32
	CompletionTokens int32
	TotalTokens      int32 // Optional, calculated if not provided
	ReasoningTokens  int32 // Optional, used by some models
}

// ExtractUsageFromResponse extracts token usage information from a LangChain Go ContentResponse
// Different LLM providers may store usage information in different formats within GenerationInfo
func ExtractUsageFromResponse(resp *llms.ContentResponse) *UsageInfo {
	if resp == nil || len(resp.Choices) == 0 {
		return nil
	}

	// Check the first choice for usage information
	choice := resp.Choices[0]
	if choice.GenerationInfo == nil {
		return nil
	}

	usage := &UsageInfo{}
	found := false

	// Try different common patterns for usage information
	// Pattern 1: LangChain Go standard format (used by OpenAI and others)
	if promptTokens, ok := ExtractInt32(choice.GenerationInfo["PromptTokens"]); ok {
		usage.PromptTokens = promptTokens
		found = true
	}
	if completionTokens, ok := ExtractInt32(choice.GenerationInfo["CompletionTokens"]); ok {
		usage.CompletionTokens = completionTokens
		found = true
	}

	// Pattern 2: OpenAI-style usage in nested map
	if !found {
		if usageMap, ok := choice.GenerationInfo["usage"].(map[string]interface{}); ok {
			if promptTokens, ok := ExtractInt32(usageMap["prompt_tokens"]); ok {
				usage.PromptTokens = promptTokens
				found = true
			}
			if completionTokens, ok := ExtractInt32(usageMap["completion_tokens"]); ok {
				usage.CompletionTokens = completionTokens
				found = true
			}
			if totalTokens, ok := ExtractInt32(usageMap["total_tokens"]); ok {
				usage.TotalTokens = totalTokens
			}
		}
	}

	// Pattern 3: Direct fields in GenerationInfo (lowercase)
	if !found {
		if promptTokens, ok := ExtractInt32(choice.GenerationInfo["prompt_tokens"]); ok {
			usage.PromptTokens = promptTokens
			found = true
		}
		if completionTokens, ok := ExtractInt32(choice.GenerationInfo["completion_tokens"]); ok {
			usage.CompletionTokens = completionTokens
			found = true
		}
		if totalTokens, ok := ExtractInt32(choice.GenerationInfo["total_tokens"]); ok {
			usage.TotalTokens = totalTokens
		}
	}

	// Pattern 4: Google AI/Vertex AI style
	if !found {
		if promptTokens, ok := ExtractInt32(choice.GenerationInfo["input_tokens"]); ok {
			usage.PromptTokens = promptTokens
			found = true
		}
		if completionTokens, ok := ExtractInt32(choice.GenerationInfo["output_tokens"]); ok {
			usage.CompletionTokens = completionTokens
			found = true
		}
		if totalTokens, ok := ExtractInt32(choice.GenerationInfo["total_tokens"]); ok {
			usage.TotalTokens = totalTokens
		}
	}

	// Pattern 5: Anthropic style (if they provide usage info)
	if !found {
		if usageInfo, ok := choice.GenerationInfo["usage"].(map[string]interface{}); ok {
			if inputTokens, ok := ExtractInt32(usageInfo["input_tokens"]); ok {
				usage.PromptTokens = inputTokens
				found = true
			}
			if outputTokens, ok := ExtractInt32(usageInfo["output_tokens"]); ok {
				usage.CompletionTokens = outputTokens
				found = true
			}
		}
	}

	// Pattern 6: Anthropic direct fields in GenerationInfo
	if !found {
		if inputTokens, ok := ExtractInt32(choice.GenerationInfo["InputTokens"]); ok {
			usage.PromptTokens = inputTokens
			found = true
		}
		if outputTokens, ok := ExtractInt32(choice.GenerationInfo["OutputTokens"]); ok {
			usage.CompletionTokens = outputTokens
			found = true
		}
	}

	// Calculate total if not provided but we have prompt and completion
	if found && usage.TotalTokens == 0 && (usage.PromptTokens > 0 || usage.CompletionTokens > 0) {
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	}

	if found {
		return usage
	}
	return nil
}

// ExtractInt32 safely extracts an int32 value from an interface{}
func ExtractInt32(value interface{}) (int32, bool) {
	switch v := value.(type) {
	case int:
		return int32(v), true
	case int32:
		return v, true
	case int64:
		return int32(v), true
	case float64:
		return int32(v), true
	case float32:
		return int32(v), true
	default:
		return 0, false
	}
}
