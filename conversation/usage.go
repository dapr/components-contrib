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
	"github.com/tmc/langchaingo/llms"
)

// UsageInfo represents token usage information extracted from LLM responses
type UsageInfo struct {
	PromptTokens     uint64
	CompletionTokens uint64
	TotalTokens      uint64 // Optional, calculated if not provided
	ReasoningTokens  uint64 // Optional, used by some models
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
	if promptTokens, ok := ExtractUInt64(choice.GenerationInfo["PromptTokens"]); ok {
		usage.PromptTokens = promptTokens
		found = true
	}
	if completionTokens, ok := ExtractUInt64(choice.GenerationInfo["CompletionTokens"]); ok {
		usage.CompletionTokens = completionTokens
		found = true
	}

	// Pattern 2: OpenAI-style usage in nested map
	if !found {
		if usageMap, ok := choice.GenerationInfo["usage"].(map[string]any); ok {
			if promptTokens, ok := ExtractUInt64(usageMap["prompt_tokens"]); ok {
				usage.PromptTokens = promptTokens
				found = true
			}
			if completionTokens, ok := ExtractUInt64(usageMap["completion_tokens"]); ok {
				usage.CompletionTokens = completionTokens
				found = true
			}
			if totalTokens, ok := ExtractUInt64(usageMap["total_tokens"]); ok {
				usage.TotalTokens = totalTokens
			}
		}
	}

	// Pattern 3: Direct fields in GenerationInfo (lowercase)
	if !found {
		if promptTokens, ok := ExtractUInt64(choice.GenerationInfo["prompt_tokens"]); ok {
			usage.PromptTokens = promptTokens
			found = true
		}
		if completionTokens, ok := ExtractUInt64(choice.GenerationInfo["completion_tokens"]); ok {
			usage.CompletionTokens = completionTokens
			found = true
		}
		if totalTokens, ok := ExtractUInt64(choice.GenerationInfo["total_tokens"]); ok {
			usage.TotalTokens = totalTokens
		}
	}

	// Pattern 4: Google AI/Vertex AI style
	if !found {
		if promptTokens, ok := ExtractUInt64(choice.GenerationInfo["input_tokens"]); ok {
			usage.PromptTokens = promptTokens
			found = true
		}
		if completionTokens, ok := ExtractUInt64(choice.GenerationInfo["output_tokens"]); ok {
			usage.CompletionTokens = completionTokens
			found = true
		}
		if totalTokens, ok := ExtractUInt64(choice.GenerationInfo["total_tokens"]); ok {
			usage.TotalTokens = totalTokens
		}
	}

	// Pattern 5: Anthropic style (if they provide usage info)
	if !found {
		if usageInfo, ok := choice.GenerationInfo["usage"].(map[string]any); ok {
			if inputTokens, ok := ExtractUInt64(usageInfo["input_tokens"]); ok {
				usage.PromptTokens = inputTokens
				found = true
			}
			if outputTokens, ok := ExtractUInt64(usageInfo["output_tokens"]); ok {
				usage.CompletionTokens = outputTokens
				found = true
			}
		}
	}

	// Pattern 6: Anthropic direct fields in GenerationInfo
	if !found {
		if inputTokens, ok := ExtractUInt64(choice.GenerationInfo["InputTokens"]); ok {
			usage.PromptTokens = inputTokens
			found = true
		}
		if outputTokens, ok := ExtractUInt64(choice.GenerationInfo["OutputTokens"]); ok {
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

// ExtractUInt64 safely extracts an int32 value from an interface{}
func ExtractUInt64(value any) (uint64, bool) {
	switch v := value.(type) {
	case int:
		return uint64(v), true //nolint:gosec // This is a valid conversion
	case int32:
		return uint64(v), true //nolint:gosec // This is a valid conversion
	case int64:
		return uint64(v), true //nolint:gosec // This is a valid conversion
	case float64:
		return uint64(v), true
	case float32:
		return uint64(v), true
	default:
		return 0, false
	}
}
