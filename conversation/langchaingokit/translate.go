package langchaingokit

import (
	"github.com/dapr/components-contrib/conversation"
)

// NOTE: These are all translations due to langchaingo data types.

// extractInt64FromGenInfo extracts an int64 value from genInfo map to extract usage data from langchaingo's GenerationInfo map in the choices response.
func extractInt64FromGenInfo(genInfo map[string]any, key string) int64 {
	if v, ok := genInfo[key]; ok {
		switch val := v.(type) {
		case int64:
			return val
		case int:
			return int64(val)
		case int32:
			return int64(val)
		case float64:
			return int64(val)
		case float32:
			return int64(val)
		}
	}
	return 0
}

// extractUsageFromLangchainGenerationInfo extracts usage statistics from langchaingo's GenerationInfo map.
// Magic strings are based on the fields here:
// ref: https://github.com/openai/openai-go/blob/main/completion.go#L192 for CompletionUsageCompletionTokensDetails
// ref: https://github.com/openai/openai-go/blob/main/completion.go#L162 for CompletionUsagePromptTokensDetails
func extractUsageFromLangchainGenerationInfo(genInfo map[string]any) *conversation.Usage {
	if genInfo == nil {
		return nil
	}

	usage := &conversation.Usage{}
	usage.CompletionTokens = extractInt64FromGenInfo(genInfo, "CompletionTokens")
	usage.PromptTokens = extractInt64FromGenInfo(genInfo, "PromptTokens")
	usage.TotalTokens = extractInt64FromGenInfo(genInfo, "TotalTokens")

	completionDetails := &conversation.CompletionTokensDetails{
		AcceptedPredictionTokens: extractInt64FromGenInfo(genInfo, "CompletionAcceptedPredictionTokens"),
		AudioTokens:              extractInt64FromGenInfo(genInfo, "CompletionAudioTokens"),
		ReasoningTokens:          extractInt64FromGenInfo(genInfo, "CompletionReasoningTokens"),
		RejectedPredictionTokens: extractInt64FromGenInfo(genInfo, "CompletionRejectedPredictionTokens"),
	}

	if completionDetails.AcceptedPredictionTokens > 0 || completionDetails.AudioTokens > 0 ||
		completionDetails.ReasoningTokens > 0 || completionDetails.RejectedPredictionTokens > 0 {
		usage.CompletionTokensDetails = completionDetails
	}

	promptDetails := &conversation.PromptTokensDetails{
		AudioTokens:  extractInt64FromGenInfo(genInfo, "PromptAudioTokens"),
		CachedTokens: extractInt64FromGenInfo(genInfo, "PromptCachedTokens"),
	}

	if promptDetails.AudioTokens > 0 || promptDetails.CachedTokens > 0 {
		usage.PromptTokensDetails = promptDetails
	}

	// Only return usage if we have at least some data
	if usage.CompletionTokens > 0 || usage.PromptTokens > 0 || usage.TotalTokens > 0 {
		return usage
	}

	return nil
}
