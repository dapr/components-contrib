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
	"fmt"

	"github.com/dapr/components-contrib/conversation"
)

// NOTE: These are all translations due to langchaingo data types.

// exposing to use also in tests
const (
	completionKey         = "CompletionTokens"
	promptKey             = "PromptTokens"
	totalKey              = "TotalTokens"
	completionAcceptedKey = "CompletionAcceptedPredictionTokens"
	completionAudioKey    = "CompletionAudioTokens"
	reasoningKey          = "CompletionReasoningTokens"
	rejectedKey           = "CompletionRejectedPredictionTokens"
	promptAudioKey        = "PromptAudioTokens"
	promptCachedKey       = "PromptCachedTokens"
)

// extractUint64FromGenInfo extracts a uint64 value from genInfo map to extract usage data from langchaingo's GenerationInfo map in the choices response.
func extractUint64FromGenInfo(genInfo map[string]any, key string) (uint64, error) {
	if v, ok := genInfo[key]; ok {
		switch val := v.(type) {
		case uint64:
			return val, nil
		case int:
			if val < 0 {
				return 0, fmt.Errorf("negative value for %s: %d", key, val)
			}
			return uint64(val), nil
		case int64:
			if val < 0 {
				return 0, fmt.Errorf("negative value for %s: %d", key, val)
			}
			return uint64(val), nil
		case int32:
			if val < 0 {
				return 0, fmt.Errorf("negative value for %s: %d", key, val)
			}
			return uint64(val), nil
		case float64:
			if val < 0 {
				return 0, fmt.Errorf("negative value for %s: %f", key, val)
			}
			return uint64(val), nil
		case float32:
			if val < 0 {
				return 0, fmt.Errorf("negative value for %s: %f", key, val)
			}
			return uint64(val), nil
		default:
			return 0, fmt.Errorf("failed to extract usage metrics for type: %T", val)
		}
	}
	return 0, nil
}

// extractUsageFromLangchainGenerationInfo extracts usage statistics from langchaingo's GenerationInfo map.
// Magic strings are based on the fields here:
// ref: https://github.com/openai/openai-go/blob/main/completion.go#L192 for CompletionUsageCompletionTokensDetails
// ref: https://github.com/openai/openai-go/blob/main/completion.go#L162 for CompletionUsagePromptTokensDetails
func extractUsageFromLangchainGenerationInfo(genInfo map[string]any) (*conversation.Usage, error) {
	if genInfo == nil {
		return nil, nil
	}

	usage := &conversation.Usage{}
	completionTokens, err := extractUint64FromGenInfo(genInfo, completionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract completion tokens: %v", err)
	}
	usage.CompletionTokens = completionTokens

	promptTokens, err := extractUint64FromGenInfo(genInfo, promptKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract prompt tokens: %v", err)
	}
	usage.PromptTokens = promptTokens

	totalTokens, err := extractUint64FromGenInfo(genInfo, totalKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract total tokens: %v", err)
	}
	usage.TotalTokens = totalTokens

	acceptedTokens, err := extractUint64FromGenInfo(genInfo, completionAcceptedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract completion accepted prediction tokens: %v", err)
	}

	audioTokens, err := extractUint64FromGenInfo(genInfo, completionAudioKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract completion audio tokens: %v", err)
	}

	reasoningTokens, err := extractUint64FromGenInfo(genInfo, reasoningKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract completion reasoning tokens: %v", err)
	}

	rejectedTokens, err := extractUint64FromGenInfo(genInfo, rejectedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract completion rejected prediction tokens: %v", err)
	}

	completionDetails := &conversation.CompletionTokensDetails{
		AcceptedPredictionTokens: acceptedTokens,
		AudioTokens:              audioTokens,
		ReasoningTokens:          reasoningTokens,
		RejectedPredictionTokens: rejectedTokens,
	}

	if completionDetails.AcceptedPredictionTokens > 0 || completionDetails.AudioTokens > 0 ||
		completionDetails.ReasoningTokens > 0 || completionDetails.RejectedPredictionTokens > 0 {
		usage.CompletionTokensDetails = completionDetails
	}

	promptAudioTokens, err := extractUint64FromGenInfo(genInfo, promptAudioKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract prompt audio tokens: %v", err)
	}

	promptCachedTokens, err := extractUint64FromGenInfo(genInfo, promptCachedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract prompt cached tokens: %v", err)
	}

	promptDetails := &conversation.PromptTokensDetails{
		AudioTokens:  promptAudioTokens,
		CachedTokens: promptCachedTokens,
	}

	if promptDetails.AudioTokens > 0 || promptDetails.CachedTokens > 0 {
		usage.PromptTokensDetails = promptDetails
	}

	// Only return usage if we have at least some data
	if usage.CompletionTokens > 0 || usage.PromptTokens > 0 || usage.TotalTokens > 0 {
		return usage, nil
	}

	return nil, nil
}
