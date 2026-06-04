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

// Usage represents token usage statistics for a completion request
type Usage struct {
	CompletionTokens        uint64                   `json:"completionTokens"`
	PromptTokens            uint64                   `json:"promptTokens"`
	TotalTokens             uint64                   `json:"totalTokens"`
	CompletionTokensDetails *CompletionTokensDetails `json:"completionTokensDetails,omitempty"`
	PromptTokensDetails     *PromptTokensDetails     `json:"promptTokensDetails,omitempty"`
}

// CompletionTokensDetails provides breakdown of completion tokens
type CompletionTokensDetails struct {
	AcceptedPredictionTokens uint64 `json:"acceptedPredictionTokens"`
	AudioTokens              uint64 `json:"audioTokens"`
	ReasoningTokens          uint64 `json:"reasoningTokens"`
	RejectedPredictionTokens uint64 `json:"rejectedPredictionTokens"`
}

// PromptTokensDetails provides breakdown of prompt tokens
type PromptTokensDetails struct {
	AudioTokens  uint64 `json:"audioTokens"`
	CachedTokens uint64 `json:"cachedTokens"`
}
