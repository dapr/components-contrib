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

// Package langchaingokit provides integration with LangChain Go for conversation components.

package langchaingokit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// LLM is a helper struct that wraps a LangChain Go model
type LLM struct {
	llms.Model
	UsageGetterFunc           func(resp *llms.ContentResponse) *conversation.UsageInfo
	StreamingDisabled         bool   // If true, disables streaming functionality (some providers do not support streaming)
	ToolCallStreamingDisabled bool   // If true, disables tool call streaming functionality (some providers do not support tool call with streaming)
	ProviderModelName         string // The name of the model as provided by the provider
}

var ErrStreamingNotSupported = errors.New("streaming is not supported by this model or provider")

func (a *LLM) SetProviderModelName(provider, model string) {
	a.ProviderModelName = provider + "/" + model
}

// SupportsToolCalling returns true to indicate this component supports tool calling
func (a *LLM) SupportsToolCalling() bool {
	return true
}

// Use the shared provider-compatible tool call ID generator from conversation package

// convertParametersToMap converts tool parameters from JSON string to map[string]any if needed
// This ensures langchaingo receives parameters in the expected format
func convertParametersToMap(params any) (any, error) {
	// If params is already a map, return it as-is
	if paramMap, ok := params.(map[string]any); ok {
		return paramMap, nil
	}

	// If params is a string, try to unmarshal it as JSON
	if paramStr, ok := params.(string); ok {
		var paramMap map[string]any
		if err := json.Unmarshal([]byte(paramStr), &paramMap); err != nil {
			return params, fmt.Errorf("tool parameters should be a json schema string: %w", err)
		}
		return paramMap, nil
	}

	return nil, errors.New("tool parameters should be a map or a json schema string")
}

// convertDaprToolsToLangchainTools converts Dapr tool definitions to langchaingo format
func convertDaprToolsToLangchainTools(tools []conversation.Tool) ([]llms.Tool, error) {
	if len(tools) == 0 {
		return nil, nil
	}

	errList := []error{}

	langchainTools := make([]llms.Tool, len(tools))
	for i, tool := range tools {
		parameters, err := convertParametersToMap(tool.Function.Parameters)
		if err != nil {
			errList = append(errList, fmt.Errorf("tool %s: %w", tool.Function.Name, err))
			continue
		}
		langchainTools[i] = llms.Tool{
			Type: tool.ToolType,
			Function: &llms.FunctionDefinition{
				Name:        tool.Function.Name,
				Description: tool.Function.Description,
				Parameters:  parameters,
			},
		}
	}
	if len(errList) > 0 {
		return langchainTools, errors.Join(errList...)
	}
	return langchainTools, nil
}

// generateContent is a helper function that generates content using the LangChain Go model.
func (a *LLM) generateContent(ctx context.Context, r *conversation.ConversationRequest, opts []llms.CallOption, checkStreaming bool) (*conversation.ConversationResponse, error) {
	if a.Model == nil {
		return nil, errors.New("LLM Model is nil - component not initialized")
	}

	if checkStreaming && a.StreamingDisabled {
		return nil, fmt.Errorf("%w: %s", conversation.ErrStreamingNotSupported, a.ProviderModelName)
	}

	// Build messages from all inputs using new content parts approach
	messages := GetMessageFromRequest(r)

	// Get tools from the request (new API structure)
	tools := r.Tools

	// Note: Tools are now only supported in ConversationRequest.Tools field

	// Add tools if provided
	if len(tools) > 0 {
		// Some providers do not support tool call streaming, so we return error here so they can
		// fallback to non-streaming mode.
		if checkStreaming && a.ToolCallStreamingDisabled {
			return nil, fmt.Errorf("%w: %s", conversation.ErrToolCallStreamingNotSupported, a.ProviderModelName)
		}
		langchainTools, err := convertDaprToolsToLangchainTools(tools)
		if err != nil {
			return nil, fmt.Errorf("failed to convert tools to API format: %w", err)
		}
		opts = append(opts, llms.WithTools(langchainTools))
	}

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationOutput, 0, len(resp.Choices))

	// Determine the primary finish reason from the first choice, as it's the most reliable.
	var primaryFinishReason string
	if len(resp.Choices) > 0 {
		primaryFinishReason = resp.Choices[0].StopReason
	}

	for i := range resp.Choices {
		result := conversation.ConversationOutput{
			Parameters: r.Parameters,
		}

		// Create response parts
		var parts []conversation.ContentPart

		// Add text content if available
		if resp.Choices[i].Content != "" {
			parts = append(parts, conversation.TextContentPart{Text: resp.Choices[i].Content})
		}

		// Add tool calls if present
		if len(resp.Choices[i].ToolCalls) > 0 {
			for _, tc := range resp.Choices[i].ToolCalls {
				// Generate provider-compatible ID if not provided by the provider
				toolCallID := tc.ID
				if toolCallID == "" {
					toolCallID = conversation.GenerateProviderCompatibleToolCallID()
				}

				// Fix for langchaingo Anthropic provider not setting Type field
				// Default to "function" if Type is empty
				toolCallType := tc.Type
				if toolCallType == "" {
					toolCallType = "function"
				}

				parts = append(parts, conversation.ToolCallContentPart{
					ID:       toolCallID,
					CallType: toolCallType,
					Function: conversation.ToolCallFunction{
						Name:      tc.FunctionCall.Name,
						Arguments: tc.FunctionCall.Arguments,
					},
				})
			}
		}

		// Set content parts and legacy result field
		result.Parts = parts
		result.Result = conversation.ExtractTextFromParts(parts) //nolint:staticcheck // Legacy field for text backward compatibility

		// Set finish reason: prioritize the primary reason from the first choice.
		if primaryFinishReason != "" {
			result.FinishReason = conversation.NormalizeFinishReason(primaryFinishReason)
		} else if resp.Choices[i].StopReason != "" {
			// Fallback to the current choice's reason if the primary one was empty.
			result.FinishReason = conversation.NormalizeFinishReason(resp.Choices[i].StopReason)
		} else {
			// If no reason is provided by the model, determine it from the content.
			result.FinishReason = conversation.DefaultFinishReason(parts)
		}

		outputs = append(outputs, result)
	}

	usageGetter := a.UsageGetterFunc
	if usageGetter == nil {
		usageGetter = conversation.ExtractUsageFromResponse
	}

	return &conversation.ConversationResponse{
		Outputs: outputs,
		Usage:   usageGetter(resp),
	}, nil
}

// Converse executes a non-streaming conversation with the LangChain Go model.
func (a *LLM) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	opts := GetOptionsFromRequest(r)
	return a.generateContent(ctx, r, opts, false)
}

// ConverseStream executes a streaming conversation with the LangChain Go model.
func (a *LLM) ConverseStream(ctx context.Context, r *conversation.ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*conversation.ConversationResponse, error) {
	opts := GetOptionsFromRequest(r, llms.WithStreamingFunc(streamFunc))
	return a.generateContent(ctx, r, opts, true)
}
