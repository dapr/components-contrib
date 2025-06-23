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

package langchaingokit

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// LLM is a helper struct that wraps a LangChain Go model
type LLM struct {
	llms.Model
	UsageGetterFunc   func(resp *llms.ContentResponse) *conversation.UsageInfo
	StreamingDisabled bool // If true, disables streaming functionality (some providers do not support streaming)
}

var ErrStreamingNotSupported = errors.New("streaming is not supported by this model or provider")

// SupportsToolCalling returns true to indicate this component supports tool calling
func (a *LLM) SupportsToolCalling() bool {
	return true
}

// convertParametersToMap converts tool parameters from JSON string to map[string]any if needed
// This ensures langchaingo receives parameters in the expected format
func convertParametersToMap(params any) any {
	// If params is already a map, return it as-is
	if paramMap, ok := params.(map[string]any); ok {
		return paramMap
	}

	// If params is a string, try to unmarshal it as JSON
	if paramStr, ok := params.(string); ok {
		var paramMap map[string]any
		if err := json.Unmarshal([]byte(paramStr), &paramMap); err != nil {
			// If unmarshaling fails, return original string
			return params
		}
		return paramMap
	}

	// For other types, return as-is (let langchaingo handle it)
	return params
}

// convertDaprToolsToLangchainTools converts Dapr tool definitions to langchaingo format
func convertDaprToolsToLangchainTools(tools []conversation.Tool) []llms.Tool {
	if len(tools) == 0 {
		return nil
	}

	langchainTools := make([]llms.Tool, len(tools))
	for i, tool := range tools {
		langchainTools[i] = llms.Tool{
			Type: tool.Type,
			Function: &llms.FunctionDefinition{
				Name:        tool.Function.Name,
				Description: tool.Function.Description,
				Parameters:  convertParametersToMap(tool.Function.Parameters),
			},
		}
	}
	return langchainTools
}

// convertLangchainToolCallsToDapr converts langchaingo tool calls to Dapr format
func convertLangchainToolCallsToDapr(toolCalls []llms.ToolCall) []conversation.ToolCall {
	if len(toolCalls) == 0 {
		return nil
	}

	daprToolCalls := make([]conversation.ToolCall, len(toolCalls))
	for i, tc := range toolCalls {
		daprToolCalls[i] = conversation.ToolCall{
			ID:   tc.ID,
			Type: tc.Type,
			Function: conversation.ToolCallFunction{
				Name:      tc.FunctionCall.Name,
				Arguments: tc.FunctionCall.Arguments,
			},
		}
	}
	return daprToolCalls
}

// generateContent is a helper function that generates content using the LangChain Go model.
func (a *LLM) generateContent(ctx context.Context, r *conversation.ConversationRequest, opts []llms.CallOption) (*conversation.ConversationResponse, error) {
	// Debug: Check if Model is nil
	if a.Model == nil {
		return nil, errors.New("LLM Model is nil - component may not have been initialized properly (this could indicate the wrong component instance is being used)")
	}

	messages := GetMessageFromRequest(r)

	// Extract tools from inputs (typically from the first user message that contains them)
	var tools []conversation.Tool
	for _, input := range r.Inputs {
		if len(input.Tools) > 0 {
			tools = input.Tools
			break // Take tools from first input that has them
		}
	}

	// Add tools if provided
	if len(tools) > 0 {
		langchainTools := convertDaprToolsToLangchainTools(tools)
		opts = append(opts, llms.WithTools(langchainTools))
	}

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		result := conversation.ConversationResult{
			Result:     resp.Choices[i].Content,
			Parameters: r.Parameters,
		}

		// Convert tool calls if present
		if len(resp.Choices[i].ToolCalls) > 0 {
			result.ToolCalls = convertLangchainToolCallsToDapr(resp.Choices[i].ToolCalls)
			result.FinishReason = "tool_calls"
		} else {
			// No tool calls, should finish with "stop"
			result.FinishReason = "stop"
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
	// Debug: Log the Model field status before calling generateContent
	if a.Model == nil {
		return nil, errors.New("LLM Model is nil in Converse() - this indicates the component instance was not properly initialized")
	}

	opts := GetOptionsFromRequest(r)

	return a.generateContent(ctx, r, opts)
}

// ConverseStream executes a streaming conversation with the LangChain Go model.
func (a *LLM) ConverseStream(ctx context.Context, r *conversation.ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*conversation.ConversationResponse, error) {
	if a.StreamingDisabled {
		return nil, ErrStreamingNotSupported
	}
	opts := GetOptionsFromRequest(r, llms.WithStreamingFunc(streamFunc))

	return a.generateContent(ctx, r, opts)
}
