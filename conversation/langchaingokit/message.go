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
	"strings"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

func GetMessageFromRequest(r *conversation.ConversationRequest) []llms.MessageContent {
	messages := make([]llms.MessageContent, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		role := ConvertLangchainRole(input.Role)

		// Process with native parts support if available
		if len(input.Parts) > 0 {
			// Handle different combinations of parts
			var textParts []string
			var toolCalls []llms.ToolCall
			var toolResults []conversation.ToolResultContentPart

			for _, part := range input.Parts {
				switch p := part.(type) {
				case conversation.TextContentPart:
					textParts = append(textParts, p.Text)
				case conversation.ToolCallContentPart:
					toolCalls = append(toolCalls, llms.ToolCall{
						ID:   p.ID,
						Type: p.CallType,
						FunctionCall: &llms.FunctionCall{
							Name:      p.Function.Name,
							Arguments: p.Function.Arguments,
						},
					})
				case conversation.ToolResultContentPart:
					toolResults = append(toolResults, p)
				}
			}

			// Create messages based on content type
			if len(toolResults) > 0 {
				// Tool result messages
				for _, result := range toolResults {
					messages = append(messages, llms.MessageContent{
						Role: llms.ChatMessageTypeTool,
						Parts: []llms.ContentPart{
							llms.ToolCallResponse{
								ToolCallID: result.ToolCallID,
								Name:       result.Name,
								Content:    result.Content,
							},
						},
					})
				}
			} else if len(toolCalls) > 0 {
				// Assistant message with tool calls - need to include them in conversation history
				// for multi-turn conversations (especially important for Anthropic)
				message := llms.MessageContent{
					Role: llms.ChatMessageTypeAI,
				}

				// Add text content if present
				if len(textParts) > 0 {
					message.Parts = []llms.ContentPart{llms.TextPart(strings.Join(textParts, " "))}
				}

				// Add tool calls to Parts array (they implement ContentPart interface)
				for _, toolCall := range toolCalls {
					message.Parts = append(message.Parts, toolCall)
				}

				messages = append(messages, message)
			} else if len(textParts) > 0 {
				// Regular text message
				messages = append(messages, llms.MessageContent{
					Role:  role,
					Parts: []llms.ContentPart{llms.TextPart(strings.Join(textParts, " "))},
				})
			}
		} else if input.Message != "" { //nolint:staticcheck // Backward compatibility check
			// Legacy message field support
			messages = append(messages, llms.MessageContent{
				Role:  role,
				Parts: []llms.ContentPart{llms.TextPart(input.Message)}, //nolint:staticcheck // Backward compatibility
			})
		}
	}

	return messages
}

func GetOptionsFromRequest(r *conversation.ConversationRequest, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, conversation.LangchainTemperature(r.Temperature))
	}

	if r.MaxTokens > 0 {
		opts = append(opts, llms.WithMaxTokens(r.MaxTokens))
	}

	return opts
}
