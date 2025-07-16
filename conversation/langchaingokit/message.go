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
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

func getMessageFromRequest(r *conversation.ConversationRequest) []llms.MessageContent {
	messages := make([]llms.MessageContent, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		role := ConvertLangchainRole(input.Role)

		messages = append(messages, llms.MessageContent{
			Role: role,
			Parts: []llms.ContentPart{
				llms.TextPart(input.Message),
			},
		})
	}

	return messages
}

// getMessageFromRequestV1Alpha2 transforms the api inputs into the langchain go sdk messages
func getMessageFromRequestV1Alpha2(r *conversation.ConversationRequestV1Alpha2) []llms.MessageContent {
	messages := make([]llms.MessageContent, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		role := ConvertLangchainRole(input.Role)

		messages = append(messages, llms.MessageContent{
			Role: role,
			Parts: []llms.ContentPart{
				llms.TextPart(input.Message),
			},
		})

		// TODO(@Sicoyle): would this be an if or else if?
		if input.ToolCalls != nil {
			for _, tool := range input.ToolCalls {
				// build up tool call based on api input
				toolCall := llms.ToolCall{
					ID:   tool.Id,
					Type: "function",
					FunctionCall: &llms.FunctionCall{
						Name: tool.Name,
					},
				}
				if tool.Arguments != nil {
					toolCall.FunctionCall.Arguments = *tool.Arguments
				}

				// transform into langchain go message
				toolCallMessage := llms.MessageContent{
					Role:  llms.ChatMessageTypeAI,
					Parts: []llms.ContentPart{toolCall},
				}

				messages = append(messages, toolCallMessage)
			}

		}

		// TODO: tool call result message typ

	}

	return messages
}

func getOptionsFromRequest(r *conversation.ConversationRequest, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, conversation.LangchainTemperature(r.Temperature))
	}

	return opts
}

func getOptionsFromRequestV1Alpha2(r *conversation.ConversationRequestV1Alpha2, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, conversation.LangchainTemperature(r.Temperature))
	}

	return opts
}
