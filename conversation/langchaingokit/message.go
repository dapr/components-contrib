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

func getOptionsFromRequest(r *conversation.ConversationRequest, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, llms.WithTemperature(r.Temperature))
	}

	return opts
}

func getOptionsFromRequestV1Alpha2(r *conversation.ConversationRequestV1Alpha2, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, llms.WithTemperature(r.Temperature))
	}

	if r.Tools != nil {
		// TODO: go back and maybe make this a pointer to slice of tools instead?
		tools := make([]llms.Tool, len(r.Tools))
		for i, tool := range r.Tools {
			tools[i] = *tool
		}
		opts = append(opts, llms.WithTools(tools))
	}

	return opts
}
