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

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// LLM is a helper struct that wraps a LangChain Go model
type LLM struct {
	llms.Model
}

func (a *LLM) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	messages := getMessageFromRequest(r)
	opts := getOptionsFromRequest(r)

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		output := conversation.ConversationResult{}
		if resp.Choices[i].Content != "" {
			output.Result = resp.Choices[i].Content
			output.Parameters = r.Parameters
		}
		outputs = append(outputs, output)
	}

	res = &conversation.ConversationResponse{
		Outputs: outputs,
	}

	return res, nil
}

func (a *LLM) ConverseV1Alpha2(ctx context.Context, r *conversation.ConversationRequestV1Alpha2) (res *conversation.ConversationResponseV1Alpha2, err error) {
	opts := getOptionsFromRequestV1Alpha2(r)

	var messages []llms.MessageContent
	if r.Message != nil {
		messages = *r.Message
	}

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResultV1Alpha2, 0, len(resp.Choices))
	for i := range resp.Choices {
		// regular text output
		output := conversation.ConversationResultV1Alpha2{}
		if resp.Choices[i].Content != "" {
			output.Result = resp.Choices[i].Content
		} else if resp.Choices[i].ToolCalls != nil {
			output.ToolCallRequest = resp.Choices[i].ToolCalls
		}

		output.StopReason = resp.Choices[i].StopReason
		output.Parameters = r.Parameters
		outputs = append(outputs, output)
	}

	res = &conversation.ConversationResponseV1Alpha2{
		ConversationContext: r.ConversationContext,
		Outputs:             outputs,
	}

	return res, nil
}
