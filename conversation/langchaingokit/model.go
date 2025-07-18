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

func (a *LLM) Converse(ctx context.Context, r *conversation.Request) (res *conversation.Response, err error) {
	opts := getOptionsFromRequest(r)

	var messages []llms.MessageContent
	if r.Message != nil {
		messages = *r.Message
	}

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.Result, 0, len(resp.Choices))
	for i := range resp.Choices {
		// regular text output
		output := conversation.Result{}
		if resp.Choices[i].Content != "" {
			output.Result = resp.Choices[i].Content
		} else if resp.Choices[i].ToolCalls != nil {
			output.ToolCallRequest = resp.Choices[i].ToolCalls
		}

		output.StopReason = resp.Choices[i].StopReason
		output.Parameters = r.Parameters
		outputs = append(outputs, output)
	}

	res = &conversation.Response{
		ConversationContext: r.ConversationContext,
		Outputs:             outputs,
	}

	return res, nil
}

func getOptionsFromRequest(r *conversation.Request, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, llms.WithTemperature(r.Temperature))
	}

	if r.Tools != nil {
		opts = append(opts, llms.WithTools(*r.Tools))
	}

	return opts
}
