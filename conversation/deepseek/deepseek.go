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

package deepseek

import (
	"context"
	"errors"
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	deepseek_go "github.com/cohesion-org/deepseek-go"
	"github.com/tmc/langchaingo/llms"
)

type Deepseek struct {
	llm *deepseek_go.Client
	md  DeepseekMetadata

	logger logger.Logger
}

func NewDeepseek(logger logger.Logger) conversation.Conversation {
	o := &Deepseek{
		logger: logger,
	}

	return o
}

func (d *Deepseek) Init(ctx context.Context, meta conversation.Metadata) error {
	md := DeepseekMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	d.llm = deepseek_go.NewClient(md.Key)
	d.md = md
	return nil
}

func (d *Deepseek) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := DeepseekMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (d *Deepseek) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	messages := make([]deepseek_go.ChatCompletionMessage, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		messages = append(messages, deepseek_go.ChatCompletionMessage{
			Role:    string(input.Role),
			Content: input.Message,
		})
	}

	request := &deepseek_go.ChatCompletionRequest{
		Model:    deepseek_go.DeepSeekChat,
		Messages: messages,
	}

	if d.md.MaxTokens > 0 {
		request.MaxTokens = d.md.MaxTokens
	}

	if r.Temperature > 0 {
		request.Temperature = float32(r.Temperature)
	}

	resp, err := d.llm.CreateChatCompletion(ctx, request)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     resp.Choices[i].Message.Content,
			Parameters: r.Parameters,
		})
	}

	res = &conversation.ConversationResponse{
		Outputs: outputs,
	}

	return res, nil
}

func (d *Deepseek) ConverseV1Alpha2(ctx context.Context, r *conversation.ConversationRequestV1Alpha2) (res *conversation.ConversationResponseV1Alpha2, err error) {
	if r.Message == nil {
		return nil, errors.New("message is nil")
	}
	messages := make([]deepseek_go.ChatCompletionMessage, 0, len(*r.Message))

	// TODO: mv this translation logic elsewhere to clean this up

	// contrib types are specific to langchaingo since most of the components use this;
	// however, deepseek does not, so we must translate to deepseek_go.ChatCompletionMessage
	for _, input := range *r.Message {
		var content string
		for _, part := range input.Parts {
			switch p := part.(type) {
			case llms.TextContent:
				content += p.Text
			}
		}

		messages = append(messages, deepseek_go.ChatCompletionMessage{
			Role:    string(input.Role),
			Content: content,
		})
	}

	request := &deepseek_go.ChatCompletionRequest{
		Model:    deepseek_go.DeepSeekChat,
		Messages: messages,
	}

	if d.md.MaxTokens > 0 {
		request.MaxTokens = d.md.MaxTokens
	}

	if r.Temperature > 0 {
		request.Temperature = float32(r.Temperature)
	}

	if r.Tools != nil {
		deepseekTools := make([]deepseek_go.Tool, 0, len(*r.Tools))
		for _, tool := range *r.Tools {
			deepseekTool := deepseek_go.Tool{
				Type: tool.Type,
			}
			if tool.Function != nil {
				deepseekTool.Function = deepseek_go.Function{
					Name:        tool.Function.Name,
					Description: tool.Function.Description,
					Parameters:  tool.Function.Parameters.(*deepseek_go.FunctionParameters), // TODO: double check this...
				}
			}
			deepseekTools = append(deepseekTools, deepseekTool)
		}
		request.Tools = deepseekTools
	}

	// TODO(@Sicoyle): do tool choice opt too for all of these!

	resp, err := d.llm.CreateChatCompletion(ctx, request)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResultV1Alpha2, 0, len(resp.Choices))

	for i := range resp.Choices {
		outputs = append(outputs, conversation.ConversationResultV1Alpha2{
			Result:     resp.Choices[i].Message.Content,
			Parameters: r.Parameters,
		})
	}

	res = &conversation.ConversationResponseV1Alpha2{
		ConversationContext: r.ConversationContext,
		Outputs:             outputs,
	}

	return res, nil
}

func (d *Deepseek) Close() error {
	return nil
}
