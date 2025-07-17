/*
Copyright 2024 The Dapr Authors
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
package echo

import (
	"context"
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
	"github.com/tmc/langchaingo/llms"
)

// Echo implement is only for test.
type Echo struct {
	model  string
	logger logger.Logger
}

func NewEcho(logger logger.Logger) conversation.Conversation {
	e := &Echo{
		logger: logger,
	}

	return e
}

func (e *Echo) Init(ctx context.Context, meta conversation.Metadata) error {
	r := &conversation.ConversationRequest{}
	err := kmeta.DecodeMetadata(meta.Properties, r)
	if err != nil {
		return err
	}

	e.model = r.Model

	return nil
}

func (e *Echo) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.ConversationRequest{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

// Converse returns inputs directly.
func (e *Echo) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	outputs := make([]conversation.ConversationResult, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     input.Message,
			Parameters: r.Parameters,
		})
	}

	res = &conversation.ConversationResponse{
		ConversationContext: r.ConversationContext,
		Outputs:             outputs,
	}

	return res, nil
}

func (e *Echo) Close() error {
	return nil
}

// ConverseV1Alpha2 returns inputs directly.
func (e *Echo) ConverseV1Alpha2(ctx context.Context, r *conversation.ConversationRequestV1Alpha2) (res *conversation.ConversationResponseV1Alpha2, err error) {
	var outputs []conversation.ConversationResultV1Alpha2
	if r.Message != nil {
		outputs = make([]conversation.ConversationResultV1Alpha2, 0, len(*r.Message))

		for _, message := range *r.Message {
			var content string
			for _, part := range message.Parts {
				switch p := part.(type) {
				case llms.TextContent:
					content += p.Text
				}
			}

			result := conversation.ConversationResultV1Alpha2{
				Result:     content,
				Parameters: r.Parameters,
			}

			for _, part := range message.Parts {
				switch p := part.(type) {
				// TODO: doulbe check this is right or if tool calls by assistant should just be a message content or special msg type
				case *llms.ToolCall:
					result.ToolCallRequest = append(result.ToolCallRequest, *p)
				case llms.ToolCallResponse:
					result.ToolCallRequest = append(result.ToolCallRequest, llms.ToolCall{
						ID:   p.ToolCallID,
						Type: "function",
						FunctionCall: &llms.FunctionCall{
							Name:      p.Name,
							Arguments: p.Content,
						},
					})
					result.Result = p.Content
				}
			}

			outputs = append(outputs, result)
		}
	}

	res = &conversation.ConversationResponseV1Alpha2{
		ConversationContext: r.ConversationContext,
		Outputs:             outputs,
	}

	return res, nil
}
