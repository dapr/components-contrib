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
	"fmt"
	"reflect"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
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
	r := &conversation.Request{}
	err := kmeta.DecodeMetadata(meta.Properties, r)
	if err != nil {
		return err
	}

	e.model = r.Model

	return nil
}

func (e *Echo) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.Request{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

// Converse returns one output per input message.
func (e *Echo) Converse(ctx context.Context, r *conversation.Request) (res *conversation.Response, err error) {
	if r.Message == nil {
		return &conversation.Response{
			ConversationContext: r.ConversationContext,
			Outputs:             []conversation.Result{},
		}, nil
	}

	outputs := make([]conversation.Result, 0, len(*r.Message))

	for _, message := range *r.Message {
		var content string
		var toolCalls []llms.ToolCall

		for i, part := range message.Parts {
			switch p := part.(type) {
			case llms.TextContent:
				// end with space if not the first part
				if i > 0 && content != "" {
					content += " "
				}
				content += p.Text
			case llms.ToolCall:
				toolCalls = append(toolCalls, p)
			case llms.ToolCallResponse:
				content = p.Content
				toolCalls = append(toolCalls, llms.ToolCall{
					ID:   p.ToolCallID,
					Type: "function",
					FunctionCall: &llms.FunctionCall{
						Name:      p.Name,
						Arguments: p.Content,
					},
				})
			default:
				return nil, fmt.Errorf("found invalid content type as input for %v", p)
			}
		}

		choice := conversation.Choice{
			FinishReason: "stop",
			Index:        0,
			Message: conversation.Message{
				Content: content,
			},
		}

		if len(toolCalls) > 0 {
			choice.Message.ToolCallRequest = &toolCalls
		}

		output := conversation.Result{
			StopReason: "stop",
			Choices:    []conversation.Choice{choice},
		}

		outputs = append(outputs, output)
	}

	res = &conversation.Response{
		ConversationContext: r.ConversationContext,
		Outputs:             outputs,
	}

	return res, nil
}

func (e *Echo) Close() error {
	return nil
}
