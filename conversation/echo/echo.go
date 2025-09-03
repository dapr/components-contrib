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
	"sort"
	"strconv"
	"strings"

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
	if r == nil || r.Message == nil {
		return &conversation.Response{
			ConversationContext: r.ConversationContext,
			Outputs:             []conversation.Result{},
		}, nil
	}

	// if we get tools, respond with tool calls for each tool
	var toolCalls []llms.ToolCall
	if r.Tools != nil {
		// create tool calls for each tool
		toolCalls = make([]llms.ToolCall, 0, len(*r.Tools))
		for id, tool := range *r.Tools {
			// extract argument names from parameters.properties
			if tool.Function == nil {
				continue // skip if no function
			}
			// try to get parameters/arg-names from tool function if any
			var parameters map[string]any
			var argNames []string
			if tool.Function.Parameters != nil {
				// ensure parameters are a map
				ok := false
				parameters, ok = tool.Function.Parameters.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("tool function parameters must be a map[string]any, got %T", tool.Function.Parameters)
				}
			}
			// try get arg names from properties
			if properties, ok := parameters["properties"]; ok {
				_, ok = properties.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("tool function properties must be a map[string]any, got %T", properties)
				}
				if propMap, ok := properties.(map[string]any); ok && len(propMap) != 0 {
					argNames = make([]string, 0, len(propMap))
					for argName := range propMap {
						argNames = append(argNames, argName)
					}
					// sort the arg names to keep deterministic order for tests
					sort.Strings(argNames)
				}
			}

			toolCalls = append(toolCalls, llms.ToolCall{
				ID:   strconv.Itoa(id),
				Type: tool.Type,
				FunctionCall: &llms.FunctionCall{
					Name:      tool.Function.Name,
					Arguments: strings.Join(argNames, ","),
				},
			})
		}
	}

	// iterate over each message in the request to echo back the content in the response. We respond with the acummulated content of the message parts and tool responses
	contentFromMessaged := make([]string, 0, len(*r.Message))
	for _, message := range *r.Message {
		for _, part := range message.Parts {
			switch p := part.(type) {
			case llms.TextContent:
				// append to slice that we'll join later with new line separators
				contentFromMessaged = append(contentFromMessaged, p.Text)
			case llms.ToolCall:
				// in case we added explicit tool calls on the request like on multi-turn conversations. We still append tool calls for each tool defined in the request.
				toolCalls = append(toolCalls, p)
			case llms.ToolCallResponse:
				// show tool responses on the request like on multi-turn conversations
				contentFromMessaged = append(contentFromMessaged, fmt.Sprintf("Tool Response for tool ID '%s' with name '%s': %s", p.ToolCallID, p.Name, p.Content))
			default:
				return nil, fmt.Errorf("found invalid content type as input for %v", p)
			}
		}
	}

	stopReason := "stop"
	if len(toolCalls) > 0 {
		stopReason = "tool_calls"
		// follows openai spec for tool_calls finish reason https://platform.openai.com/docs/api-reference/chat/object
	}
	choice := conversation.Choice{
		FinishReason: stopReason,
		Index:        0,
		Message: conversation.Message{
			Content: strings.Join(contentFromMessaged, "\n"),
		},
	}

	if len(toolCalls) > 0 {
		choice.Message.ToolCallRequest = &toolCalls
	}

	output := conversation.Result{
		StopReason: stopReason,
		Choices:    []conversation.Choice{choice},
	}

	res = &conversation.Response{
		ConversationContext: r.ConversationContext,
		Outputs:             []conversation.Result{output},
	}

	return res, nil
}

func (e *Echo) Close() error {
	return nil
}
