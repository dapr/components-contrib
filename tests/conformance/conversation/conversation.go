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

package conversation

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/tmc/langchaingo/llms"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(componentName string) TestConfig {
	tc := TestConfig{
		utils.CommonConfig{
			ComponentType: "conversation",
			ComponentName: componentName,
		},
	}

	return tc
}

func ConformanceTests(t *testing.T, props map[string]string, conv conversation.Conversation, component string) {
	t.Run("init", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		err := conv.Init(ctx, conversation.Metadata{
			Base: metadata.Base{
				Properties: props,
			},
		})
		require.NoError(t, err)
	})

	if t.Failed() {
		t.Fatal("initialization failed")
	}

	t.Run("converse", func(t *testing.T) {
		t.Run("get a non-empty response without errors", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "what is the time?",
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Result)
		})
		t.Run("v1alpha2 api - test user message type", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			userMsgs := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "user msg"},
					},
				},
			}

			req := &conversation.ConversationRequestV1Alpha2{
				Message: &userMsgs,
			}
			resp, err := conv.ConverseV1Alpha2(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Result)
			assert.Equal(t, "user msg", resp.Outputs[0].Result)
		})
		t.Run("v1alpha2 api - test system message type", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			systemMsgs := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeSystem,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "system msg"},
					},
				},
			}

			req := &conversation.ConversationRequestV1Alpha2{
				Message: &systemMsgs,
			}
			resp, err := conv.ConverseV1Alpha2(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Result)
			assert.Equal(t, "system msg", resp.Outputs[0].Result)
		})
		t.Run("v1alpha2 api - test assistant message type", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			assistantMsgs := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeAI,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "assistant msg"},
					},
				},
			}

			req := &conversation.ConversationRequestV1Alpha2{
				Message: &assistantMsgs,
			}
			resp, err := conv.ConverseV1Alpha2(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Result)
			assert.Equal(t, "assistant msg", resp.Outputs[0].Result)
		})
		t.Run("v1alpha2 api - test tool call response", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			toolResponseMsgs := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeTool,
					Parts: []llms.ContentPart{
						llms.ToolCallResponse{
							ToolCallID: "tool_id",
							Name:       "get_name",
							Content:    "Dapr",
						},
					},
				},
			}

			req := &conversation.ConversationRequestV1Alpha2{
				Message: &toolResponseMsgs,
			}
			resp, err := conv.ConverseV1Alpha2(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)

			assert.Equal(t, "tool_id", resp.Outputs[0].ToolCallRequest[0].ID)
			assert.Equal(t, "get_name", resp.Outputs[0].ToolCallRequest[0].FunctionCall.Name)
			assert.Equal(t, "Dapr", resp.Outputs[0].Result)
		})
	})
}
