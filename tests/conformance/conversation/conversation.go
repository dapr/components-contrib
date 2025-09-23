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
	"slices"
	"testing"
	"time"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/mistral"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"

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
	providerStopReasons := []string{"stop", "end_turn", "FinishReasonStop", "tool_calls"}

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

			// Note: Temperature is set to 1 for OpenAI models to avoid issues with GPT-5 which does not support temperature=0.
			// This can be removed once langchaingo is updated to handle this automatically (tmc/langchaingo#1374).
			req := &conversation.Request{
				Message: &[]llms.MessageContent{
					{
						Role: llms.ChatMessageTypeHuman,
						Parts: []llms.ContentPart{
							llms.TextContent{Text: "what is the time?"},
						},
					},
				},
			}
			if component == "openai" {
				req.Temperature = 1
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
		})
		t.Run("test user message type", func(t *testing.T) {
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

			req := &conversation.Request{
				Message: &userMsgs,
			}
			if component == "openai" {
				req.Temperature = 1
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
			// anthropic responds with end_turn but other llm providers return with stop
			assert.True(t, slices.Contains(providerStopReasons, resp.Outputs[0].StopReason))
			assert.Empty(t, resp.Outputs[0].Choices[0].Message.ToolCallRequest)
		})
		t.Run("test system message type", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			systemMsgs := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeSystem,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "system msg"},
					},
				},
				// The user msg is required as anthropic is unique in the sense that they require a user msg in addition to a system msg.
				// While other providers such as openai do not have this requirement.
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "user msg"},
					},
				},
			}

			req := &conversation.Request{
				Message: &systemMsgs,
			}
			if component == "openai" {
				req.Temperature = 1
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
			// anthropic responds with end_turn but other llm providers return with stop
			assert.True(t, slices.Contains(providerStopReasons, resp.Outputs[0].StopReason), resp.Outputs[0].StopReason)
			assert.Empty(t, resp.Outputs[0].Choices[0].Message.ToolCallRequest)
		})
		t.Run("test assistant message type", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			// Create tool call and response for reuse
			toolCall := llms.ToolCall{
				ID:   "tool_id",
				Type: "function",
				FunctionCall: &llms.FunctionCall{
					Name:      "testfunc",
					Arguments: `{"test": "value"}`,
				},
			}

			toolResponse := llms.ToolCallResponse{
				ToolCallID: "tool_id",
				Name:       "testfunc",
				Content:    "test-response",
			}

			var assistantMsgs []llms.MessageContent

			// mistral must have tool info wrapped as text
			if component != "mistral" {
				assistantMsgs = []llms.MessageContent{
					{
						Role: llms.ChatMessageTypeAI,
						Parts: []llms.ContentPart{
							llms.TextContent{Text: "assistant msg"},
						},
					},
					{
						Role: llms.ChatMessageTypeAI,
						Parts: []llms.ContentPart{
							toolCall,
						},
					},
					// anthropic expects a conversation to end with a user message to generate a response,
					// therefore, in testing an assistant msg we must also include a human msg for it to generate an output for us;
					// otherwise it assumes the conversation is over.
					{
						Role: llms.ChatMessageTypeTool,
						Parts: []llms.ContentPart{
							toolResponse,
						},
					},
					{
						Role: llms.ChatMessageTypeHuman,
						Parts: []llms.ContentPart{
							llms.TextContent{Text: "continue the conversation"},
						},
					},
				}
			} else {
				assistantMsgs = []llms.MessageContent{
					{
						Role: llms.ChatMessageTypeAI,
						Parts: []llms.ContentPart{
							llms.TextContent{Text: "assistant msg"},
						},
					},
					{
						Role: llms.ChatMessageTypeAI,
						Parts: []llms.ContentPart{
							mistral.CreateToolCallPart(&toolCall),
						},
					},
					// anthropic expects a conversation to end with a user message to generate a response,
					// therefore, in testing an assistant msg we must also include a human msg for it to generate an output for us;
					// otherwise it assumes the conversation is over.
					mistral.CreateToolResponseMessage(toolResponse),
					{
						Role: llms.ChatMessageTypeHuman,
						Parts: []llms.ContentPart{
							llms.TextContent{Text: "continue the conversation"},
						},
					},
				}
			}

			req := &conversation.Request{
				Message: &assistantMsgs,
			}
			if component == "openai" {
				req.Temperature = 1
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)

			// We expect a single output. In the future, depending on request (so probably a different test),
			// we might get more than one as shown in new API platform.openai.com/docs/api-reference/responses/object
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
			// anthropic responds with end_turn but other llm providers return with stop
			assert.True(t, slices.Contains(providerStopReasons, resp.Outputs[0].StopReason))
			if resp.Outputs[0].Choices[0].Message.ToolCallRequest != nil && len(*resp.Outputs[0].Choices[0].Message.ToolCallRequest) > 0 {
				assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.ToolCallRequest)
				require.JSONEq(t, `{"test": "value"}`, (*resp.Outputs[0].Choices[0].Message.ToolCallRequest)[0].FunctionCall.Arguments)
			}
		})

		t.Run("test developer message type", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			developerMsgs := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "developer msg"},
					},
				},
			}

			req := &conversation.Request{
				Message: &developerMsgs,
			}
			if component == "openai" {
				req.Temperature = 1
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
			// anthropic responds with end_turn but other llm providers return with stop
			assert.True(t, slices.Contains(providerStopReasons, resp.Outputs[0].StopReason))
			if resp.Outputs[0].Choices[0].Message.ToolCallRequest != nil {
				assert.Empty(t, *resp.Outputs[0].Choices[0].Message.ToolCallRequest)
			}
		})

		t.Run("test tool message type - confirming active tool calling capability (empty tool choice)", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()

			messages := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "What is this open source project called?"},
					},
				},
			}

			tools := []llms.Tool{
				{
					Type: "function",
					Function: &llms.FunctionDefinition{
						Name:        "get_project_name",
						Description: "Get the name of an open source project",
						Parameters: map[string]any{
							"type": "object",
							"properties": map[string]any{
								"repo_link": map[string]any{
									"type":        "string",
									"description": "The repository link",
								},
							},
							"required": []string{"repo_link"},
						},
					},
				},
			}

			req := &conversation.Request{
				Message: &messages,
				Tools:   &tools,
			}
			if component == "openai" {
				req.Temperature = 1
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)

			if resp.Outputs[0].Choices[0].Message.ToolCallRequest != nil {
				assert.GreaterOrEqual(t, len(*resp.Outputs[0].Choices[0].Message.ToolCallRequest), 1)

				var toolCall llms.ToolCall
				for i := range *resp.Outputs[0].Choices[0].Message.ToolCallRequest {
					if (*resp.Outputs[0].Choices[0].Message.ToolCallRequest)[i].FunctionCall.Name == "get_project_name" {
						toolCall = (*resp.Outputs[0].Choices[0].Message.ToolCallRequest)[i]
						break
					}
				}
				require.NotEmpty(t, toolCall.ID)
				assert.Equal(t, "get_project_name", toolCall.FunctionCall.Name)
				assert.Contains(t, toolCall.FunctionCall.Arguments, "repo_link")

				toolResponse := llms.ToolCallResponse{
					ToolCallID: toolCall.ID,
					Name:       "get_project_name",
					Content:    "The open source project name is Dapr.",
				}

				responseMessages := []llms.MessageContent{
					{
						Role: llms.ChatMessageTypeHuman,
						Parts: []llms.ContentPart{
							llms.TextContent{Text: "What is the project name?"},
						},
					},
				}

				// mistral must have tool info wrapped as text
				if component != "mistral" {
					responseMessages = append(responseMessages,
						llms.MessageContent{
							Role:  llms.ChatMessageTypeAI,
							Parts: []llms.ContentPart{toolCall},
						},
						llms.MessageContent{
							Role:  llms.ChatMessageTypeTool,
							Parts: []llms.ContentPart{toolResponse},
						},
					)
				} else {
					responseMessages = append(responseMessages,
						llms.MessageContent{
							Role:  llms.ChatMessageTypeAI,
							Parts: []llms.ContentPart{mistral.CreateToolCallPart(&toolCall)},
						},
						mistral.CreateToolResponseMessage(toolResponse),
					)
				}

				req2 := &conversation.Request{
					Message: &responseMessages,
				}
				if component == "openai" {
					req2.Temperature = 1
				}

				resp2, err2 := conv.Converse(ctx, req2)
				require.NoError(t, err2)
				assert.Len(t, resp2.Outputs, 1)
				assert.NotEmpty(t, resp2.Outputs[0].Choices[0].Message.Content)
				assert.True(t, slices.Contains(providerStopReasons, resp2.Outputs[0].StopReason))
			} else {
				assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
				assert.True(t, slices.Contains(providerStopReasons, resp.Outputs[0].StopReason))
			}
		})

		t.Run("test conversation history with tool calls", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()

			// Note: this subtest is created based on mistral tool calling docs example:
			// https://docs.mistral.ai/capabilities/function_calling/
			messages := []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "What's the status of my transaction T1001?"},
					},
				},
			}

			tools := []llms.Tool{
				{
					Type: "function",
					Function: &llms.FunctionDefinition{
						Name:        "retrieve_payment_status",
						Description: "Get payment status of a transaction",
						Parameters: map[string]any{
							"type": "object",
							"properties": map[string]any{
								"transaction_id": map[string]any{
									"type":        "string",
									"description": "The transaction id.",
								},
								"items": map[string]any{
									"type":        "array",
									"description": "List of items in the transaction",
									"items": map[string]any{
										"type": "string",
									},
								},
							},
							"required": []string{"transaction_id", "items"},
						},
					},
				},
			}

			req1 := &conversation.Request{
				Message: &messages,
				Tools:   &tools,
			}
			if component == "openai" {
				req1.Temperature = 1
			}

			resp1, err := conv.Converse(ctx, req1)
			require.NoError(t, err)

			// handle potentially multiple outputs from different llm providers
			var toolCall llms.ToolCall
			found := false
			for _, output := range resp1.Outputs {
				if output.Choices[0].Message.ToolCallRequest != nil {
					// find the tool call with the expected function name
					for i := range *output.Choices[0].Message.ToolCallRequest {
						if (*output.Choices[0].Message.ToolCallRequest)[i].FunctionCall.Name == "retrieve_payment_status" {
							toolCall = (*output.Choices[0].Message.ToolCallRequest)[i]
							found = true
							break
						}
					}
					if found {
						break
					}
				}
			}

			// check if we got a tool call request
			if found {
				assert.Equal(t, "retrieve_payment_status", toolCall.FunctionCall.Name)
				assert.Contains(t, toolCall.FunctionCall.Arguments, "transaction_id")

				toolResponse := llms.ToolCallResponse{
					ToolCallID: toolCall.ID,
					Name:       "retrieve_payment_status",
					Content:    `{"status": "Paid"}`,
				}

				var toolResponseMessages []llms.MessageContent
				if component != "mistral" {
					toolResponseMessages = []llms.MessageContent{
						{
							Role: llms.ChatMessageTypeHuman,
							Parts: []llms.ContentPart{
								llms.TextContent{Text: "What's the status of my transaction T1001?"},
							},
						},
						{
							Role:  llms.ChatMessageTypeAI,
							Parts: []llms.ContentPart{toolCall},
						},
						{
							Role: llms.ChatMessageTypeTool,
							Parts: []llms.ContentPart{
								toolResponse,
							},
						},
					}
				} else {
					toolResponseMessages = []llms.MessageContent{
						{
							Role: llms.ChatMessageTypeHuman,
							Parts: []llms.ContentPart{
								llms.TextContent{Text: "What's the status of my transaction T1001?"},
							},
						},
						{
							Role:  llms.ChatMessageTypeAI,
							Parts: []llms.ContentPart{mistral.CreateToolCallPart(&toolCall)},
						},
						mistral.CreateToolResponseMessage(toolResponse),
					}
				}

				req2 := &conversation.Request{
					Message: &toolResponseMessages,
				}
				if component == "openai" {
					req2.Temperature = 1
				}

				resp2, err := conv.Converse(ctx, req2)
				require.NoError(t, err)
				assert.Len(t, resp2.Outputs, 1)
				assert.NotEmpty(t, resp2.Outputs[0].Choices[0].Message.Content)
				assert.True(t, slices.Contains(providerStopReasons, resp2.Outputs[0].StopReason))
			} else {
				// it is valid too if no tool call was generated
				assert.NotEmpty(t, resp1.Outputs[0].Choices[0].Message.Content)
				assert.True(t, slices.Contains(providerStopReasons, resp1.Outputs[0].StopReason))
			}
		})
	})
}
