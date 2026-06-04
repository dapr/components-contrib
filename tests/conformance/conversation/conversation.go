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
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
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
	// load, unload, and length are stop reasons for ollama
	// FINISH, stop_sequence, max_tokens are stop reasons for bedrock
	// unknown we custom add as langchaingo does not provide stop reasons for ollama, so we put unknown for this value
	providerStopReasons := []string{"stop", "end_turn", "FinishReasonStop", "tool_calls", "load", "unload", "length", "FINISH", "stop_sequence", "max_tokens", "unknown"}

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
			require.NotNil(t, resp)
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
			// anthropic responds with end_turn but other llm providers return with
			// also, ollama due to limitations in langchaingo does not provide a stop reason, so we in the backend (contrib normalizeFinishReason() provide one)
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
			t.Logf("Stop reason for %s: '%s'", component, resp.Outputs[0].StopReason)
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

			// mistral and ollama must have tool info wrapped as text
			if component == "mistral" || component == "ollama" {
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
							langchaingokit.CreateToolCallPart(&toolCall),
						},
					},
					// anthropic expects a conversation to end with a user message to generate a response,
					// therefore, in testing an assistant msg we must also include a human msg for it to generate an output for us;
					// otherwise it assumes the conversation is over.
					langchaingokit.CreateToolResponseMessage(toolResponse),
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
			t.Logf("Stop reason for %s: '%s'", component, resp.Outputs[0].StopReason)
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
			t.Logf("Stop reason for %s: '%s'", component, resp.Outputs[0].StopReason)
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
				if component == "mistral" || component == "ollama" {
					responseMessages = append(responseMessages,
						llms.MessageContent{
							Role:  llms.ChatMessageTypeAI,
							Parts: []llms.ContentPart{langchaingokit.CreateToolCallPart(&toolCall)},
						},
						langchaingokit.CreateToolResponseMessage(toolResponse),
					)
				} else {
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
				t.Logf("Stop reason for %s (tool resp2): '%s'", component, resp2.Outputs[0].StopReason)
				assert.True(t, slices.Contains(providerStopReasons, resp2.Outputs[0].StopReason))
			} else {
				assert.NotEmpty(t, resp.Outputs[0].Choices[0].Message.Content)
				t.Logf("Stop reason for %s (tool resp): '%s'", component, resp.Outputs[0].StopReason)
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
				if component == "mistral" || component == "ollama" {
					toolResponseMessages = []llms.MessageContent{
						{
							Role: llms.ChatMessageTypeHuman,
							Parts: []llms.ContentPart{
								llms.TextContent{Text: "What's the status of my transaction T1001?"},
							},
						},
						{
							Role:  llms.ChatMessageTypeAI,
							Parts: []llms.ContentPart{langchaingokit.CreateToolCallPart(&toolCall)},
						},
						langchaingokit.CreateToolResponseMessage(toolResponse),
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
							Parts: []llms.ContentPart{toolCall},
						},
						{
							Role: llms.ChatMessageTypeTool,
							Parts: []llms.ContentPart{
								toolResponse,
							},
						},
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
				t.Logf("Stop reason for %s (history resp2): '%s'", component, resp2.Outputs[0].StopReason)
				assert.True(t, slices.Contains(providerStopReasons, resp2.Outputs[0].StopReason))
			} else {
				// it is valid too if no tool call was generated
				assert.NotEmpty(t, resp1.Outputs[0].Choices[0].Message.Content)
				t.Logf("Stop reason for %s (history resp1): '%s'", component, resp1.Outputs[0].StopReason)
				assert.True(t, slices.Contains(providerStopReasons, resp1.Outputs[0].StopReason))
			}
		})

		t.Run("test response format returned", func(t *testing.T) {
			if component == "echo" || component == "ollama" || component == "bedrock" {
				t.Skipf("component %s doesn't support structured output", component)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()

			responseFormat := map[string]any{
				"type":   "object",
				"strict": true,
				"properties": map[string]any{
					"calculation": map[string]any{
						"type":        "string",
						"description": "The mathematical expression being calculated",
					},
					"result": map[string]any{
						"type":        "number",
						"description": "The numerical result of the calculation",
					},
					"explanation": map[string]any{
						"type":        "string",
						"description": "A brief explanation of how you got the result",
					},
					"confidence": map[string]any{
						"type":        "string",
						"enum":        []any{"low", "medium", "high"},
						"description": "Your confidence level in this answer",
					},
				},
				"required":             []any{"calculation", "result", "explanation", "confidence"},
				"additionalProperties": false,
			}

			req := &conversation.Request{
				Message: &[]llms.MessageContent{
					{
						Role: llms.ChatMessageTypeHuman,
						Parts: []llms.ContentPart{
							// NOTE: this is the part that guarantees the field addition works.
							llms.TextContent{Text: "What is 2+2? Provide your answer in the structured format that I provided."},
						},
					},
				},
				ResponseFormatAsJSONSchema: responseFormat,
			}
			if component == "openai" {
				req.Temperature = 1
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Len(t, resp.Outputs, 1)
			content := resp.Outputs[0].Choices[0].Message.Content
			assert.NotEmpty(t, content)
			t.Logf("Structured output response content: %s", content)

			var result struct {
				Calculation string  `json:"calculation"`
				Result      float64 `json:"result"`
				Explanation string  `json:"explanation"`
				Confidence  string  `json:"confidence"`
			}

			err = json.Unmarshal([]byte(content), &result)
			require.NoError(t, err, "Response should be valid JSON matching the provided schema")
			assert.NotEmpty(t, result.Calculation, "Response should contain the 'calculation' field")
			assert.Equal(t, float64(4), result.Result, "Result should be 4")
			assert.NotEmpty(t, result.Explanation, "Response should contain the 'explanation' field")
			assert.Contains(t, []string{"low", "medium", "high"}, result.Confidence, "Confidence should be one of the enum values")
			t.Run("test prompt cache retention", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
				defer cancel()

				// create a long prompt to ensure it's cacheable (OpenAI requires >1024 tokens)
				longPrompt := "You are a helpful assistant. " + strings.Repeat("This is important context information that should be cached. ", 100) + "The end of the prompt."

				promptCacheRetention := 24 * time.Hour

				// first request - no cached tokens
				req1 := &conversation.Request{
					Message: &[]llms.MessageContent{
						{
							Role: llms.ChatMessageTypeSystem,
							Parts: []llms.ContentPart{
								llms.TextContent{Text: longPrompt},
							},
						},
						{
							Role: llms.ChatMessageTypeHuman,
							Parts: []llms.ContentPart{
								llms.TextContent{Text: "what is 2+2?"},
							},
						},
					},
					PromptCacheRetention: &promptCacheRetention,
					Metadata: map[string]string{
						"prompt_cache_retention": "24h",
					},
				}
				if component == "openai" {
					req1.Temperature = 1
				}

				resp1, err := conv.Converse(ctx, req1)
				require.NoError(t, err)
				require.NotNil(t, resp1)
				assert.Len(t, resp1.Outputs, 1)
				assert.NotEmpty(t, resp1.Outputs[0].Choices[0].Message.Content)
				t.Logf("Request 1 Response Content: %q", resp1.Outputs[0].Choices[0].Message.Content)
				t.Logf("Request 1 Response Length: %d characters", len(resp1.Outputs[0].Choices[0].Message.Content))

				// verify usage data is returned as this lets us check how many cached tokens there are.
				// we need usage metrics to verify this prompt cache feature works.
				if resp1.Usage != nil {
					t.Logf("Request 1 Usage - Total: %d, Prompt: %d, Completion: %d",
						resp1.Usage.TotalTokens, resp1.Usage.PromptTokens, resp1.Usage.CompletionTokens)
					if resp1.Usage.PromptTokensDetails != nil {
						t.Logf("Request 1 Prompt Details - Cached: %d, Audio: %d",
							resp1.Usage.PromptTokensDetails.CachedTokens, resp1.Usage.PromptTokensDetails.AudioTokens)
					}
					if resp1.Usage.CompletionTokensDetails != nil {
						t.Logf("Request 1 Completion Details - Reasoning: %d, Audio: %d, AcceptedPrediction: %d, RejectedPrediction: %d",
							resp1.Usage.CompletionTokensDetails.ReasoningTokens,
							resp1.Usage.CompletionTokensDetails.AudioTokens,
							resp1.Usage.CompletionTokensDetails.AcceptedPredictionTokens,
							resp1.Usage.CompletionTokensDetails.RejectedPredictionTokens)
					}
					assert.Greater(t, resp1.Usage.TotalTokens, uint64(0), "Total tokens should be greater than 0")
				} else {
					t.Logf("Request 1: No usage data returned")
				}

				// second request with same prompt - should have cached tokens
				// NOTE: Sometimes it actually takes a few requests for this to show up pending model/provider...
				req2 := &conversation.Request{
					Message: &[]llms.MessageContent{
						{
							Role: llms.ChatMessageTypeSystem,
							Parts: []llms.ContentPart{
								llms.TextContent{Text: longPrompt},
							},
						},
						{
							Role: llms.ChatMessageTypeHuman,
							Parts: []llms.ContentPart{
								llms.TextContent{Text: "what is 3+3?"},
							},
						},
					},
					PromptCacheRetention: &promptCacheRetention,
					Metadata: map[string]string{
						"prompt_cache_retention": "24h",
					},
				}
				if component == "openai" {
					req2.Temperature = 1
				}

				resp2, err := conv.Converse(ctx, req2)
				require.NoError(t, err)
				require.NotNil(t, resp2)
				assert.Len(t, resp2.Outputs, 1)
				assert.NotEmpty(t, resp2.Outputs[0].Choices[0].Message.Content)

				// Verify cached tokens are used in the second request
				if resp2.Usage != nil {
					t.Logf("Request 2 Usage - Total: %d, Prompt: %d, Completion: %d",
						resp2.Usage.TotalTokens, resp2.Usage.PromptTokens, resp2.Usage.CompletionTokens)
					if resp2.Usage.PromptTokensDetails != nil {
						t.Logf("Request 2 Prompt Details - Cached: %d, Audio: %d",
							resp2.Usage.PromptTokensDetails.CachedTokens, resp2.Usage.PromptTokensDetails.AudioTokens)
						t.Logf("Cached tokens on second request: %d", resp2.Usage.PromptTokensDetails.CachedTokens)
						// For OpenAI with proper caching, we'd expect cached tokens > 0
						// but this depends on timing and cache state, so we don't assert
					} else {
						t.Logf("Request 2: No prompt token details returned")
					}
					if resp2.Usage.CompletionTokensDetails != nil {
						t.Logf("Request 2 Completion Details - Reasoning: %d, Audio: %d, AcceptedPrediction: %d, RejectedPrediction: %d",
							resp2.Usage.CompletionTokensDetails.ReasoningTokens,
							resp2.Usage.CompletionTokensDetails.AudioTokens,
							resp2.Usage.CompletionTokensDetails.AcceptedPredictionTokens,
							resp2.Usage.CompletionTokensDetails.RejectedPredictionTokens)
					}
				} else {
					t.Logf("Request 2: No usage data returned for second request to verify that prompt caching is working")
				}
			})
		})
	})
}
