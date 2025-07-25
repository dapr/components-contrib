package echo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
)

func TestConverse(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []llms.MessageContent
		expected *conversation.Response
	}{
		{
			name: "basic input",
			inputs: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "hello"},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "stop",
						Choices: []conversation.Choice{
							{
								FinishReason: "stop",
								Index:        0,
								Message: conversation.Message{
									Content: "hello",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "empty input",
			inputs: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: ""},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "stop",
						Choices: []conversation.Choice{
							{
								FinishReason: "stop",
								Index:        0,
								Message: conversation.Message{
									Content: "",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple inputs with multiple content parts",
			inputs: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "first message"},
						llms.TextContent{Text: "second message"},
					},
				},
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "third message"},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "stop",
						Choices: []conversation.Choice{
							{
								FinishReason: "stop",
								Index:        0,
								Message: conversation.Message{
									Content: "first message second message",
								},
							},
						},
					},
					{
						StopReason: "stop",
						Choices: []conversation.Choice{
							{
								FinishReason: "stop",
								Index:        0,
								Message: conversation.Message{
									Content: "third message",
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEcho(logger.NewLogger("echo test"))
			e.Init(t.Context(), conversation.Metadata{})
			r, err := e.Converse(t.Context(), &conversation.Request{
				Message: &tt.inputs,
			})
			require.NoError(t, err)
			assert.Len(t, r.Outputs, len(tt.expected.Outputs))
			assert.Equal(t, tt.expected.Outputs, r.Outputs)
		})
	}
}

func TestConverseAlpha2(t *testing.T) {
	tests := []struct {
		name     string
		messages []llms.MessageContent
		expected *conversation.Response
	}{
		{
			name: "tool call request",
			messages: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeAI,
					Parts: []llms.ContentPart{
						llms.ToolCall{
							ID:   "myid",
							Type: "function",
							FunctionCall: &llms.FunctionCall{
								Name:      "myfunc",
								Arguments: `{"name": "Dapr"}`,
							},
						},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "stop",
						Choices: []conversation.Choice{
							{
								FinishReason: "stop",
								Index:        0,
								Message: conversation.Message{
									ToolCallRequest: &[]llms.ToolCall{
										{
											ID:   "myid",
											Type: "function",
											FunctionCall: &llms.FunctionCall{
												Name:      "myfunc",
												Arguments: `{"name": "Dapr"}`,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "tool call response",
			messages: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeTool,
					Parts: []llms.ContentPart{
						llms.ToolCallResponse{
							ToolCallID: "myid",
							Content:    "Dapr",
							Name:       "myfunc",
						},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "stop",
						Choices: []conversation.Choice{
							{
								FinishReason: "stop",
								Index:        0,
								Message: conversation.Message{
									Content: "Dapr",
									ToolCallRequest: &[]llms.ToolCall{
										{
											ID:   "myid",
											Type: "function",
											FunctionCall: &llms.FunctionCall{
												Name:      "myfunc",
												Arguments: "Dapr",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "mixed content with text and tool call",
			messages: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeAI,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "text msg"},
						llms.ToolCall{
							ID:   "myid",
							Type: "function",
							FunctionCall: &llms.FunctionCall{
								Name:      "myfunc",
								Arguments: `{"name": "Dapr"}`,
							},
						},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "stop",
						Choices: []conversation.Choice{
							{
								FinishReason: "stop",
								Index:        0,
								Message: conversation.Message{
									Content: "text msg",
									ToolCallRequest: &[]llms.ToolCall{
										{
											ID:   "myid",
											Type: "function",
											FunctionCall: &llms.FunctionCall{
												Name:      "myfunc",
												Arguments: `{"name": "Dapr"}`,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEcho(logger.NewLogger("echo test"))
			e.Init(t.Context(), conversation.Metadata{})

			r, err := e.Converse(t.Context(), &conversation.Request{
				Message: &tt.messages,
			})
			require.NoError(t, err)

			assert.Len(t, r.Outputs, 1)
			assert.Equal(t, tt.expected.Outputs, r.Outputs)

			output := r.Outputs[0]
			assert.Len(t, output.Choices, 1) // each test has one choice per output
			choice := output.Choices[0]
			expectedOutput := tt.expected.Outputs[0]
			expectedChoice := expectedOutput.Choices[0]

			if expectedChoice.Message.ToolCallRequest != nil {
				assert.NotNil(t, choice.Message.ToolCallRequest)
				assert.Len(t, *choice.Message.ToolCallRequest, len(*expectedChoice.Message.ToolCallRequest))

				for j, toolCall := range *choice.Message.ToolCallRequest {
					expectedToolCall := (*expectedChoice.Message.ToolCallRequest)[j]
					assert.Equal(t, expectedToolCall.ID, toolCall.ID)
					assert.Equal(t, expectedToolCall.Type, toolCall.Type)

					if expectedToolCall.FunctionCall != nil {
						assert.NotNil(t, toolCall.FunctionCall)
						assert.Equal(t, expectedToolCall.FunctionCall.Name, toolCall.FunctionCall.Name)
						assert.Equal(t, expectedToolCall.FunctionCall.Arguments, toolCall.FunctionCall.Arguments)
					}
				}
			} else {
				assert.Nil(t, choice.Message.ToolCallRequest)
			}

			if expectedChoice.Message.Content != "" {
				assert.Equal(t, expectedChoice.Message.Content, choice.Message.Content)
			}
		})
	}
}
