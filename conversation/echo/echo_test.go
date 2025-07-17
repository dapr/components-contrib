package echo

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
	"github.com/tmc/langchaingo/llms"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConverse(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []conversation.ConversationInput
		expected *conversation.ConversationResponse
	}{
		{
			name: "basic input",
			inputs: []conversation.ConversationInput{
				{
					Message: "hello",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:     "hello",
						Parameters: nil,
					},
				},
			},
		},
		{
			name: "empty input",
			inputs: []conversation.ConversationInput{
				{
					Message: "",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:     "",
						Parameters: nil,
					},
				},
			},
		},
		{
			name: "multiple inputs - echo comp returns 1st input only",
			inputs: []conversation.ConversationInput{
				{
					Message: "first message",
				},
				{
					Message: "second message",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:     "first message",
						Parameters: nil,
					},
					{
						Result:     "second message",
						Parameters: nil,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEcho(logger.NewLogger("echo test"))
			e.Init(t.Context(), conversation.Metadata{})

			r, err := e.Converse(t.Context(), &conversation.ConversationRequest{
				Inputs: tt.inputs,
			})
			require.NoError(t, err)
			assert.Len(t, r.Outputs, len(tt.inputs))
			assert.Equal(t, tt.expected.Outputs, r.Outputs)
		})
	}
}

func TestConverseV1Alpha2(t *testing.T) {
	tests := []struct {
		name     string
		messages []*llms.MessageContent
		expected *conversation.ConversationResponseV1Alpha2
	}{
		{
			name: "tool call request",
			messages: []*llms.MessageContent{
				{
					Role: llms.ChatMessageTypeAI,
					Parts: []llms.ContentPart{
						&llms.ToolCall{
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
			expected: &conversation.ConversationResponseV1Alpha2{
				Outputs: []conversation.ConversationResultV1Alpha2{
					{
						Result: "",
						ToolCallRequest: []llms.ToolCall{
							{
								ID:   "myid",
								Type: "function",
								FunctionCall: &llms.FunctionCall{
									Name:      "myfunc",
									Arguments: `{"name": "Dapr"}`,
								},
							},
						},
						Parameters: nil,
					},
				},
			},
		},
		{
			name: "tool call response",
			messages: []*llms.MessageContent{
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
			expected: &conversation.ConversationResponseV1Alpha2{
				Outputs: []conversation.ConversationResultV1Alpha2{
					{
						Result: "Dapr",
						ToolCallRequest: []llms.ToolCall{
							{
								ID:   "myid",
								Type: "function",
								FunctionCall: &llms.FunctionCall{
									Name:      "myfunc",
									Arguments: "Dapr",
								},
							},
						},
						Parameters: nil,
					},
				},
			},
		},
		{
			name: "mixed content with text and tool call",
			messages: []*llms.MessageContent{
				{
					Role: llms.ChatMessageTypeAI,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "text msg"},
						&llms.ToolCall{
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
			expected: &conversation.ConversationResponseV1Alpha2{
				Outputs: []conversation.ConversationResultV1Alpha2{
					{
						Result: "text msg",
						ToolCallRequest: []llms.ToolCall{
							{
								ID:   "myid",
								Type: "function",
								FunctionCall: &llms.FunctionCall{
									Name:      "myfunc",
									Arguments: `{"name": "Dapr"}`,
								},
							},
						},
						Parameters: nil,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEcho(logger.NewLogger("echo test"))
			e.Init(t.Context(), conversation.Metadata{})

			r, err := e.ConverseV1Alpha2(t.Context(), &conversation.ConversationRequestV1Alpha2{
				Message: tt.messages,
			})
			require.NoError(t, err)
			assert.Len(t, r.Outputs, len(tt.messages))
			assert.Equal(t, tt.expected.Outputs, r.Outputs)
		})
	}
}
