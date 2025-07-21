package echo

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"
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
						Result:          "hello",
						Parameters:      nil,
						ToolCallRequest: []llms.ToolCall{},
						StopReason:      "done",
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
						Result:          "",
						Parameters:      nil,
						ToolCallRequest: []llms.ToolCall{},
						StopReason:      "done",
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
						Result:          "first message",
						Parameters:      nil,
						ToolCallRequest: []llms.ToolCall{},
						StopReason:      "done",
					},
					{
						Result:          "second message",
						Parameters:      nil,
						ToolCallRequest: []llms.ToolCall{},
						StopReason:      "done",
					},
					{
						Result:          "third message",
						Parameters:      nil,
						ToolCallRequest: []llms.ToolCall{},
						StopReason:      "done",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEcho(logger.NewLogger("echo test"))
			e.Init(t.Context(), conversation.Metadata{})

			messages := make([]llms.MessageContent, len(tt.inputs))
			copy(messages, tt.inputs)

			r, err := e.Converse(t.Context(), &conversation.Request{
				Message: &messages,
			})
			require.NoError(t, err)
			assert.Len(t, r.Outputs, len(tt.expected.Outputs))
			assert.Equal(t, tt.expected.Outputs, r.Outputs)
		})
	}
}

func TestConverseV1Alpha2(t *testing.T) {
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
			expected: &conversation.Response{
				Outputs: []conversation.Result{
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
			messages: []llms.MessageContent{
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
			expected: &conversation.Response{
				Outputs: []conversation.Result{
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

			r, err := e.Converse(t.Context(), &conversation.Request{
				Message: &tt.messages,
			})
			require.NoError(t, err)
			assert.Len(t, r.Outputs, len(tt.messages))
			assert.Equal(t, tt.expected.Outputs, r.Outputs)
		})
	}
}
