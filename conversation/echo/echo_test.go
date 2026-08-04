package echo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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
									Content: "first message\nsecond message\nthird message",
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
			assert.Len(t, r.Outputs, 1)
			assert.Equal(t, tt.expected.Outputs, r.Outputs)
		})
	}
}

func TestConverseMaxTokens(t *testing.T) {
	fiveWords := []llms.MessageContent{
		{
			Role: llms.ChatMessageTypeHuman,
			Parts: []llms.ContentPart{
				llms.TextContent{Text: "one two three four five"},
			},
		},
	}

	tests := []struct {
		name             string
		properties       map[string]string
		requestMaxTokens *int64
		tools            []llms.Tool
		wantContent      string
		wantStopReason   string
		wantPrompt       uint64
		wantCompletion   uint64
	}{
		{
			name:             "request max tokens truncates content",
			requestMaxTokens: ptr.Of(int64(2)),
			wantContent:      "one two",
			wantStopReason:   "length",
			wantPrompt:       5,
			wantCompletion:   2,
		},
		{
			name:             "max tokens equal to word count does not truncate",
			requestMaxTokens: ptr.Of(int64(5)),
			wantContent:      "one two three four five",
			wantStopReason:   "stop",
			wantPrompt:       5,
			wantCompletion:   5,
		},
		{
			name:           "metadata max tokens is the default",
			properties:     map[string]string{"maxTokens": "2"},
			wantContent:    "one two",
			wantStopReason: "length",
			wantPrompt:     5,
			wantCompletion: 2,
		},
		{
			name:             "request max tokens overrides metadata default",
			properties:       map[string]string{"maxTokens": "2"},
			requestMaxTokens: ptr.Of(int64(100)),
			wantContent:      "one two three four five",
			wantStopReason:   "stop",
			wantPrompt:       5,
			wantCompletion:   5,
		},
		{
			name:             "truncation takes precedence over tool_calls stop reason",
			requestMaxTokens: ptr.Of(int64(2)),
			tools: []llms.Tool{
				{
					Type:     "function",
					Function: &llms.FunctionDefinition{Name: "myfunc"},
				},
			},
			wantContent:    "one two",
			wantStopReason: "length",
			wantPrompt:     5,
			wantCompletion: 2,
		},
		{
			name:             "zero request max tokens without default does not truncate",
			requestMaxTokens: ptr.Of(int64(0)),
			wantContent:      "one two three four five",
			wantStopReason:   "stop",
			wantPrompt:       5,
			wantCompletion:   5,
		},
		{
			name:             "zero request max tokens falls back to metadata default",
			properties:       map[string]string{"maxTokens": "2"},
			requestMaxTokens: ptr.Of(int64(0)),
			wantContent:      "one two",
			wantStopReason:   "length",
			wantPrompt:       5,
			wantCompletion:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEcho(logger.NewLogger("echo test"))
			require.NoError(t, e.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{Properties: tt.properties},
			}))

			req := &conversation.Request{
				Message:   &fiveWords,
				MaxTokens: tt.requestMaxTokens,
			}
			if len(tt.tools) > 0 {
				req.Tools = &tt.tools
			}

			resp, err := e.Converse(t.Context(), req)
			require.NoError(t, err)
			require.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]
			assert.Equal(t, tt.wantStopReason, output.StopReason)
			require.Len(t, output.Choices, 1)
			assert.Equal(t, tt.wantStopReason, output.Choices[0].FinishReason)
			assert.Equal(t, tt.wantContent, output.Choices[0].Message.Content)

			require.NotNil(t, resp.Usage)
			assert.Equal(t, tt.wantPrompt, resp.Usage.PromptTokens)
			assert.Equal(t, tt.wantCompletion, resp.Usage.CompletionTokens)
			assert.Equal(t, tt.wantPrompt+tt.wantCompletion, resp.Usage.TotalTokens)

			if len(tt.tools) > 0 {
				require.NotNil(t, output.Choices[0].Message.ToolCallRequest,
					"tool calls must be retained when truncation changes the stop reason")
				assert.Len(t, *output.Choices[0].Message.ToolCallRequest, len(tt.tools))
			}
		})
	}
}

func TestInitInvalidMaxTokens(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	err := e.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{Properties: map[string]string{"maxTokens": "not-a-number"}},
	})
	require.Error(t, err)
}

func TestConverseAlpha2(t *testing.T) {
	tests := []struct {
		name     string
		messages []llms.MessageContent
		tools    []llms.Tool
		expected *conversation.Response
	}{
		{
			name: "tool call request",
			messages: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "hello echo"},
					},
				},
			},
			tools: []llms.Tool{
				{
					Type: "function",
					Function: &llms.FunctionDefinition{
						Name:        "myfunc",
						Description: "A function that does something",
						Parameters: map[string]any{
							"type": "object",
							"properties": map[string]any{
								"name": map[string]any{
									"type":        "string",
									"description": "The name to process",
								},
							},
						},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "tool_calls",
						Choices: []conversation.Choice{
							{
								FinishReason: "tool_calls",
								Index:        0,
								Message: conversation.Message{
									Content: "hello echo",
									ToolCallRequest: &[]llms.ToolCall{
										{
											ID:   "0", // ID is auto-generated by the echo component
											Type: "function",
											FunctionCall: &llms.FunctionCall{
												Name:      "myfunc",
												Arguments: "name",
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
			name: "tool call request with multiple arguments alphabetic ordering of arguments",
			messages: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "hello echo"},
					},
				},
			},
			tools: []llms.Tool{
				{
					Type: "function",
					Function: &llms.FunctionDefinition{
						Name:        "myfunc",
						Description: "A function that does something",
						Parameters: map[string]any{
							"type": "object",
							"properties": map[string]any{
								"unit": map[string]any{
									"type":        "string",
									"description": "unit should come last",
								},
								"name": map[string]any{
									"type":        "string",
									"description": "The name to process, should come second",
								},
								"location": map[string]any{
									"type":        "string",
									"description": "location should come first",
								},
							},
						},
					},
				},
			},
			expected: &conversation.Response{
				Outputs: []conversation.Result{
					{
						StopReason: "tool_calls",
						Choices: []conversation.Choice{
							{
								FinishReason: "tool_calls",
								Index:        0,
								Message: conversation.Message{
									Content: "hello echo",
									ToolCallRequest: &[]llms.ToolCall{
										{
											ID:   "0", // ID is auto-generated by the echo component
											Type: "function",
											FunctionCall: &llms.FunctionCall{
												Name:      "myfunc",
												Arguments: "location,name,unit",
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
			name: "text message with tool call response",
			// echo responds with the text message and tool call response appended to the message content
			messages: []llms.MessageContent{
				{
					Role: llms.ChatMessageTypeHuman,
					Parts: []llms.ContentPart{
						llms.TextContent{Text: "hello echo"},
					},
				},
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
									Content: "hello echo\nTool Response for tool ID 'myid' with name 'myfunc': Dapr",
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

			request := &conversation.Request{
				Message: &tt.messages,
			}
			if len(tt.tools) > 0 {
				request.Tools = &tt.tools
			}

			r, err := e.Converse(t.Context(), request)
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
