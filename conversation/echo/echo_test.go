package echo

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"

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
					Content: "hello",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:       "hello",
						ToolCallName: "",
						Parameters:   nil,
					},
				},
			},
		},
		{
			name: "empty input",
			inputs: []conversation.ConversationInput{
				{
					Content: "",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:       "",
						ToolCallName: "",
						Parameters:   nil,
					},
				},
			},
		},
		{
			name: "multiple inputs - echo comp returns 1st input only",
			inputs: []conversation.ConversationInput{
				{
					Content: "first message",
				},
				{
					Content: "second message",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:       "first message",
						ToolCallName: "",
						Parameters:   nil,
					},
					{
						Result:       "second message",
						ToolCallName: "",
						Parameters:   nil,
					},
				},
			},
		},
		{
			name: "tool call input",
			inputs: []conversation.ConversationInput{
				{
					Content:      "first message",
					ToolCallName: "mytool",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:       "first message",
						ToolCallName: "mytool",
						Parameters:   nil,
					},
				},
			},
		},
		{
			name: "tool call input",
			inputs: []conversation.ConversationInput{
				{
					Content:      "first message",
					ToolCallName: "mytool",
				},
				{
					Content:      "second message",
					ToolCallName: "mysecond",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:       "first message",
						ToolCallName: "mytool",
						Parameters:   nil,
					},
					{
						Result:       "second message",
						ToolCallName: "mysecond",
						Parameters:   nil,
					},
				},
			},
		},
		{
			name: "mixed - normal input content and then a tool call input",
			inputs: []conversation.ConversationInput{
				{
					Content: "first message",
				},
				{
					Content:      "second message",
					ToolCallName: "mysecond",
				},
			},
			expected: &conversation.ConversationResponse{
				Outputs: []conversation.ConversationResult{
					{
						Result:       "first message",
						ToolCallName: "",
						Parameters:   nil,
					},
					{
						Result:       "second message",
						ToolCallName: "mysecond",
						Parameters:   nil,
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
