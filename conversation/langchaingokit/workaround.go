package langchaingokit

import (
	"strings"

	"github.com/tmc/langchaingo/llms"
)

// CreateToolCallPart creates mistral and ollama api compatible tool call messages.
// Most LLM providers can handle tool calls using the tool call object;
// however, mistral and ollama requires it as text in conversation history.
// This is due to langchaingo limitations.
func CreateToolCallPart(toolCall *llms.ToolCall) llms.ContentPart {
	if toolCall == nil {
		return nil
	}

	if toolCall.FunctionCall == nil {
		return llms.TextContent{
			Text: "Tool call [ID: " + toolCall.ID + "]: <no function call>",
		}
	}

	return llms.TextContent{
		Text: "Tool call [ID: " + toolCall.ID + "]: " + toolCall.FunctionCall.Name + "(" + toolCall.FunctionCall.Arguments + ")",
	}
}

// CreateToolResponseMessage creates mistral and ollama api compatible tool response message
// using the human role specifically otherwise mistral will reject the tool response message.
// Most LLM providers can handle tool call responses using the tool call response object;
// however, mistral and ollama requires it as text in conversation history.
// This is due to langchaingo limitations.
func CreateToolResponseMessage(responses ...llms.ContentPart) llms.MessageContent {
	msg := llms.MessageContent{
		Role: llms.ChatMessageTypeHuman,
	}
	if len(responses) == 0 {
		return msg
	}
	var toolID, name string

	mistralContentParts := make([]string, 0, len(responses))
	for _, response := range responses {
		if resp, ok := response.(llms.ToolCallResponse); ok {
			if toolID == "" {
				toolID = resp.ToolCallID
			}
			if name == "" {
				name = resp.Name
			}
			mistralContentParts = append(mistralContentParts, resp.Content)
		}
	}
	if len(mistralContentParts) > 0 {
		msg.Parts = []llms.ContentPart{
			llms.TextContent{
				Text: "Tool response [ID: " + toolID + ", Name: " + name + "]: " + strings.Join(mistralContentParts, "\n"),
			},
		}
	}
	return msg
}
