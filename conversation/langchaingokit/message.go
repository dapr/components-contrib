package langchaingokit

import (
	"github.com/dapr/components-contrib/conversation"
	"github.com/tmc/langchaingo/llms"
)

func GetMessageFromRequest(r *conversation.ConversationRequest) []llms.MessageContent {
	messages := make([]llms.MessageContent, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		role := ConvertLangchainRole(input.Role)

		messages = append(messages, llms.MessageContent{
			Role: role,
			Parts: []llms.ContentPart{
				llms.TextPart(input.Message),
			},
		})
	}

	return messages
}

func GetOptionsFromRequest(r *conversation.ConversationRequest, opts ...llms.CallOption) []llms.CallOption {
	if opts == nil {
		opts = make([]llms.CallOption, 0)
	}

	if r.Temperature > 0 {
		opts = append(opts, conversation.LangchainTemperature(r.Temperature))
	}

	return opts
}
