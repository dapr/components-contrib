package langchaingokit

import (
	"context"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

// LLM is a helper struct that wraps a LangChain Go model
type LLM struct {
	llms.Model
}

func (a *LLM) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	messages := GetMessageFromRequest(r)
	opts := GetOptionsFromRequest(r)

	resp, err := a.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     resp.Choices[i].Content,
			Parameters: r.Parameters,
		})
	}

	res = &conversation.ConversationResponse{
		Outputs: outputs,
	}

	return res, nil
}
