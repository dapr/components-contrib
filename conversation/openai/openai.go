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
package openai

import (
	"context"
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/sashabaranov/go-openai"
)

type OpenAI struct {
	key   string
	model string

	logger logger.Logger
}

func NewOpenAI(logger logger.Logger) conversation.Conversation {
	o := &OpenAI{
		logger: logger,
	}

	return o
}

func (o *OpenAI) Init(ctx context.Context, meta conversation.Metadata) error {
	r := &conversation.ConversationRequest{}
	err := kmeta.DecodeMetadata(meta.Properties, &r)
	if err != nil {
		return err
	}

	o.key = r.Key
	o.model = r.Model

	return nil
}

func (o *OpenAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.ConversationRequest{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (o *OpenAI) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	// Note: OPENAI does not support load balance
	client := openai.NewClient(o.key)

	messages := make([]openai.ChatCompletionMessage, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		messages = append(messages, openai.ChatCompletionMessage{
			Role:    openai.ChatMessageRoleUser,
			Content: input,
		})
	}

	req := openai.ChatCompletionRequest{
		Model:    o.model,
		Messages: messages,
	}

	// TODO: support ConversationContext

	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		o.logger.Error(err)
		return nil, err
	}

	o.logger.Debug(resp)

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     resp.Choices[i].Message.Content,
			Parameters: r.Parameters,
		})
	}

	res = &conversation.ConversationResponse{
		ConversationContext: resp.ID,
		Outputs:             outputs,
	}

	return res, nil
}
