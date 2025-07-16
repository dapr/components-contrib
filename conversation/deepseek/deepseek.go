/*
Copyright 2025 The Dapr Authors
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

package deepseek

import (
	"context"
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	deepseek_go "github.com/cohesion-org/deepseek-go"
)

type Deepseek struct {
	llm *deepseek_go.Client
	md  DeepseekMetadata

	logger logger.Logger
}

func NewDeepseek(logger logger.Logger) conversation.Conversation {
	o := &Deepseek{
		logger: logger,
	}

	return o
}

func (d *Deepseek) Init(ctx context.Context, meta conversation.Metadata) error {
	md := DeepseekMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	d.llm = deepseek_go.NewClient(md.Key)
	d.md = md
	return nil
}

func (d *Deepseek) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := DeepseekMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (d *Deepseek) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	messages := make([]deepseek_go.ChatCompletionMessage, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		messages = append(messages, deepseek_go.ChatCompletionMessage{
			Role:    string(input.Role),
			Content: input.Message,
		})
	}

	request := &deepseek_go.ChatCompletionRequest{
		Model:    deepseek_go.DeepSeekChat,
		Messages: messages,
	}

	if d.md.MaxTokens > 0 {
		request.MaxTokens = d.md.MaxTokens
	}

	if r.Temperature > 0 {
		request.Temperature = float32(r.Temperature)
	}

	resp, err := d.llm.CreateChatCompletion(ctx, request)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     resp.Choices[i].Message.Content,
			Parameters: r.Parameters,
		})
	}

	res = &conversation.ConversationResponse{
		Outputs: outputs,
	}

	return res, nil
}

func (d *Deepseek) Close() error {
	return nil
}
