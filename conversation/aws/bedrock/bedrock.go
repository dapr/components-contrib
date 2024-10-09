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
package bedrock

import (
	"context"
	"reflect"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/bedrock"
)

type AWSBedrock struct {
	model string
	llm   *bedrock.LLM

	logger logger.Logger
}

type AWSBedrockMetadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Model        string `json:"model"`
}

func NewAWSBedrock(logger logger.Logger) conversation.Conversation {
	b := &AWSBedrock{
		logger: logger,
	}

	return b
}

func convertRole(role conversation.Role) llms.ChatMessageType {
	switch role {
	case conversation.RoleSystem:
		return llms.ChatMessageTypeSystem
	case conversation.RoleUser:
		return llms.ChatMessageTypeHuman
	case conversation.RoleAssistant:
		return llms.ChatMessageTypeAI
	case conversation.RoleTool:
		return llms.ChatMessageTypeTool
	case conversation.RoleFunction:
		return llms.ChatMessageTypeFunction
	default:
		return llms.ChatMessageTypeHuman
	}
}

func (b *AWSBedrock) Init(ctx context.Context, meta conversation.Metadata) error {
	m := AWSBedrockMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	awsConfig, err := awsAuth.GetConfigV2(m.AccessKey, m.SecretKey, m.SessionToken, m.Region, m.Endpoint)
	if err != nil {
		return err
	}

	bedrockClient := bedrockruntime.NewFromConfig(awsConfig)

	opts := []bedrock.Option{bedrock.WithClient(bedrockClient)}
	if m.Model != "" {
		opts = append(opts, bedrock.WithModel(m.Model))
	}

	llm, err := bedrock.New(
		opts...,
	)
	if err != nil {
		return err
	}

	b.llm = llm
	return nil
}

func (b *AWSBedrock) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AWSBedrockMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (b *AWSBedrock) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	messages := make([]llms.MessageContent, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		role := convertRole(input.Role)

		messages = append(messages, llms.MessageContent{
			Role: role,
			Parts: []llms.ContentPart{
				llms.TextPart(input.Message),
			},
		})
	}

	resp, err := b.llm.GenerateContent(ctx, messages)
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

func (b *AWSBedrock) Close() error {
	return nil
}
