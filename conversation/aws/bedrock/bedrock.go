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
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/tmc/langchaingo/llms/bedrock"
)

type AWSBedrock struct {
	model string
	langchaingokit.LLM

	logger logger.Logger
}

type AWSBedrockMetadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Model        string `json:"model"`
	CacheTTL     string `json:"cacheTTL"`
}

func NewAWSBedrock(logger logger.Logger) conversation.Conversation {
	b := &AWSBedrock{
		logger: logger,
	}

	return b
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
	b.model = m.Model

	llm, err := bedrock.New(
		opts...,
	)
	if err != nil {
		return err
	}

	b.LLM.Model = llm

	if m.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, m.CacheTTL, b.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		b.LLM.Model = cachedModel
	}
	return nil
}

func (b *AWSBedrock) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AWSBedrockMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (b *AWSBedrock) Close() error {
	return nil
}
