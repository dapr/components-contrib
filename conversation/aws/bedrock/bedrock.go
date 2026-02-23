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
	"time"

	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"

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
	Region           string         `json:"region"`
	Endpoint         string         `json:"endpoint"`
	AccessKey        string         `json:"accessKey"`
	SecretKey        string         `json:"secretKey"`
	SessionToken     string         `json:"sessionToken"`
	Model            string         `json:"model"`
	ResponseCacheTTL *time.Duration `json:"responseCacheTTL,omitempty" mapstructure:"responseCacheTTL" mapstructurealiases:"cacheTTL" mdaliases:"cacheTTL"`

	// TODO: @mikeee - Consider exporting awsCommonAuth.awsRAOpts and using it here
	AssumeRoleArn   string `json:"assumeRoleArn"`
	TrustAnchorArn  string `json:"trustAnchorArn"`
	TrustProfileArn string `json:"trustProfileArn"`
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

	configOpts := awsCommonAuth.Options{
		Logger:          b.logger,
		Properties:      nil,
		Region:          m.Region,
		AccessKey:       m.AccessKey,
		SecretKey:       m.SecretKey,
		SessionToken:    m.SessionToken,
		AssumeRoleArn:   m.AssumeRoleArn,
		TrustAnchorArn:  m.TrustAnchorArn,
		TrustProfileArn: m.TrustProfileArn,
		Endpoint:        m.Endpoint,
	}

	awsConfig, err := awsCommon.NewConfig(ctx, configOpts)
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

	if m.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, m.ResponseCacheTTL, b.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		b.LLM.Model = cachedModel
	}
	return nil
}

func (b *AWSBedrock) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AWSBedrockMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (b *AWSBedrock) Close() error {
	return nil
}
