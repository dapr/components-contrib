/*
Copyright 2026 The Dapr Authors
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

package conversation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/anthropic"
	bedrock "github.com/dapr/components-contrib/conversation/aws/bedrock"
	"github.com/dapr/components-contrib/conversation/deepseek"
	"github.com/dapr/components-contrib/conversation/googleai"
	"github.com/dapr/components-contrib/conversation/huggingface"
	"github.com/dapr/components-contrib/conversation/mistral"
	"github.com/dapr/components-contrib/conversation/ollama"
	"github.com/dapr/components-contrib/conversation/openai"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// captureModel records the folded call options passed to GenerateContent.
type captureModel struct {
	got llms.CallOptions
}

func (s *captureModel) GenerateContent(_ context.Context, _ []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
	for _, opt := range options {
		opt(&s.got)
	}
	return &llms.ContentResponse{Choices: []*llms.ContentChoice{{Content: "ok", StopReason: "stop"}}}, nil
}

func (s *captureModel) Call(_ context.Context, _ string, _ ...llms.CallOption) (string, error) {
	return "", nil
}

// TestInitWiresMaxTokens guards the one-line Init() wiring in every provider:
// the maxTokens component metadata default must reach the LLM call options,
// and the OpenAI-compatible providers must force the legacy max_tokens wire
// field. This pins the failure mode of the historical DeepSeek regression
// (#3846), where metadata decoded fine but was never applied to any request.
func TestInitWiresMaxTokens(t *testing.T) {
	log := logger.NewLogger("wiring test")
	props := map[string]string{"key": "test-key", "maxTokens": "50"}

	tests := []struct {
		name           string
		construct      func() conversation.Conversation
		setModel       func(conversation.Conversation, llms.Model)
		properties     map[string]string
		wantLegacyFlag bool
	}{
		{
			name:       "openai keeps the modern max_completion_tokens field",
			construct:  func() conversation.Conversation { return openai.NewOpenAI(log) },
			setModel:   func(c conversation.Conversation, m llms.Model) { c.(*openai.OpenAI).Model = m },
			properties: props,
			// gpt-5/o-series reject the legacy max_tokens field.
			wantLegacyFlag: false,
		},
		{
			name:           "anthropic",
			construct:      func() conversation.Conversation { return anthropic.NewAnthropic(log) },
			setModel:       func(c conversation.Conversation, m llms.Model) { c.(*anthropic.Anthropic).Model = m },
			properties:     props,
			wantLegacyFlag: false,
		},
		{
			name:           "googleai forces the legacy max_tokens field",
			construct:      func() conversation.Conversation { return googleai.NewGoogleAI(log) },
			setModel:       func(c conversation.Conversation, m llms.Model) { c.(*googleai.GoogleAI).Model = m },
			properties:     props,
			wantLegacyFlag: true,
		},
		{
			name:           "mistral",
			construct:      func() conversation.Conversation { return mistral.NewMistral(log) },
			setModel:       func(c conversation.Conversation, m llms.Model) { c.(*mistral.Mistral).Model = m },
			properties:     props,
			wantLegacyFlag: false,
		},
		{
			name:           "ollama forces the legacy max_tokens field",
			construct:      func() conversation.Conversation { return ollama.NewOllama(log) },
			setModel:       func(c conversation.Conversation, m llms.Model) { c.(*ollama.Ollama).Model = m },
			properties:     map[string]string{"maxTokens": "50"},
			wantLegacyFlag: true,
		},
		{
			name:           "huggingface forces the legacy max_tokens field",
			construct:      func() conversation.Conversation { return huggingface.NewHuggingface(log) },
			setModel:       func(c conversation.Conversation, m llms.Model) { c.(*huggingface.Huggingface).Model = m },
			properties:     props,
			wantLegacyFlag: true,
		},
		{
			name:           "deepseek forces the legacy max_tokens field",
			construct:      func() conversation.Conversation { return deepseek.NewDeepseek(log) },
			setModel:       func(c conversation.Conversation, m llms.Model) { c.(*deepseek.Deepseek).Model = m },
			properties:     props,
			wantLegacyFlag: true,
		},
		{
			name:      "bedrock",
			construct: func() conversation.Conversation { return bedrock.NewAWSBedrock(log) },
			setModel:  func(c conversation.Conversation, m llms.Model) { c.(*bedrock.AWSBedrock).Model = m },
			properties: map[string]string{
				"region":    "us-east-1",
				"accessKey": "test-key",
				"secretKey": "test-secret",
				"model":     "amazon.titan-text-lite-v1",
				"maxTokens": "50",
			},
			wantLegacyFlag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comp := tt.construct()
			err := comp.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{Properties: tt.properties},
			})
			if err != nil && tt.name == "bedrock" {
				t.Skipf("Skipping test due to AWS config error: %v", err)
			}
			require.NoError(t, err)

			stub := &captureModel{}
			tt.setModel(comp, stub)

			_, err = comp.Converse(t.Context(), &conversation.Request{
				Message: &[]llms.MessageContent{
					{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
				},
			})
			require.NoError(t, err)

			assert.Equal(t, 50, stub.got.MaxTokens, "maxTokens metadata default must reach the call options")
			if tt.wantLegacyFlag {
				assert.Equal(t, true, stub.got.Metadata["openai:use_legacy_max_tokens"],
					"OpenAI-compatible provider must force the legacy max_tokens field")
			} else if stub.got.Metadata != nil {
				assert.NotContains(t, stub.got.Metadata, "openai:use_legacy_max_tokens")
			}
		})
	}
}
