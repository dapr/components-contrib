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

package deepseek

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
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

// TestInitWiresMaxTokens guards the Init wiring: the maxTokens component
// metadata default must reach the LLM call options. This pins the failure
// mode of the historical DeepSeek regression (#3846), where metadata decoded
// fine but was never applied to any request.
func TestInitWiresMaxTokens(t *testing.T) {
	d := NewDeepseek(logger.NewLogger("test")).(*Deepseek)
	err := d.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{Properties: map[string]string{"key": "test-key", "maxTokens": "50"}},
	})
	require.NoError(t, err)

	stub := &captureModel{}
	d.Model = stub

	_, err = d.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{
			{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, 50, stub.got.MaxTokens, "maxTokens metadata default must reach the call options")
	assert.Equal(t, true, stub.got.Metadata["openai:use_legacy_max_tokens"],
		"this OpenAI-compatible provider must force the legacy max_tokens field")
}
