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

package openrouter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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
// metadata default must reach the LLM call options.
func TestInitWiresMaxTokens(t *testing.T) {
	o := NewOpenrouter(logger.NewLogger("test")).(*Openrouter)
	err := o.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{Properties: map[string]string{"key": "test-key", "maxTokens": "50"}},
	})
	require.NoError(t, err)

	stub := &captureModel{}
	o.Model = stub

	_, err = o.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{
			{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, 50, stub.got.MaxTokens, "maxTokens metadata default must reach the call options")
	if stub.got.Metadata != nil {
		assert.NotContains(t, stub.got.Metadata, "openai:use_legacy_max_tokens",
			"OpenRouter accepts max_completion_tokens, so the legacy field must not be forced")
	}
}

// TestMaxTokensOnTheWire checks the request body OpenRouter receives: the
// component default is sent as max_completion_tokens and a request-level
// value overrides it.
func TestMaxTokensOnTheWire(t *testing.T) {
	for name, test := range map[string]struct {
		requestMaxTokens *int64
		want             float64
	}{
		"component default": {want: 50},
		"request override":  {requestMaxTokens: ptr.Of(int64(7)), want: 7},
	} {
		t.Run(name, func(t *testing.T) {
			var body map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				if !assert.NoError(t, err) {
					return
				}
				assert.NoError(t, json.Unmarshal(raw, &body))
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, chatCompletionsReply)
			}))
			t.Cleanup(srv.Close)

			o := NewOpenrouter(logger.NewLogger("test"))
			require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
				"key":       "test-key",
				"endpoint":  srv.URL,
				"maxTokens": "50",
			}}}))

			msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
			_, err := o.Converse(t.Context(), &conversation.Request{Message: &msgs, MaxTokens: test.requestMaxTokens})
			require.NoError(t, err)

			require.NotNil(t, body)
			assert.InDelta(t, test.want, body["max_completion_tokens"], 0)
			assert.NotContains(t, body, "max_tokens")
		})
	}
}
