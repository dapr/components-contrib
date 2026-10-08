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

package spark

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test"))
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{},
		}})
		require.EqualError(t, err, "spark api key is required")
	})

	t.Run("defaults", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test")).(*Spark)
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{"key": "test-key"},
		}})
		require.NoError(t, err)
		assert.Equal(t, "https://maas-api.cn-huabei-1.xf-yun.com/v2", s.md.Endpoint)
		assert.Equal(t, "spark-x2.5", s.GetModel())
		assert.Equal(t, "disabled", s.md.Thinking)
		assert.Equal(t, "test-key", s.md.Key)
	})

	t.Run("default endpoint with trailing slash", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test")).(*Spark)
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{
				"key":      "test-key",
				"endpoint": "https://maas-api.cn-huabei-1.xf-yun.com/v2/",
			},
		}})
		require.NoError(t, err)
		assert.Equal(t, "disabled", s.md.Thinking)
	})

	t.Run("legacy spark http api", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test")).(*Spark)
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{
				"key":      "test-key",
				"endpoint": "https://spark-api-open.xf-yun.com/v1",
				"model":    "4.0Ultra",
			},
		}})
		require.NoError(t, err)
		assert.Equal(t, "https://spark-api-open.xf-yun.com/v1", s.md.Endpoint)
		assert.Equal(t, "4.0Ultra", s.GetModel())
		assert.Empty(t, s.md.Thinking, "the thinking field is not sent to custom endpoints by default")
	})

	t.Run("invalid thinking mode", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test"))
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{"key": "test-key", "thinking": "sometimes"},
		}})
		require.ErrorContains(t, err, `invalid thinking mode "sometimes"`)
	})

	t.Run("with cache", func(t *testing.T) {
		s := NewSpark(logger.NewLogger("test"))
		err := s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
			Properties: map[string]string{"key": "test-key", "cacheTTL": "10m"},
		}})
		require.NoError(t, err)
	})
}

// captureServer is a fake chat completions API that records the JSON body of each request.
func captureServer(t *testing.T) (*httptest.Server, *[]map[string]any) {
	t.Helper()
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := map[string]any{}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","object":"chat.completion","created":1,"model":"spark-x2.5",` +
			`"choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}]}`))
	}))
	t.Cleanup(server.Close)
	return server, &bodies
}

func converse(t *testing.T, properties map[string]string) {
	t.Helper()
	s := NewSpark(logger.NewLogger("test"))
	require.NoError(t, s.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: properties}}))
	messages := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "ping")}
	resp, err := s.Converse(t.Context(), &conversation.Request{Message: &messages})
	require.NoError(t, err)
	assert.Equal(t, "pong", resp.Outputs[0].Choices[0].Message.Content)
}

func TestThinkingMode(t *testing.T) {
	t.Run("sent when configured", func(t *testing.T) {
		server, bodies := captureServer(t)
		converse(t, map[string]string{"key": "test-key", "endpoint": server.URL, "thinking": "Auto"})
		require.Len(t, *bodies, 1)
		assert.Equal(t, map[string]any{"type": "auto"}, (*bodies)[0]["thinking"])
		assert.Equal(t, "spark-x2.5", (*bodies)[0]["model"])
	})

	t.Run("not sent to a custom endpoint by default", func(t *testing.T) {
		server, bodies := captureServer(t)
		converse(t, map[string]string{"key": "test-key", "endpoint": server.URL, "model": "4.0Ultra"})
		require.Len(t, *bodies, 1)
		assert.NotContains(t, (*bodies)[0], "thinking")
	})

	t.Run("keeps an explicit thinking field and other requests", func(t *testing.T) {
		server, bodies := captureServer(t)
		client := &thinkingClient{doer: server.Client(), thinking: "disabled"}

		req, err := http.NewRequest(http.MethodPost, server.URL+"/chat/completions",
			strings.NewReader(`{"model":"m","thinking":{"type":"enabled"}}`))
		require.NoError(t, err)
		res, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, res.Body.Close())

		req, err = http.NewRequest(http.MethodPost, server.URL+"/embeddings", strings.NewReader(`{"model":"m"}`))
		require.NoError(t, err)
		res, err = client.Do(req)
		require.NoError(t, err)
		require.NoError(t, res.Body.Close())

		require.Len(t, *bodies, 2)
		assert.Equal(t, map[string]any{"type": "enabled"}, (*bodies)[0]["thinking"])
		assert.NotContains(t, (*bodies)[1], "thinking")
	})
}

func TestMaxTokens(t *testing.T) {
	t.Run("default is sent as max_tokens", func(t *testing.T) {
		server, bodies := captureServer(t)
		converse(t, map[string]string{"key": "test-key", "endpoint": server.URL, "maxTokens": "50"})
		require.Len(t, *bodies, 1)
		assert.InDelta(t, 50, (*bodies)[0]["max_tokens"], 0)
		assert.NotContains(t, (*bodies)[0], "max_completion_tokens")
	})

	t.Run("not sent when unset", func(t *testing.T) {
		server, bodies := captureServer(t)
		converse(t, map[string]string{"key": "test-key", "endpoint": server.URL})
		require.Len(t, *bodies, 1)
		assert.NotContains(t, (*bodies)[0], "max_tokens")
		assert.NotContains(t, (*bodies)[0], "max_completion_tokens")
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := NewSpark(logger.NewLogger("test")).(*Spark).GetComponentMetadata()
	for _, name := range []string{"key", "model", "endpoint", "responseCacheTTL", "thinking", "maxTokens"} {
		assert.Contains(t, md, name)
	}
	assert.NotContains(t, md, "Key")
	assert.NotContains(t, md, "MaxTokens")
}
